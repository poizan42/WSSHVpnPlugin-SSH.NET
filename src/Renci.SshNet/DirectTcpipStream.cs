using System;
using System.Globalization;
using System.Threading;

using Renci.SshNet.Channels;
using Renci.SshNet.Common;

namespace Renci.SshNet
{
    /// <summary>
    /// A byte stream carried by a <c>direct-tcpip</c> SSH channel.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Deliberately not a <see cref="System.IO.Stream"/>. The intended consumer is a user-space
    /// TCP/IP stack with one of these per flow, and a <c>Stream</c> shape forces it either to copy
    /// received bytes into its own buffer or to hold a blocked read - and <c>Stream</c>'s default
    /// asynchronous read pins a thread-pool thread per blocked flow. Instead the received bytes are
    /// peeked in place with <see cref="TryRead"/> and released with <see cref="Advance"/> only once
    /// the far end has acknowledged them, so the same buffer serves as the retransmit buffer.
    /// </para>
    /// <para>
    /// Nothing here blocks. <see cref="TrySend"/> reports what the remote window allowed, and the
    /// notifications say when to try again. All of the notifications are raised on the session's
    /// message listener thread, so handlers must be O(1) and must not block: one thread dispatches
    /// every channel on the session.
    /// </para>
    /// </remarks>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Naming",
        "CA1711:Identifiers should not have incorrect suffix",
        Justification = "It is a stream of bytes over a channel; it deliberately does not derive from Stream, for the reasons in the remarks.")]
    public sealed class DirectTcpipStream : IDisposable
    {
        private readonly ChannelDirectTcpip _channel;
        private readonly Lock _readLock = new Lock();
        private readonly byte[] _received;

        private int _start;
        private int _end;
        private bool _peerEof;
        private bool _peerClosed;
        private int _disposed;

        internal DirectTcpipStream(ChannelDirectTcpip channel, int bufferSize)
        {
            _channel = channel;
            _received = new byte[bufferSize];

            // The consumer decides when the remote party may send more, which is the whole point of
            // choosing a window smaller than the receive buffer.
            _channel.DeferWindowCredit = true;

            _channel.DataReceived += OnDataReceived;
            _channel.EndOfData += OnEndOfData;
            _channel.Closed += OnClosed;
            _channel.WindowAvailable += OnWindowAvailable;
            _channel.Exception += OnException;
        }

        /// <summary>
        /// Occurs when bytes have arrived and <see cref="TryRead"/> has something to return.
        /// </summary>
        public event EventHandler<EventArgs> DataAvailable;

        /// <summary>
        /// Occurs when the remote window has been enlarged, so a <see cref="TrySend"/> that reported
        /// a full window may now make progress.
        /// </summary>
        public event EventHandler<EventArgs> WindowAvailable;

        /// <summary>
        /// Occurs when the remote party will send no more data.
        /// </summary>
        public event EventHandler<EventArgs> PeerEof;

        /// <summary>
        /// Occurs when the channel has been closed by the remote party.
        /// </summary>
        public event EventHandler<EventArgs> PeerClosed;

        /// <summary>
        /// Occurs when the channel fails.
        /// </summary>
        public event EventHandler<ExceptionEventArgs> Error;

        /// <summary>
        /// Gets a value indicating whether the channel is still open.
        /// </summary>
        /// <value>
        /// <see langword="true"/> if the channel is open; otherwise, <see langword="false"/>.
        /// </value>
        public bool IsOpen
        {
            get { return _channel.IsOpen; }
        }

        /// <summary>
        /// Gets a value indicating whether the channel has been closed by the remote party.
        /// </summary>
        /// <value>
        /// <see langword="true"/> if the channel was closed; otherwise, <see langword="false"/>.
        /// </value>
        public bool IsPeerClosed
        {
            get
            {
                lock (_readLock)
                {
                    return _peerClosed;
                }
            }
        }

        /// <summary>
        /// Gets a value indicating whether the remote party has signalled that it will send no more.
        /// </summary>
        /// <value>
        /// <see langword="true"/> if end of data was received; otherwise, <see langword="false"/>.
        /// </value>
        public bool IsPeerEof
        {
            get
            {
                lock (_readLock)
                {
                    return _peerEof;
                }
            }
        }

        /// <summary>
        /// Peeks at the bytes received and not yet released.
        /// </summary>
        /// <param name="data">Receives the bytes, valid until the next call to <see cref="Advance"/>.</param>
        /// <returns>
        /// <see langword="true"/> if there was anything to read; otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// The segment points into this stream's own buffer. It stays valid until
        /// <see cref="Advance"/> releases it, which is what lets a caller retransmit from it rather
        /// than keeping a copy of its own.
        /// </remarks>
        public bool TryRead(out ArraySegment<byte> data)
        {
            lock (_readLock)
            {
                var count = _end - _start;
                if (count == 0)
                {
                    data = default;
                    return false;
                }

                data = new ArraySegment<byte>(_received, _start, count);
                return true;
            }
        }

        /// <summary>
        /// Releases bytes previously returned by <see cref="TryRead"/>.
        /// </summary>
        /// <param name="count">The number of bytes to release, from the start of the peeked segment.</param>
        /// <returns>
        /// <see langword="true"/> if enough has been released that <see cref="FlushWindowCredit"/>
        /// should be called; otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// Does not send anything, deliberately. Crediting the window emits a message, and sending
        /// blocks for the duration of a key exchange; a consumer draining its buffer must not inherit
        /// that. Record here, and flush from whichever thread owns sending.
        /// </remarks>
        public bool Advance(int count)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(count);

            lock (_readLock)
            {
                if (count > _end - _start)
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(count),
                        string.Format(CultureInfo.InvariantCulture,
                                      "Cannot release {0} bytes; only {1} have been read.",
                                      count,
                                      _end - _start));
                }

                _start += count;

                if (_start == _end)
                {
                    _start = 0;
                    _end = 0;
                }
            }

            return _channel.ReleaseReceivedData(count);
        }

        /// <summary>
        /// Credits the remote party for the bytes released by <see cref="Advance"/>.
        /// </summary>
        /// <remarks>
        /// Blocks for the duration of a key exchange, like any send.
        /// </remarks>
        public void FlushWindowCredit()
        {
            _channel.FlushWindowCredit();
        }

        /// <summary>
        /// Sends as much as the remote window currently allows, without waiting.
        /// </summary>
        /// <param name="data">An array of <see cref="byte"/> containing the payload to send.</param>
        /// <param name="offset">The zero-based offset in <paramref name="data"/> at which to begin taking data from.</param>
        /// <param name="count">The number of bytes of <paramref name="data"/> to send.</param>
        /// <param name="written">Receives the number of bytes actually sent.</param>
        /// <returns>The outcome of the send.</returns>
        public ChannelSendResult TrySend(byte[] data, int offset, int count, out int written)
        {
            return _channel.TrySend(data, offset, count, out written);
        }

        /// <summary>
        /// Signals that this side will send no more data.
        /// </summary>
        public void SendEof()
        {
            if (_channel.IsOpen)
            {
                _channel.SendEof();
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            // Unsubscribed before disposing, and outside the read lock: the channel's close path
            // waits on the message listener thread, and taking a lock a notification handler also
            // takes would deadlock the two against each other.
            _channel.DataReceived -= OnDataReceived;
            _channel.EndOfData -= OnEndOfData;
            _channel.Closed -= OnClosed;
            _channel.WindowAvailable -= OnWindowAvailable;
            _channel.Exception -= OnException;

            _channel.Dispose();
        }

        /// <summary>
        /// Copies received bytes into the buffer. Runs on the session's message listener thread, and
        /// the data aliases a buffer the session reuses, so it has to be copied before returning.
        /// </summary>
        private void OnDataReceived(object sender, ChannelDataEventArgs e)
        {
            lock (_readLock)
            {
                var count = e.Data.Count;

                if (_received.Length - _end < count)
                {
                    // Reclaim the space already released before giving up on it.
                    Compact();
                }

                if (_received.Length - _end < count)
                {
                    // Only reachable if the buffer is smaller than the window we advertised, which
                    // is a configuration error rather than a runtime condition: the remote party is
                    // entitled to send everything the window allows.
                    throw new SshException(string.Format(
                        CultureInfo.InvariantCulture,
                        "Received {0} bytes with only {1} of buffer free; the receive buffer is smaller than the advertised window.",
                        count,
                        _received.Length - _end));
                }

                Buffer.BlockCopy(e.Data.Array, e.Data.Offset, _received, _end, count);
                _end += count;
            }

            DataAvailable?.Invoke(this, EventArgs.Empty);
        }

        private void Compact()
        {
            var count = _end - _start;
            if (count > 0 && _start > 0)
            {
                Buffer.BlockCopy(_received, _start, _received, 0, count);
            }

            _start = 0;
            _end = count;
        }

        private void OnEndOfData(object sender, ChannelEventArgs e)
        {
            lock (_readLock)
            {
                _peerEof = true;
            }

            PeerEof?.Invoke(this, EventArgs.Empty);
        }

        private void OnClosed(object sender, ChannelEventArgs e)
        {
            lock (_readLock)
            {
                _peerClosed = true;
            }

            PeerClosed?.Invoke(this, EventArgs.Empty);
        }

        private void OnWindowAvailable(object sender, EventArgs e)
        {
            WindowAvailable?.Invoke(this, EventArgs.Empty);
        }

        private void OnException(object sender, ExceptionEventArgs e)
        {
            Error?.Invoke(this, e);
        }
    }
}
