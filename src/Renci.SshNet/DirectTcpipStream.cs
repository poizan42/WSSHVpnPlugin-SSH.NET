using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

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
    public sealed class DirectTcpipStream : IDisposable, IAsyncDisposable
    {
        /// <summary>
        /// How much of the receive buffer is allocated before any data arrives.
        /// </summary>
        /// <remarks>
        /// The buffer grows to <c>bufferSize</c> as it is actually needed, rather than being taken
        /// in full up front. That matters when a channel is opened per connection: a large window is
        /// what makes a long round trip fast, but paying for it on every idle channel is what makes
        /// it unaffordable. OpenSSH sizes its channel buffers the same way, growing on demand while
        /// advertising a window far larger than most channels ever use.
        /// </remarks>
        private const int InitialBufferSize = 16 * 1024;

        private static long _windowAdjustsSent;
        private static long _windowBytesCredited;

        private readonly ChannelDirectTcpip _channel;
        private readonly Lock _readLock = new Lock();
        private readonly int _maximumBufferSize;
        private byte[] _received;

        private int _start;
        private int _end;
        private bool _peerEof;
        private bool _peerClosed;
        private int _disposed;

        internal DirectTcpipStream(ChannelDirectTcpip channel, int bufferSize)
        {
            _channel = channel;
            _maximumBufferSize = bufferSize;
            _received = new byte[Math.Min(InitialBufferSize, bufferSize)];

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

        /// <summary>Gets how many window adjustments deferred crediting has sent, across all channels.</summary>
        /// <remarks>
        /// Diagnostics for a throughput investigation: roughly 55 KB stays in flight per round trip
        /// however large the granted window, which is the signature of a sender waiting on credit.
        /// Whether credit actually flows - and in what sizes - is exactly what these observe. They
        /// live here rather than on the channel because the channel type is internal.
        /// </remarks>
        public static long WindowAdjustsSent
        {
            get { return Interlocked.Read(ref _windowAdjustsSent); }
        }

        /// <summary>Gets how many bytes those adjustments credited.</summary>
        public static long WindowBytesCredited
        {
            get { return Interlocked.Read(ref _windowBytesCredited); }
        }

        /// <summary>
        /// Counts one window adjustment and the bytes it credited.
        /// </summary>
        /// <param name="credited">The number of bytes the adjustment credited.</param>
        internal static void CountWindowCredit(uint credited)
        {
            _ = Interlocked.Increment(ref _windowAdjustsSent);
            _ = Interlocked.Add(ref _windowBytesCredited, credited);
        }

        /// <summary>
        /// Gets the window the remote party granted when the channel opened, and how much of it is
        /// still free.
        /// </summary>
        /// <value>
        /// The number of bytes that may still be sent before waiting for an adjustment.
        /// </value>
        /// <remarks>
        /// Exposed for diagnostics. It is what the far end will accept from us, and is the mirror of
        /// the window this side advertises - the pair of them set the throughput a channel can reach
        /// over a given round trip.
        /// </remarks>
        public uint RemoteWindowSize
        {
            get { return _channel.RemoteWindowSize; }
        }

        /// <summary>
        /// Gets the largest data payload the remote party will accept in one message.
        /// </summary>
        public uint RemotePacketSize
        {
            get { return _channel.RemotePacketSize; }
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
        /// Opens the channel this stream was created over.
        /// </summary>
        /// <param name="host">The name or address of the remote host to forward to.</param>
        /// <param name="port">The port of the remote host to forward to.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>A task that represents the open.</returns>
        /// <exception cref="SshException">The server refused the channel, or the session failed while the open was outstanding.</exception>
        /// <remarks>
        /// <para>
        /// The stream subscribes to the channel in its constructor, before this is called, so data
        /// arriving immediately behind the confirmation lands in the buffer rather than being lost -
        /// which is why opening is the stream's job and not something done to a bare channel first.
        /// </para>
        /// <para>
        /// Cancellation means the caller stopped waiting, not that the channel is gone: the server
        /// still owes an answer. A caller that cancels must hand the stream to
        /// <see cref="AbandonAsync"/> rather than <see cref="Dispose"/>, or a confirmation arriving
        /// late leaves the server holding the channel for the life of the session.
        /// </para>
        /// </remarks>
        public Task OpenAsync(string host, uint port, CancellationToken cancellationToken)
        {
            // The originator endpoint is informational; the server may log it. There is no accepted
            // connection behind this channel to take a real one from.
            return _channel.OpenAsync(host, port, "127.0.0.1", 0, cancellationToken);
        }

        /// <summary>
        /// Walks away from a stream whose open the caller no longer wants, without leaking what the
        /// server may still grant.
        /// </summary>
        /// <returns>
        /// A task that completes once the open has settled and the channel has been closed and
        /// disposed. For an unreachable destination that is bounded by the server's own connect
        /// timeout, so callers typically observe this task rather than await it inline.
        /// </returns>
        public Task AbandonAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return Task.CompletedTask;
            }

            Unsubscribe();

            return _channel.AbandonAsync();
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
        /// <see cref="Advance"/> releases it or <see cref="FlushWindowCredit"/> reclaims the
        /// released space, which is what lets a caller retransmit from it rather than keeping a
        /// copy of its own.
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
        /// <para>
        /// Blocks for the duration of a key exchange, like any send.
        /// </para>
        /// <para>
        /// This is also where released buffer space is reclaimed, and the coupling is not a
        /// convenience: the space released at the front of the buffer is exactly what covers the
        /// bytes the credit permits the remote party to send. Crediting without compacting lets
        /// the window outrun the free space at the tail; compacting anywhere else races a consumer
        /// reading a peeked segment. Here it does neither - this runs on the consumer's own
        /// thread, and calling it invalidates any segment previously returned by
        /// <see cref="TryRead"/>, the same way <see cref="Advance"/> does.
        /// </para>
        /// </remarks>
        public void FlushWindowCredit()
        {
            lock (_readLock)
            {
                Compact();
            }

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

            Unsubscribe();

            _channel.Dispose();
        }

        /// <summary>
        /// Disposes the stream without parking a thread on the close handshake.
        /// </summary>
        /// <returns>A task that represents the disposal.</returns>
        /// <remarks>
        /// The close is driven through <see cref="ChannelDirectTcpip.CloseAsync"/> first, so the
        /// <see cref="IDisposable.Dispose"/> the channel still needs afterwards finds the channel
        /// already closed and has nothing left to wait on.
        /// </remarks>
        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            Unsubscribe();

            await _channel.CloseAsync().ConfigureAwait(false);

            _channel.Dispose();
        }

        /// <summary>
        /// Detaches from the channel's notifications.
        /// </summary>
        /// <remarks>
        /// Before disposing, and outside the read lock: the channel's close path waits on the
        /// message listener thread, and taking a lock a notification handler also takes would
        /// deadlock the two against each other.
        /// </remarks>
        private void Unsubscribe()
        {
            _channel.DataReceived -= OnDataReceived;
            _channel.EndOfData -= OnEndOfData;
            _channel.Closed -= OnClosed;
            _channel.WindowAvailable -= OnWindowAvailable;
            _channel.Exception -= OnException;
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
                    // Never compacted in place from here: this runs on the message listener thread,
                    // and a consumer may be reading a segment it peeked with TryRead - an
                    // overlapping copy under its feet tears the bytes it is sending. Torn bytes are
                    // not an error anywhere on this side; they surface as the far end resetting a
                    // perfectly healthy connection, because TLS notices before anything else can.
                    // Grow copies into a fresh array, which the held segment does not point into,
                    // and space is otherwise reclaimed on the consumer's own thread in
                    // FlushWindowCredit.
                    Grow(count);
                }

                if (_received.Length - _end < count)
                {
                    // Only reachable if the buffer cannot reach the window we advertised, which
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

        /// <summary>
        /// Enlarges the receive buffer, up to the maximum the window needs.
        /// </summary>
        /// <param name="needed">How many bytes have to fit beyond what is already held.</param>
        /// <remarks>
        /// Called on the message listener thread, under the read lock. Any segment a consumer is
        /// holding from <c>TryRead</c> stays valid: it refers to the old array, whose contents are
        /// copied rather than altered. At full capacity this degenerates into an out-of-place
        /// compaction - a fresh same-sized array - which is the safe (if allocating) fallback when
        /// space has been released but not yet reclaimed by <c>FlushWindowCredit</c>.
        /// </remarks>
        private void Grow(int needed)
        {
            var held = _end - _start;
            var required = held + needed;

            if (required > _maximumBufferSize)
            {
                return;
            }

            var capacity = _received.Length;

            while (capacity < required)
            {
                capacity = Math.Min(capacity * 2, _maximumBufferSize);
            }

            var bigger = new byte[capacity];

            if (held > 0)
            {
                Buffer.BlockCopy(_received, _start, bigger, 0, held);
            }

            _received = bigger;
            _start = 0;
            _end = held;
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
