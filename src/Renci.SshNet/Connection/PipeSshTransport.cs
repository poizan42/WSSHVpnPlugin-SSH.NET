#nullable enable
using System;
using System.Threading;
using System.Threading.Tasks;

using Renci.SshNet.Common;

namespace Renci.SshNet.Connection
{
    /// <summary>
    /// The delegate a <see cref="PipeSshTransport"/> invokes to put outgoing bytes on the wire.
    /// </summary>
    /// <param name="bytes">The bytes to send, valid only for the duration of the call.</param>
    public delegate void PipeSshTransportSend(ReadOnlySpan<byte> bytes);

    /// <summary>
    /// A transport whose wire is owned by the host: received bytes are handed in through
    /// <see cref="Deliver"/>, and outgoing bytes are handed out through a delegate.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Exists for the Windows VPN plug-in's platform-owned-transport architecture, where the VPN
    /// platform owns the TCP socket to the SSH server and the plug-in only sees bytes: inbound as
    /// decapsulate deliveries, outbound as send-buffer appends. The session neither owns a socket
    /// nor knows any of that; it reads and writes a byte stream.
    /// </para>
    /// <para>
    /// The receive side is a growable buffer guarded by one monitor. Growth is out-of-place — the
    /// consumer copies out under the same lock, so no array is ever mutated while a reader holds a
    /// reference into it. <see cref="Read"/> maps its timeout directly onto
    /// <see cref="Monitor.Wait(object, TimeSpan)"/>; the async members wrap the synchronous core,
    /// which is the only path the plug-in uses.
    /// </para>
    /// </remarks>
    public sealed class PipeSshTransport : SshTransport
    {
        private const int InitialCapacity = 64 * 1024;

        private readonly object _gate = new object();
        private readonly PipeSshTransportSend _send;

        /// <summary>Received bytes in [<see cref="_start"/>, <see cref="_end"/>).</summary>
        private byte[] _buffer = new byte[InitialCapacity];
        private int _start;
        private int _end;

        private bool _shutdown;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="PipeSshTransport"/> class.
        /// </summary>
        /// <param name="send">The delegate that puts outgoing bytes on the wire.</param>
        public PipeSshTransport(PipeSshTransportSend send)
        {
            if (send is null)
            {
                throw new ArgumentNullException(nameof(send));
            }

            _send = send;
        }

        /// <inheritdoc/>
        public override bool IsConnected
        {
            get
            {
                lock (_gate)
                {
                    return !_shutdown && !_disposed;
                }
            }
        }

        /// <summary>
        /// Hands received wire bytes to the transport, waking a blocked <see cref="Read"/>.
        /// </summary>
        /// <param name="bytes">The received bytes; copied before the call returns.</param>
        /// <remarks>
        /// Silently discards the bytes once the transport is shut down or disposed: teardown races
        /// deliveries by nature, and a delivery losing that race has nowhere to go.
        /// </remarks>
        public void Deliver(ReadOnlySpan<byte> bytes)
        {
            if (bytes.IsEmpty)
            {
                return;
            }

            lock (_gate)
            {
                if (_shutdown || _disposed)
                {
                    return;
                }

                var pending = _end - _start;
                if (_buffer.Length - _end < bytes.Length)
                {
                    if (pending + bytes.Length <= _buffer.Length && _start >= _buffer.Length / 2)
                    {
                        // Enough total room; slide the pending bytes down. The reader only touches
                        // the array under this same lock, so the in-place copy has no observer.
                        Array.Copy(_buffer, _start, _buffer, 0, pending);
                    }
                    else
                    {
                        var grown = new byte[Math.Max(_buffer.Length * 2, pending + bytes.Length)];
                        Array.Copy(_buffer, _start, grown, 0, pending);
                        _buffer = grown;
                    }

                    _start = 0;
                    _end = pending;
                }

                bytes.CopyTo(_buffer.AsSpan(_end));
                _end += bytes.Length;

                Monitor.PulseAll(_gate);
            }
        }

        /// <inheritdoc/>
        public override int Read(byte[] buffer, int offset, int count, TimeSpan timeout)
        {
            if (count == 0)
            {
                return 0;
            }

            lock (_gate)
            {
                while (_end == _start)
                {
                    if (_shutdown || _disposed)
                    {
                        return 0;
                    }

                    if (timeout == Timeout.InfiniteTimeSpan)
                    {
                        _ = Monitor.Wait(_gate);
                    }
                    else if (!Monitor.Wait(_gate, timeout))
                    {
                        throw new SshOperationTimeoutException("The read timed out.");
                    }
                }

                var read = Math.Min(count, _end - _start);
                Array.Copy(_buffer, _start, buffer, offset, read);
                _start += read;

                if (_start == _end)
                {
                    _start = 0;
                    _end = 0;
                }

                return read;
            }
        }

        /// <inheritdoc/>
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            // The plug-in drives the session through the synchronous API only; a thread-hopping
            // wrapper keeps the contract honoured for anything else without complicating the core.
            return Task.Run(() => Read(buffer, offset, count, Timeout.InfiniteTimeSpan), cancellationToken);
        }

        /// <inheritdoc/>
        public override void Write(byte[] buffer, int offset, int count)
        {
            if (count == 0)
            {
                return;
            }

            lock (_gate)
            {
                if (_shutdown || _disposed)
                {
                    throw new SshConnectionException("The transport is closed.");
                }
            }

            // Outside the gate: the send delegate crosses into platform calls that must not block
            // deliveries. The session already serializes writers (SendPacket runs under its own
            // lock), which is the single-writer half of the transport contract.
            _send(new ReadOnlySpan<byte>(buffer, offset, count));
        }

        /// <inheritdoc/>
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return Task.Run(() => Write(buffer, offset, count), cancellationToken);
        }

        /// <inheritdoc/>
        public override void Shutdown()
        {
            lock (_gate)
            {
                _shutdown = true;
                Monitor.PulseAll(_gate);
            }
        }

        /// <inheritdoc/>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                lock (_gate)
                {
                    _disposed = true;
                    _shutdown = true;
                    Monitor.PulseAll(_gate);
                }
            }
        }
    }
}
