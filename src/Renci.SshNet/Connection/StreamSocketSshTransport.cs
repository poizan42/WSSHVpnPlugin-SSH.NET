using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Renci.SshNet.Common;

using Windows.Networking;
using Windows.Networking.Sockets;

namespace Renci.SshNet.Connection
{
    /// <summary>
    /// An <see cref="SshTransport"/> backed by a WinRT <see cref="StreamSocket"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This exists for hosts that must run the SSH session over a WinRT socket rather than a
    /// <see cref="System.Net.Sockets.Socket"/>. A Windows VPN plug-in is the motivating case: it
    /// lives in an app container where only the WinRT socket types are usable, and it needs to
    /// choose the local interface the session runs over so that its own traffic does not route back
    /// into the tunnel it installs. Use <see cref="ConnectAsync(string, int, HostName, ILoggerFactory, CancellationToken)"/>
    /// for that, and <see cref="Socket"/> to reach the socket itself.
    /// </para>
    /// <para>
    /// Both stream adapters are unbuffered. The session frames and buffers itself, and a buffered
    /// writer would sit on outgoing SSH packets rather than sending them.
    /// </para>
    /// <para>
    /// The blocking members wait on the underlying task, which is safe only because no session
    /// thread carries a synchronization context.
    /// </para>
    /// </remarks>
    public sealed class StreamSocketSshTransport : SshTransport
    {
        private readonly StreamSocket _socket;
        private readonly Stream _input;
        private readonly Stream _output;
        private readonly ILogger _logger;

        private int _disposed;
        private int _shutdown;
        private bool _isConnected;

        private StreamSocketSshTransport(StreamSocket socket, ILoggerFactory loggerFactory)
        {
            _socket = socket;
            // The read side is buffered and the write side is not, and the asymmetry is deliberate.
            //
            // Unbuffered writes: the session frames and batches its own packets, and a buffered
            // writer would sit on an outgoing packet until something flushed it.
            //
            // Buffered reads: the adapter then posts reads of the buffer size against the socket,
            // rather than whatever modest length the session asked for. That matters because the
            // operating system's receive-window auto-tuning only opens the underlying TCP window as
            // wide as the reader shows it can absorb - measured with unbuffered reads, the whole
            // tunnel plateaued at the classic 64 KB-window ceiling (about 1.2 MB/s over a 48 ms
            // round trip; 40 reads/s of ~30 KB each, every read spending its whole life waiting for
            // bytes to arrive) while a native socket over the same link ran 30x faster.
            _input = socket.InputStream.AsStreamForRead(bufferSize: 1024 * 1024);
            _output = socket.OutputStream.AsStreamForWrite(bufferSize: 0);
            _logger = loggerFactory.CreateLogger<StreamSocketSshTransport>();
            _isConnected = true;
        }

        /// <summary>
        /// Gets the socket carrying the SSH session.
        /// </summary>
        /// <value>
        /// The <see cref="StreamSocket"/> the session is running over.
        /// </value>
        public StreamSocket Socket
        {
            get { return _socket; }
        }

        /// <inheritdoc/>
        public override bool IsConnected
        {
            get { return _isConnected && Volatile.Read(ref _disposed) == 0; }
        }

        /// <summary>
        /// Connects to the specified SSH endpoint.
        /// </summary>
        /// <param name="host">The host name or address of the SSH server.</param>
        /// <param name="port">The port of the SSH server.</param>
        /// <param name="loggerFactory">The factory used to create the transport's logger.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>
        /// A task that represents the connection attempt. The value of its
        /// <see cref="Task{TResult}.Result"/> is the connected transport.
        /// </returns>
        public static Task<StreamSocketSshTransport> ConnectAsync(
            string host,
            int port,
            ILoggerFactory loggerFactory,
            CancellationToken cancellationToken)
        {
            return ConnectAsync(host, port, localAddress: null, loggerFactory, cancellationToken);
        }

        /// <summary>
        /// Connects to the specified SSH endpoint from a chosen local address.
        /// </summary>
        /// <param name="host">The host name or address of the SSH server.</param>
        /// <param name="port">The port of the SSH server.</param>
        /// <param name="localAddress">
        /// The local address to connect from, or <see langword="null"/> to let the system choose one.
        /// </param>
        /// <param name="loggerFactory">The factory used to create the transport's logger.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>
        /// A task that represents the connection attempt. The value of its
        /// <see cref="Task{TResult}.Result"/> is the connected transport.
        /// </returns>
        /// <remarks>
        /// Binding the source address is what keeps the session on a particular interface. Note that
        /// it also stops the connection from going through a configured proxy, and that the operating
        /// system's forwarding and weak-host settings can still send the packets elsewhere, so this
        /// is a strong hint rather than a guarantee.
        /// </remarks>
        public static async Task<StreamSocketSshTransport> ConnectAsync(
            string host,
            int port,
            HostName localAddress,
            ILoggerFactory loggerFactory,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(loggerFactory);

            var socket = new StreamSocket();
            try
            {
                socket.Control.NoDelay = true;
                socket.Control.KeepAlive = true;

                var service = port.ToString(CultureInfo.InvariantCulture);
                var remoteAddress = new HostName(host);

                if (localAddress is null)
                {
                    await socket.ConnectAsync(remoteAddress, service)
                                .AsTask(cancellationToken)
                                .ConfigureAwait(false);
                }
                else
                {
                    // The local service name has to be the empty string, not null: null is rejected,
                    // and an empty name asks for an ephemeral port.
                    var endpointPair = new EndpointPair(localAddress, string.Empty, remoteAddress, service);

                    await socket.ConnectAsync(endpointPair)
                                .AsTask(cancellationToken)
                                .ConfigureAwait(false);
                }

                return new StreamSocketSshTransport(socket, loggerFactory);
            }
            catch (Exception)
            {
                socket.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Gets how many reads have been issued against the socket, and how many bytes and
        /// microseconds they took.
        /// </summary>
        /// <remarks>
        /// Diagnostics for a throughput investigation. Each read is a separate WinRT operation that
        /// this thread blocks on, so the interesting question is how many of them a given data rate
        /// costs and how long each takes - a small average size with a large per-read cost is the
        /// signature of the transport being the limit rather than anything above it.
        /// </remarks>
        public static long ReadCount;

        /// <summary>Bytes returned by those reads.</summary>
        public static long BytesRead;

        /// <summary>Ticks spent inside those reads.</summary>
        public static long ReadTicks;

        /// <inheritdoc/>
        public override int Read(byte[] buffer, int offset, int count, TimeSpan timeout)
        {
            if (HasShutDown)
            {
                return 0;
            }

            if (timeout == Timeout.InfiniteTimeSpan)
            {
                var started = Stopwatch.GetTimestamp();
                var read = Complete(() => _input.Read(buffer, offset, count));

                _ = Interlocked.Increment(ref ReadCount);
                _ = Interlocked.Add(ref BytesRead, read);
                _ = Interlocked.Add(ref ReadTicks, Stopwatch.GetTimestamp() - started);

                return read;
            }

            using (var cts = new CancellationTokenSource(timeout))
            {
                try
                {
                    return Complete(() => ReadBlocking(buffer, offset, count, cts.Token));
                }
                catch (OperationCanceledException) when (cts.IsCancellationRequested)
                {
                    throw new SshOperationTimeoutException(string.Format(
                        CultureInfo.InvariantCulture,
                        "Socket read operation has timed out after {0:F0} milliseconds.",
                        timeout.TotalMilliseconds));
                }
            }
        }

        /// <inheritdoc/>
        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (HasShutDown)
            {
                return 0;
            }

            int read;
            try
            {
                read = await _input.ReadAsync(buffer.AsMemory(offset, count), cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (IsExpectedDuringTeardown(ex))
            {
                _isConnected = false;
                return 0;
            }
            catch (COMException ex)
            {
                _isConnected = false;
                throw new IOException(ex.Message, ex);
            }

            if (read == 0)
            {
                _isConnected = false;
            }

            return read;
        }

        /// <inheritdoc/>
        public override void Write(byte[] buffer, int offset, int count)
        {
            _output.Write(buffer, offset, count);
        }

        /// <inheritdoc/>
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return _output.WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        }

        /// <inheritdoc/>
        public override void Shutdown()
        {
            _isConnected = false;
            _ = Interlocked.Exchange(ref _shutdown, 1);

            // StreamSocket has no half-close; cancelling pending I/O is what interrupts the blocked
            // read on the message listener thread.
            try
            {
                _ = _socket.CancelIOAsync().AsTask().Wait(TimeSpan.FromSeconds(1));
            }
            catch (Exception ex)
            {
                _logger.LogInformation(ex, "Failure cancelling socket I/O during shutdown");
            }
        }

        /// <inheritdoc/>
        protected override void Dispose(bool disposing)
        {
            if (!disposing || Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            _isConnected = false;
            _ = Interlocked.Exchange(ref _shutdown, 1);

            _input.Dispose();
            _output.Dispose();
            _socket.Dispose();
        }

        /// <summary>
        /// Runs a blocking read, reporting a close rather than an error when the failure is the
        /// result of our own teardown.
        /// </summary>
        /// <param name="read">The read to perform.</param>
        /// <returns>
        /// The number of bytes read, or <c>0</c> if the connection was closed.
        /// </returns>
        private int Complete(Func<int> read)
        {
            int bytesRead;
            try
            {
                bytesRead = read();
            }
            catch (Exception ex) when (IsExpectedDuringTeardown(ex))
            {
                _isConnected = false;
                return 0;
            }
            catch (COMException ex)
            {
                // The WinRT stream adapter reports socket failures as COMException. Present them as
                // an I/O failure so that callers see the same shape of error whichever transport is
                // in use.
                _isConnected = false;
                throw new IOException(ex.Message, ex);
            }

            if (bytesRead == 0)
            {
                _isConnected = false;
            }

            return bytesRead;
        }

        /// <summary>
        /// Performs a cancellable read, blocking until it completes.
        /// </summary>
        /// <param name="buffer">The buffer to write the received bytes to.</param>
        /// <param name="offset">The position in <paramref name="buffer"/> at which to start writing.</param>
        /// <param name="count">The maximum number of bytes to read.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>
        /// The number of bytes read.
        /// </returns>
        private int ReadBlocking(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            // Blocking on a ValueTask that has not completed is not supported, so only that case is
            // turned into a Task. Converting unconditionally would allocate one per read even when
            // the data was already to hand, and the banner exchange reads a byte at a time.
            //
            // Preserve makes the ValueTask safe to inspect and then consume: it hands back itself
            // when the read already completed, and materialises a Task only when it did not, which
            // is the case that needs one regardless.
            var read = _input.ReadAsync(buffer.AsMemory(offset, count), cancellationToken).Preserve();

            return read.IsCompletedSuccessfully
                ? read.Result
                : read.AsTask().GetAwaiter().GetResult();
        }

        /// <summary>
        /// Gets a value indicating whether <see cref="Shutdown"/> or <see cref="Dispose(bool)"/> has run.
        /// </summary>
        private bool HasShutDown
        {
            get { return Volatile.Read(ref _shutdown) != 0; }
        }

        /// <summary>
        /// Determines whether an exception is the expected consequence of <see cref="Shutdown"/> or
        /// <see cref="Dispose(bool)"/>, in which case the session should see a clean close instead
        /// of an error. Anything else is a genuine failure and is left to propagate.
        /// </summary>
        /// <param name="exception">The exception to classify.</param>
        /// <returns>
        /// <see langword="true"/> if the exception is a consequence of teardown; otherwise, <see langword="false"/>.
        /// </returns>
        private static bool IsTeardownStatus(Exception exception)
        {
            // The WinRT stream adapter surfaces socket failures as COMException, sometimes wrapped
            // in an IOException, so the status can be on either.
            var hresult = exception is COMException
                ? exception.HResult
                : exception.InnerException?.HResult ?? exception.HResult;

            // Verified by test: interrupting a blocked read with CancelIOAsync surfaces as an
            // IOException whose status is OperationAborted, and a peer reset as ConnectionResetByPeer.
            return SocketError.GetStatus(hresult) is SocketErrorStatus.OperationAborted
                or SocketErrorStatus.ConnectionResetByPeer
                or SocketErrorStatus.NetworkDroppedConnectionOnReset
                or SocketErrorStatus.SoftwareCausedConnectionAbort;
        }

        private bool IsExpectedDuringTeardown(Exception exception)
        {
            if (!HasShutDown)
            {
                return false;
            }

            // Disposing the adapters and cancelling the read are our own doing, so those types are
            // unambiguous. Anything else has to name a socket status that actually means the
            // connection went away - a COMException reporting something unrelated is a real failure
            // and must not be mistaken for an orderly close just because it landed during teardown.
            return exception is ObjectDisposedException or OperationCanceledException
                || IsTeardownStatus(exception);
        }
    }
}
