using System;
using System.Threading;
using System.Threading.Tasks;

namespace Renci.SshNet.Connection
{
    /// <summary>
    /// Represents the byte transport that carries an SSH session.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The built-in transport is a <see cref="System.Net.Sockets.Socket"/>. This abstraction exists so
    /// that a host which cannot use one - such as a Windows VPN plug-in, which runs in an app container
    /// where the WinRT socket types are what is available - can supply its own.
    /// </para>
    /// <para>
    /// Implementations must be safe for a single reader and a single writer running concurrently;
    /// the session reads on a dedicated thread while other threads send.
    /// </para>
    /// </remarks>
    public abstract class SshTransport : IDisposable
    {
        /// <summary>
        /// Gets a value indicating whether the transport is connected.
        /// </summary>
        /// <value>
        /// <see langword="true"/> if the transport is connected; otherwise, <see langword="false"/>.
        /// </value>
        public abstract bool IsConnected { get; }

        /// <summary>
        /// Reads up to <paramref name="count"/> bytes, blocking until at least one byte is available.
        /// </summary>
        /// <param name="buffer">The buffer to write the received bytes to.</param>
        /// <param name="offset">The position in <paramref name="buffer"/> at which to start writing.</param>
        /// <param name="count">The maximum number of bytes to read.</param>
        /// <param name="timeout">The maximum time to wait for data, or <see cref="Timeout.InfiniteTimeSpan"/> to wait indefinitely.</param>
        /// <returns>
        /// The number of bytes read, or <c>0</c> if the remote party closed the connection.
        /// </returns>
        /// <exception cref="Common.SshOperationTimeoutException">The read timed out.</exception>
        public abstract int Read(byte[] buffer, int offset, int count, TimeSpan timeout);

        /// <summary>
        /// Asynchronously reads up to <paramref name="count"/> bytes.
        /// </summary>
        /// <param name="buffer">The buffer to write the received bytes to.</param>
        /// <param name="offset">The position in <paramref name="buffer"/> at which to start writing.</param>
        /// <param name="count">The maximum number of bytes to read.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>
        /// A task that represents the read. The value of its <see cref="Task{TResult}.Result"/> is the
        /// number of bytes read, or <c>0</c> if the remote party closed the connection.
        /// </returns>
        public abstract Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken);

        /// <summary>
        /// Writes <paramref name="count"/> bytes, blocking until all of them have been handed to the transport.
        /// </summary>
        /// <param name="buffer">The buffer holding the bytes to send.</param>
        /// <param name="offset">The position in <paramref name="buffer"/> at which to start reading.</param>
        /// <param name="count">The number of bytes to send.</param>
        public abstract void Write(byte[] buffer, int offset, int count);

        /// <summary>
        /// Asynchronously writes <paramref name="count"/> bytes.
        /// </summary>
        /// <param name="buffer">The buffer holding the bytes to send.</param>
        /// <param name="offset">The position in <paramref name="buffer"/> at which to start reading.</param>
        /// <param name="count">The number of bytes to send.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>
        /// A task that represents the write.
        /// </returns>
        public abstract Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken);

        /// <summary>
        /// Disables sending and receiving, interrupting any blocked read.
        /// </summary>
        /// <remarks>
        /// This is called before <see cref="Dispose()"/> when the session is torn down. It must not throw
        /// if the transport is already closed.
        /// </remarks>
        public abstract void Shutdown();

        /// <summary>
        /// Releases all resources used by the transport.
        /// </summary>
        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases the unmanaged resources used by the transport and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to release both managed and unmanaged resources; <see langword="false"/> to release only unmanaged resources.</param>
        protected abstract void Dispose(bool disposing);
    }
}
