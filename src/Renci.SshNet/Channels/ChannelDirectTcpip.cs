using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Renci.SshNet.Abstractions;
using Renci.SshNet.Common;
using Renci.SshNet.Messages.Connection;

namespace Renci.SshNet.Channels
{
    /// <summary>
    /// Implements "direct-tcpip" SSH channel.
    /// </summary>
    internal sealed class ChannelDirectTcpip : ClientChannel, IChannelDirectTcpip
    {
        private readonly Lock _socketLock = new Lock();
        private readonly ILogger _logger;
        private EventWaitHandle _channelOpen = new AutoResetEvent(initialState: false);
        private EventWaitHandle _channelData = new AutoResetEvent(initialState: false);
        private IForwardedPort _forwardedPort;
        private Socket _socket;
        private TaskCompletionSource<bool> _openCompletion;
        private uint _openFailureReason;
        private string _openFailureDescription;

        /// <summary>
        /// Initializes a new instance of the <see cref="ChannelDirectTcpip"/> class.
        /// </summary>
        /// <param name="session">The session.</param>
        /// <param name="localChannelNumber">The local channel number.</param>
        /// <param name="localWindowSize">Size of the window.</param>
        /// <param name="localPacketSize">Size of the packet.</param>
        public ChannelDirectTcpip(ISession session, uint localChannelNumber, uint localWindowSize, uint localPacketSize)
            : base(session, localChannelNumber, localWindowSize, localPacketSize)
        {
            _logger = session.SessionLoggerFactory.CreateLogger<ChannelDirectTcpip>();
        }

        /// <summary>
        /// Gets the type of the channel.
        /// </summary>
        /// <value>
        /// The type of the channel.
        /// </value>
        public override ChannelTypes ChannelType
        {
            get { return ChannelTypes.DirectTcpip; }
        }

        public void Open(string remoteHost, uint port, IForwardedPort forwardedPort, Socket socket)
        {
            EnsureCanOpen();

            _socket = socket;
            _forwardedPort = forwardedPort;
            _forwardedPort.Closing += ForwardedPort_Closing;

            var ep = (IPEndPoint)socket.RemoteEndPoint;

            SendChannelOpen(remoteHost, port, ep.Address.ToString(), (uint)ep.Port);

            // Wait for channel to open
            WaitOnHandle(_channelOpen);
        }

        /// <summary>
        /// Opens a channel to a remote host without binding it to a socket.
        /// </summary>
        /// <param name="remoteHost">The name of the remote host to forward to.</param>
        /// <param name="port">The port of the remote host to forward to.</param>
        /// <param name="originatorAddress">The address to report as the originator of the connection.</param>
        /// <param name="originatorPort">The port to report as the originator of the connection.</param>
        /// <exception cref="SshException">The channel is already open, the session is not connected, or the server refused the channel.</exception>
        /// <remarks>
        /// The originator endpoint is informational: the protocol carries it to the server, which may
        /// log it, and nothing depends on it locally. Callers that are not forwarding an accepted
        /// connection can report whatever identifies the flow.
        /// </remarks>
        public void Open(string remoteHost, uint port, string originatorAddress, uint originatorPort)
        {
            EnsureCanOpen();

            SendChannelOpen(remoteHost, port, originatorAddress, originatorPort);

            WaitOnHandle(_channelOpen);

            if (!IsOpen)
            {
                throw CreateOpenFailedException();
            }
        }

        /// <summary>
        /// Opens a channel to a remote host without binding it to a socket, asynchronously.
        /// </summary>
        /// <param name="remoteHost">The name of the remote host to forward to.</param>
        /// <param name="port">The port of the remote host to forward to.</param>
        /// <param name="originatorAddress">The address to report as the originator of the connection.</param>
        /// <param name="originatorPort">The port to report as the originator of the connection.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>A task that represents the open.</returns>
        /// <exception cref="SshException">The channel is already open, the session is not connected, or the server refused the channel.</exception>
        /// <remarks>
        /// The returned task also faults when the session fails or is disconnected while the open is
        /// outstanding. Open confirmation and open failure are not the only ways this can end, and
        /// without that a dropped connection would leave the caller waiting indefinitely.
        /// </remarks>
        public async Task OpenAsync(string remoteHost, uint port, string originatorAddress, uint originatorPort, CancellationToken cancellationToken)
        {
            EnsureCanOpen();
            cancellationToken.ThrowIfCancellationRequested();

            // Continuations run off the message listener thread: whatever the caller does next must
            // not execute inside the session's message loop.
            var completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _openCompletion = completion;

            SendChannelOpen(remoteHost, port, originatorAddress, originatorPort);

            using (cancellationToken.Register(static state => ((TaskCompletionSource<bool>)state).TrySetCanceled(), completion, useSynchronizationContext: false))
            {
                _ = await completion.Task.ConfigureAwait(false);
            }

            if (!IsOpen)
            {
                throw CreateOpenFailedException();
            }
        }

        private void EnsureCanOpen()
        {
            if (IsOpen)
            {
                throw new SshException("Channel is already open.");
            }

            if (!IsConnected)
            {
                throw new SshException("Session is not connected.");
            }
        }

        private void SendChannelOpen(string remoteHost, uint port, string originatorAddress, uint originatorPort)
        {
            SendMessage(new ChannelOpenMessage(LocalChannelNumber,
                                               LocalWindowSize,
                                               LocalPacketSize,
                                               new DirectTcpipChannelInfo(remoteHost, port, originatorAddress, originatorPort)));
        }

        private SshException CreateOpenFailedException()
        {
            return new SshException(string.Format(
                System.Globalization.CultureInfo.InvariantCulture,
                "The server refused to open the channel: {0} (reason {1}).",
                _openFailureDescription ?? "no description given",
                _openFailureReason));
        }

        /// <summary>
        /// Occurs as the forwarded port is being stopped.
        /// </summary>
        private void ForwardedPort_Closing(object sender, EventArgs eventArgs)
        {
            // signal to the client that we will not send anything anymore; this should also interrupt the
            // blocking receive in Bind if the client sends FIN/ACK in time
            ShutdownSocket(SocketShutdown.Send);

            // if the FIN/ACK is not sent in time by the remote client, then interrupt the blocking receive
            // by closing the socket
            CloseSocket();
        }

        /// <summary>
        /// Binds channel to remote host.
        /// </summary>
        public void Bind()
        {
            // Cannot bind if channel is not open
            if (!IsOpen)
            {
                return;
            }

            var buffer = new byte[RemotePacketSize];

            SocketAbstraction.ReadContinuous(_socket, buffer, 0, buffer.Length, SendData);

            // even though the client has disconnected, we still want to properly close the
            // channel
            //
            // we'll do this in in Close() - invoked through Dispose(bool) - that way we have
            // a single place from which we send an SSH_MSG_CHANNEL_EOF message and wait for
            // the SSH_MSG_CHANNEL_CLOSE message
        }

        /// <summary>
        /// Closes the socket, hereby interrupting the blocking receive in <see cref="Bind()"/>.
        /// </summary>
        private void CloseSocket()
        {
            if (_socket is null)
            {
                return;
            }

            lock (_socketLock)
            {
                if (_socket is null)
                {
                    return;
                }

                // closing a socket actually disposes the socket, so we can safely dereference
                // the field to avoid entering the lock again later
                _socket.Dispose();
                _socket = null;
            }
        }

        /// <summary>
        /// Shuts down the socket.
        /// </summary>
        /// <param name="how">One of the <see cref="SocketShutdown"/> values that specifies the operation that will no longer be allowed.</param>
        private void ShutdownSocket(SocketShutdown how)
        {
            if (_socket is null)
            {
                return;
            }

            lock (_socketLock)
            {
                if (!_socket.IsConnected())
                {
                    return;
                }

                try
                {
                    _socket.Shutdown(how);
                }
                catch (SocketException ex)
                {
                    _logger.LogInformation(ex, "Failure shutting down socket");
                }
            }
        }

        /// <summary>
        /// Closes the channel, waiting for the SSH_MSG_CHANNEL_CLOSE message to be received from the server.
        /// </summary>
        protected override void Close()
        {
            var forwardedPort = _forwardedPort;
            if (forwardedPort != null)
            {
                forwardedPort.Closing -= ForwardedPort_Closing;
                _forwardedPort = null;
            }

            // signal to the client that we will not send anything anymore; this will also interrupt the
            // blocking receive in Bind if the client sends FIN/ACK in time
            //
            // if the FIN/ACK is not sent in time, the socket will be closed after the channel is closed
            ShutdownSocket(SocketShutdown.Send);

            // close the SSH channel
            base.Close();

            // close the socket
            CloseSocket();
        }

        /// <summary>
        /// Called when channel data is received.
        /// </summary>
        /// <param name="data">The data.</param>
        protected override void OnData(ArraySegment<byte> data)
        {
            base.OnData(data);

            if (_socket != null)
            {
                lock (_socketLock)
                {
                    if (_socket.IsConnected())
                    {
                        SocketAbstraction.Send(_socket, data.Array, data.Offset, data.Count);
                    }
                }
            }
        }

        /// <summary>
        /// Called when channel is opened by the server.
        /// </summary>
        /// <param name="remoteChannelNumber">The remote channel number.</param>
        /// <param name="initialWindowSize">Initial size of the window.</param>
        /// <param name="maximumPacketSize">Maximum size of the packet.</param>
        protected override void OnOpenConfirmation(uint remoteChannelNumber, uint initialWindowSize, uint maximumPacketSize)
        {
            base.OnOpenConfirmation(remoteChannelNumber, initialWindowSize, maximumPacketSize);

            _ = _channelOpen?.Set();
            _ = _openCompletion?.TrySetResult(true);
        }

        protected override void OnOpenFailure(uint reasonCode, string description, string language)
        {
            base.OnOpenFailure(reasonCode, description, language);

            _openFailureReason = reasonCode;
            _openFailureDescription = description;

            _ = _channelOpen?.Set();

            // Completed rather than faulted: the caller is told by the IsOpen check, which keeps the
            // sync and async paths reporting a refusal the same way.
            _ = _openCompletion?.TrySetResult(false);
        }

        /// <summary>
        /// Called when channel has no more data to receive.
        /// </summary>
        protected override void OnEof()
        {
            base.OnEof();

            // the channel will send no more data, and hence it does not make sense to receive
            // any more data from the client to send to the remote party (and we surely won't
            // send anything anymore)
            //
            // this will also interrupt the blocking receive in Bind()
            ShutdownSocket(SocketShutdown.Send);
        }

        /// <summary>
        /// Called whenever an unhandled <see cref="Exception"/> occurs in <see cref="Session"/> causing
        /// the message loop to be interrupted, or when an exception occurred processing a channel message.
        /// </summary>
        protected override void OnErrorOccurred(Exception exp)
        {
            base.OnErrorOccurred(exp);

            // An open in flight ends here too, not only at confirmation or failure.
            _ = _openCompletion?.TrySetException(exp);

            // signal to the client that we will not send anything anymore; this will also interrupt the
            // blocking receive in Bind if the client sends FIN/ACK in time
            //
            // if the FIN/ACK is not sent in time, the socket will be closed in Close(bool)
            ShutdownSocket(SocketShutdown.Send);
        }

        /// <summary>
        /// Called when the server wants to terminate the connection immediately.
        /// </summary>
        /// <remarks>
        /// The sender MUST NOT send or receive any data after this message, and
        /// the recipient MUST NOT accept any data after receiving this message.
        /// </remarks>
        protected override void OnDisconnected()
        {
            base.OnDisconnected();

            _ = _openCompletion?.TrySetException(
                new SshConnectionException("The session was disconnected while the channel was being opened."));

            // the channel will accept or send no more data, and hence it does not make sense
            // to accept any more data from the client (and we surely won't send anything
            // anymore)
            //
            // so lets signal to the client that we will not send or receive anything anymore
            // this will also interrupt the blocking receive in Bind()
            ShutdownSocket(SocketShutdown.Both);
        }

        protected override void Dispose(bool disposing)
        {
            // make sure we've unsubscribed from all session events and closed the channel
            // before we starting disposing
            base.Dispose(disposing);

            if (disposing)
            {
                if (_socket != null)
                {
                    lock (_socketLock)
                    {
                        var socket = _socket;
                        if (socket != null)
                        {
                            _socket = null;
                            socket.Dispose();
                        }
                    }
                }

                var channelOpen = _channelOpen;
                if (channelOpen != null)
                {
                    _channelOpen = null;
                    channelOpen.Dispose();
                }

                var channelData = _channelData;
                if (channelData != null)
                {
                    _channelData = null;
                    channelData.Dispose();
                }
            }
        }
    }
}
