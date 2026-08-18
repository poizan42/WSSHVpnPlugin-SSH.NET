using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Renci.SshNet.Common;
using Renci.SshNet.Messages;
using Renci.SshNet.Messages.Connection;

namespace Renci.SshNet.Channels
{
    /// <summary>
    /// Represents base class for SSH channel implementations.
    /// </summary>
    internal abstract class Channel : IChannel
    {
        private readonly Lock _serverWindowSizeLock = new Lock();
        private readonly Lock _messagingLock = new Lock();
        private readonly Lock _sendDataLock = new Lock();
        private readonly Lock _localWindowLock = new Lock();
        private readonly uint _initialWindowSize;
        private readonly ISession _session;
        private readonly ILogger _logger;

        /// <summary>
        /// Completes when the server's SSH_MSG_CHANNEL_CLOSE arrives, or when the session dies and
        /// it never will. The asynchronous mirror of <see cref="_channelClosedWaitHandle"/>: a wait
        /// on the handle parks a thread, and the whole point of <see cref="CloseAsync"/> is not to.
        /// </summary>
        private readonly TaskCompletionSource<bool> _channelClosedCompletion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        private EventWaitHandle _channelClosedWaitHandle = new ManualResetEvent(initialState: false);
        private EventWaitHandle _channelServerWindowAdjustWaitHandle = new ManualResetEvent(initialState: false);
        private uint? _remoteWindowSize;
        private uint? _remoteChannelNumber;
        private uint? _remotePacketSize;

        /// <summary>
        /// 0 until disposed. Interlocked, because disposal may arrive from a reaper worker while
        /// another teardown path races it - the dispose body must run exactly once regardless of
        /// thread.
        /// </summary>
        private int _isDisposed;

        /// <summary>
        /// Bytes the consumer has released that have not yet been credited to the remote party.
        /// </summary>
        /// <remarks>
        /// Only used when <see cref="DeferWindowCredit"/> is set. Credits are batched rather than
        /// sent per release, because a window adjust per read would put a message on the wire for
        /// every few bytes consumed.
        /// </remarks>
        private uint _uncreditedBytes;

        /// <summary>
        /// Holds a value indicating whether the SSH_MSG_CHANNEL_CLOSE has been sent to the remote party.
        /// </summary>
        /// <value>
        /// <see langword="true"/> when a SSH_MSG_CHANNEL_CLOSE message has been sent to the other party;
        /// otherwise, <see langword="false"/>.
        /// </value>
        private bool _closeMessageSent;

        /// <summary>
        /// Holds a value indicating whether a SSH_MSG_CHANNEL_CLOSE has been received from the other
        /// party.
        /// </summary>
        /// <value>
        /// <see langword="true"/> when a SSH_MSG_CHANNEL_CLOSE message has been received from the other party;
        /// otherwise, <see langword="false"/>.
        /// </value>
        private bool _closeMessageReceived;

        /// <summary>
        /// Holds a value indicating whether the SSH_MSG_CHANNEL_EOF has been received from the other party.
        /// </summary>
        /// <value>
        /// <see langword="true"/> when a SSH_MSG_CHANNEL_EOF message has been received from the other party;
        /// otherwise, <see langword="false"/>.
        /// </value>
        private bool _eofMessageReceived;

        /// <summary>
        /// Holds a value indicating whether the SSH_MSG_CHANNEL_EOF has been sent to the remote party.
        /// </summary>
        /// <value>
        /// <see langword="true"/> when a SSH_MSG_CHANNEL_EOF message has been sent to the remote party;
        /// otherwise, <see langword="false"/>.
        /// </value>
        private bool _eofMessageSent;

        /// <summary>
        /// Occurs when an exception is thrown when processing channel messages.
        /// </summary>
        public event EventHandler<ExceptionEventArgs> Exception;

        /// <summary>
        /// Occurs when the remote party has enlarged the window, so that a send which previously
        /// reported <see cref="ChannelSendResult.WindowFull"/> may now make progress.
        /// </summary>
        /// <remarks>
        /// Raised on the session's message listener thread. Handlers must be O(1) and must not block
        /// - setting a flag and queueing work is the intended shape. Anything that waits here stalls
        /// every channel on the session, because one thread dispatches them all.
        /// </remarks>
        public event EventHandler<EventArgs> WindowAvailable;

        /// <summary>
        /// Gets or sets a value indicating whether the local window is credited when the consumer
        /// releases bytes rather than when they arrive.
        /// </summary>
        /// <value>
        /// <see langword="true"/> to credit on release; otherwise, <see langword="false"/>, which
        /// credits on receipt. The default is <see langword="false"/>.
        /// </value>
        /// <remarks>
        /// <para>
        /// Crediting on receipt means the window is never really backpressure: the remote party is
        /// told it may send more as soon as the bytes arrive, whether or not anything has consumed
        /// them. That is fine for a consumer that drains promptly, and wrong for one that wants the
        /// window to throttle a producer it cannot keep up with.
        /// </para>
        /// <para>
        /// A channel that sets this <em>must</em> call <see cref="ReleaseReceivedData"/> as it
        /// consumes, and flush the credit, or the window shrinks to nothing and the remote party
        /// stops sending. It is off by default so that existing consumers are unaffected.
        /// </para>
        /// </remarks>
        public bool DeferWindowCredit { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="Channel"/> class.
        /// </summary>
        /// <param name="session">The session.</param>
        /// <param name="localChannelNumber">The local channel number.</param>
        /// <param name="localWindowSize">Size of the window.</param>
        /// <param name="localPacketSize">Size of the packet.</param>
        protected Channel(ISession session, uint localChannelNumber, uint localWindowSize, uint localPacketSize)
        {
            _session = session;
            _initialWindowSize = localWindowSize;
            LocalChannelNumber = localChannelNumber;
            LocalPacketSize = localPacketSize;
            LocalWindowSize = localWindowSize;
            _logger = session.SessionLoggerFactory.CreateLogger(GetType());

            session.ChannelWindowAdjustReceived += OnChannelWindowAdjust;
            session.ChannelDataReceived += OnChannelData;
            session.ChannelExtendedDataReceived += OnChannelExtendedData;
            session.ChannelEofReceived += OnChannelEof;
            session.ChannelCloseReceived += OnChannelClose;
            session.ChannelRequestReceived += OnChannelRequest;
            session.ChannelSuccessReceived += OnChannelSuccess;
            session.ChannelFailureReceived += OnChannelFailure;
            session.ErrorOccured += Session_ErrorOccurred;
            session.Disconnected += Session_Disconnected;
        }

        /// <summary>
        /// Gets the session.
        /// </summary>
        /// <value>
        ///  The session.
        /// </value>
        protected ISession Session
        {
            get { return _session; }
        }

        /// <summary>
        /// Gets the type of the channel.
        /// </summary>
        /// <value>
        /// The type of the channel.
        /// </value>
        public abstract ChannelTypes ChannelType { get; }

        /// <summary>
        /// Gets the local channel number.
        /// </summary>
        /// <value>
        /// The local channel number.
        /// </value>
        public uint LocalChannelNumber { get; private set; }

        /// <summary>
        /// Gets the maximum size of a data packet that we can receive using the channel.
        /// </summary>
        /// <value>
        /// The maximum size of a packet.
        /// </value>
        /// <remarks>
        /// <para>
        /// This is the maximum size (in bytes) we support for the data (payload) of a
        /// <c>SSH_MSG_CHANNEL_DATA</c> message we receive.
        /// </para>
        /// <para>
        /// We currently do not enforce this limit.
        /// </para>
        /// </remarks>
        public uint LocalPacketSize { get; private set; }

        /// <summary>
        /// Gets the size of the local window.
        /// </summary>
        /// <value>
        /// The size of the local window.
        /// </value>
        public uint LocalWindowSize { get; private set; }

        /// <summary>
        /// Gets the remote channel number.
        /// </summary>
        /// <value>
        /// The remote channel number.
        /// </value>
        public uint RemoteChannelNumber
        {
            get
            {
                if (!_remoteChannelNumber.HasValue)
                {
                    throw CreateRemoteChannelInfoNotAvailableException();
                }

                return _remoteChannelNumber.Value;
            }
            private set
            {
                _remoteChannelNumber = value;
            }
        }

        /// <summary>
        /// Gets the maximum size of a data packet that we can send using the channel.
        /// </summary>
        /// <value>
        /// The maximum size of data that can be sent using a <see cref="ChannelDataMessage"/>
        /// on the current channel.
        /// </value>
        /// <exception cref="InvalidOperationException">The channel has not been opened, or the open has not yet been confirmed.</exception>
        public uint RemotePacketSize
        {
            get
            {
                if (!_remotePacketSize.HasValue)
                {
                    throw CreateRemoteChannelInfoNotAvailableException();
                }

                return _remotePacketSize.Value;
            }
            private set
            {
                _remotePacketSize = value;
            }
        }

        /// <summary>
        /// Gets the window size of the remote server.
        /// </summary>
        /// <value>
        /// The size of the server window.
        /// </value>
        public uint RemoteWindowSize
        {
            get
            {
                if (!_remoteWindowSize.HasValue)
                {
                    throw CreateRemoteChannelInfoNotAvailableException();
                }

                return _remoteWindowSize.Value;
            }
            private set
            {
                _remoteWindowSize = value;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether this channel is open.
        /// </summary>
        /// <value>
        /// <see langword="true"/> if this channel is open; otherwise, <see langword="false"/>.
        /// </value>
        public bool IsOpen { get; protected set; }

        /// <summary>
        /// Occurs when <see cref="ChannelDataMessage"/> is received.
        /// </summary>
        public event EventHandler<ChannelDataEventArgs> DataReceived;

        /// <summary>
        /// Occurs when <see cref="ChannelExtendedDataMessage"/> is received.
        /// </summary>
        public event EventHandler<ChannelExtendedDataEventArgs> ExtendedDataReceived;

        /// <summary>
        /// Occurs when <see cref="ChannelEofMessage"/> is received.
        /// </summary>
        public event EventHandler<ChannelEventArgs> EndOfData;

        /// <summary>
        /// Occurs when <see cref="ChannelCloseMessage"/> is received.
        /// </summary>
        public event EventHandler<ChannelEventArgs> Closed;

        /// <summary>
        /// Occurs when <see cref="ChannelRequestMessage"/> is received.
        /// </summary>
        public event EventHandler<ChannelRequestEventArgs> RequestReceived;

        /// <summary>
        /// Occurs when <see cref="ChannelSuccessMessage"/> is received.
        /// </summary>
        public event EventHandler<ChannelEventArgs> RequestSucceeded;

        /// <summary>
        /// Occurs when <see cref="ChannelFailureMessage"/> is received.
        /// </summary>
        public event EventHandler<ChannelEventArgs> RequestFailed;

        /// <summary>
        /// Gets a value indicating whether the session is connected.
        /// </summary>
        /// <value>
        /// <see langword="true"/> if the session is connected; otherwise, <see langword="false"/>.
        /// </value>
        protected bool IsConnected
        {
            get { return _session.IsConnected; }
        }

        /// <summary>
        /// Gets the connection info.
        /// </summary>
        /// <value>The connection info.</value>
        protected IConnectionInfo ConnectionInfo
        {
            get { return _session.ConnectionInfo; }
        }

        /// <summary>
        /// Gets the session semaphore to control number of session channels.
        /// </summary>
        /// <value>The session semaphore.</value>
        protected SemaphoreSlim SessionSemaphore
        {
            get { return _session.SessionSemaphore; }
        }

        /// <summary>
        /// Initializes the information on the remote channel.
        /// </summary>
        /// <param name="remoteChannelNumber">The remote channel number.</param>
        /// <param name="remoteWindowSize">The remote window size.</param>
        /// <param name="remotePacketSize">The remote packet size.</param>
        protected void InitializeRemoteInfo(uint remoteChannelNumber, uint remoteWindowSize, uint remotePacketSize)
        {
            RemoteChannelNumber = remoteChannelNumber;
            RemoteWindowSize = remoteWindowSize;
            RemotePacketSize = remotePacketSize;
        }

        /// <summary>
        /// Sends a SSH_MSG_CHANNEL_DATA message with the specified payload.
        /// </summary>
        /// <param name="data">The payload to send.</param>
        public void SendData(byte[] data)
        {
            SendData(data, 0, data.Length);
        }

        /// <summary>
        /// Sends a SSH_MSG_CHANNEL_DATA message with the specified payload.
        /// </summary>
        /// <param name="data">An array of <see cref="byte"/> containing the payload to send.</param>
        /// <param name="offset">The zero-based offset in <paramref name="data"/> at which to begin taking data from.</param>
        /// <param name="size">The number of bytes of <paramref name="data"/> to send.</param>
        /// <remarks>
        /// <para>
        /// When the size of the data to send exceeds the maximum packet size or the remote window
        /// size does not allow the full data to be sent, then this method will send the data in
        /// multiple chunks and will wait for the remote window size to be adjusted when it's zero.
        /// </para>
        /// <para>
        /// This is done to support SSH servers will a small window size that do not aggressively
        /// increase their window size. We need to take into account that there may be SSH servers
        /// that only increase their window size when it has reached zero.
        /// </para>
        /// </remarks>
        public void SendData(byte[] data, int offset, int size)
        {
            // send channel messages only while channel is open
            if (!IsOpen)
            {
                return;
            }

            lock (_sendDataLock)
            {
                var totalBytesToSend = size;
                int sizeOfCurrentMessage;
                while ((sizeOfCurrentMessage = GetDataLengthThatCanBeSentInMessage(totalBytesToSend)) > 0)
                {
                    var channelDataMessage = new ChannelDataMessage(RemoteChannelNumber,
                                                                    data,
                                                                    offset,
                                                                    sizeOfCurrentMessage);
                    _session.SendMessage(channelDataMessage);

                    totalBytesToSend -= sizeOfCurrentMessage;
                    offset += sizeOfCurrentMessage;
                }
            }
        }

        /// <summary>
        /// Sends as much of the payload as the remote window currently allows, without waiting.
        /// </summary>
        /// <param name="data">An array of <see cref="byte"/> containing the payload to send.</param>
        /// <param name="offset">The zero-based offset in <paramref name="data"/> at which to begin taking data from.</param>
        /// <param name="count">The number of bytes of <paramref name="data"/> to send.</param>
        /// <param name="written">Receives the number of bytes actually sent.</param>
        /// <returns>
        /// <see cref="ChannelSendResult.Written"/> when all of it was sent,
        /// <see cref="ChannelSendResult.WindowFull"/> when the remote window ran out first, or
        /// <see cref="ChannelSendResult.Closed"/> when the channel is not open.
        /// </returns>
        /// <remarks>
        /// <para>
        /// The difference from <see cref="SendData(byte[], int, int)"/> is that this never parks. A
        /// blocking send waits for a window adjust that only the message listener thread can deliver,
        /// so calling it from that thread stalls the whole session until it times out.
        /// </para>
        /// <para>
        /// The partial count matters as much as the status: with 500 bytes of window and 1360 to
        /// send, a caller told only "full" must either stall the flow or resend from the start.
        /// </para>
        /// </remarks>
        public ChannelSendResult TrySend(byte[] data, int offset, int count, out int written)
        {
            ArgumentNullException.ThrowIfNull(data);

            written = 0;

            if (!IsOpen)
            {
                return ChannelSendResult.Closed;
            }

            lock (_sendDataLock)
            {
                while (written < count)
                {
                    // Re-checked inside the loop: the channel can close between chunks, and a closed
                    // channel silently accepting sends is what turns a disconnect into a live spin.
                    if (!IsOpen)
                    {
                        return written == 0 ? ChannelSendResult.Closed : ChannelSendResult.WindowFull;
                    }

                    uint chunk;

                    lock (_serverWindowSizeLock)
                    {
                        chunk = Math.Min(RemotePacketSize, (uint)(count - written));
                        chunk = Math.Min(chunk, RemoteWindowSize);

                        if (chunk == 0)
                        {
                            break;
                        }

                        RemoteWindowSize -= chunk;
                    }

                    _session.SendMessage(new ChannelDataMessage(RemoteChannelNumber, data, offset + written, (int)chunk));
                    written += (int)chunk;
                }
            }

            return written == count ? ChannelSendResult.Written : ChannelSendResult.WindowFull;
        }

        /// <summary>
        /// Called when channel window need to be adjust.
        /// </summary>
        /// <param name="bytesToAdd">The bytes to add.</param>
        protected virtual void OnWindowAdjust(uint bytesToAdd)
        {
            lock (_serverWindowSizeLock)
            {
                RemoteWindowSize += bytesToAdd;
            }

            _ = _channelServerWindowAdjustWaitHandle?.Set();

            WindowAvailable?.Invoke(this, EventArgs.Empty);
        }

        /// <summary>
        /// Called when channel data is received.
        /// </summary>
        /// <param name="data">The data.</param>
        protected virtual void OnData(ArraySegment<byte> data)
        {
            AdjustDataWindow(data.Count);

            DataReceived?.Invoke(this, new ChannelDataEventArgs(LocalChannelNumber, data));
        }

        /// <summary>
        /// Called when channel extended data is received.
        /// </summary>
        /// <param name="data">The data.</param>
        /// <param name="dataTypeCode">The data type code.</param>
        protected virtual void OnExtendedData(byte[] data, uint dataTypeCode)
        {
            AdjustDataWindow(data.Length);

            ExtendedDataReceived?.Invoke(this, new ChannelExtendedDataEventArgs(LocalChannelNumber, data, dataTypeCode));
        }

        /// <summary>
        /// Called when channel has no more data to receive.
        /// </summary>
        protected virtual void OnEof()
        {
            _eofMessageReceived = true;

            EndOfData?.Invoke(this, new ChannelEventArgs(LocalChannelNumber));
        }

        /// <summary>
        /// Called when channel is closed by the server.
        /// </summary>
        protected virtual void OnClose()
        {
            _closeMessageReceived = true;

            // Signal that SSH_MSG_CHANNEL_CLOSE message was received from server.
            // We need to signal this before invoking Close() as it may very well
            // be blocked waiting for this signal.
            var channelClosedWaitHandle = _channelClosedWaitHandle;
            if (channelClosedWaitHandle != null)
            {
                _ = channelClosedWaitHandle.Set();
            }

            _ = _channelClosedCompletion.TrySetResult(true);

            // close the channel
            Close();
        }

        /// <summary>
        /// Called when channel request received.
        /// </summary>
        /// <param name="info">Channel request information.</param>
        protected virtual void OnRequest(RequestInfo info)
        {
            RequestReceived?.Invoke(this, new ChannelRequestEventArgs(info));
        }

        /// <summary>
        /// Called when channel request was successful.
        /// </summary>
        protected virtual void OnSuccess()
        {
            RequestSucceeded?.Invoke(this, new ChannelEventArgs(LocalChannelNumber));
        }

        /// <summary>
        /// Called when channel request failed.
        /// </summary>
        protected virtual void OnFailure()
        {
            RequestFailed?.Invoke(this, new ChannelEventArgs(LocalChannelNumber));
        }

        /// <summary>
        /// Raises <see cref="Exception"/> event.
        /// </summary>
        /// <param name="exception">The exception.</param>
        private void RaiseExceptionEvent(Exception exception)
        {
            Exception?.Invoke(this, new ExceptionEventArgs(exception));
        }

        /// <summary>
        /// Sends a message to the server.
        /// </summary>
        /// <param name="message">The message to send.</param>
        /// <returns>
        /// <see langword="true"/> if the message was sent to the server; otherwise, <see langword="false"/>.
        /// </returns>
        /// <exception cref="InvalidOperationException">The size of the packet exceeds the maximum size defined by the protocol.</exception>
        /// <remarks>
        /// This methods returns <see langword="false"/> when the attempt to send the message results in a
        /// <see cref="SocketException"/> or a <see cref="SshException"/>.
        /// </remarks>
        private bool TrySendMessage(Message message)
        {
            return _session.TrySendMessage(message);
        }

        /// <summary>
        /// Sends SSH message to the server.
        /// </summary>
        /// <param name="message">The message.</param>
        protected void SendMessage(Message message)
        {
            // Send channel messages only while channel is open
            if (!IsOpen)
            {
                return;
            }

            _session.SendMessage(message);
        }

        /// <summary>
        /// Sends a SSH_MSG_CHANNEL_EOF message to the remote server.
        /// </summary>
        /// <exception cref="InvalidOperationException">The channel is closed.</exception>
        public void SendEof()
        {
            if (!IsOpen)
            {
                throw CreateChannelClosedException();
            }

            lock (_messagingLock)
            {
                _session.SendMessage(new ChannelEofMessage(RemoteChannelNumber));
                _eofMessageSent = true;
            }
        }

        /// <summary>
        /// Waits for the handle to be signaled or for an error to occurs.
        /// </summary>
        /// <param name="waitHandle">The wait handle.</param>
        protected void WaitOnHandle(WaitHandle waitHandle)
        {
            _session.WaitOnHandle(waitHandle);
        }

        /// <summary>
        /// Closes the channel, waiting for the SSH_MSG_CHANNEL_CLOSE message to be received from the server.
        /// </summary>
        protected virtual void Close()
        {
            if (TrySendCloseSequence())
            {
                // Only wait for the channel to be closed by the server if we didn't send a
                // SSH_MSG_CHANNEL_CLOSE as response to a SSH_MSG_CHANNEL_CLOSE sent by the server.
                // (When we did, the handle is already set and the wait returns immediately.)
                var channelClosedWaitHandle = _channelClosedWaitHandle;
                if (channelClosedWaitHandle is not null)
                {
                    var closeWaitResult = _session.TryWait(channelClosedWaitHandle, ConnectionInfo.ChannelCloseTimeout);
                    if (closeWaitResult != WaitResult.Success)
                    {
                        _logger.LogInformation("Wait for channel close not successful: {CloseWaitResult}", closeWaitResult);
                    }
                }
            }

            CompleteClose();
        }

        /// <summary>
        /// Closes the channel without ever parking a thread: the wait for the server's
        /// SSH_MSG_CHANNEL_CLOSE costs an object, not a blocked thread.
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>A task that represents the close.</returns>
        /// <remarks>
        /// The bookkeeping in <see cref="CompleteClose"/> runs on every exit path, including timeout
        /// and cancellation. Without that, a later <see cref="Dispose()"/> would find the channel
        /// still marked open and re-enter the blocking <see cref="Close"/> path, and nothing would
        /// have been gained.
        /// </remarks>
        public virtual async Task CloseAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                if (TrySendCloseSequence())
                {
                    try
                    {
                        _ = await _channelClosedCompletion.Task
                            .WaitAsync(ConnectionInfo.ChannelCloseTimeout, cancellationToken)
                            .ConfigureAwait(false);
                    }
                    catch (TimeoutException ex)
                    {
                        _logger.LogInformation(ex, "Wait for channel close not successful: TimedOut");
                    }
                }
            }
            finally
            {
                CompleteClose();
            }
        }

        /// <summary>
        /// Sends the EOF-then-CLOSE sequence, if this side still owes it.
        /// </summary>
        /// <returns>
        /// <see langword="true"/> if a SSH_MSG_CHANNEL_CLOSE was sent just now, so the caller should
        /// wait for the server's; otherwise, <see langword="false"/>.
        /// </returns>
        private bool TrySendCloseSequence()
        {
            /*
             * Synchronize sending SSH_MSG_CHANNEL_EOF and SSH_MSG_CHANNEL_CLOSE to ensure that these messages
             * are sent in that order; when both the client and the server attempt to close the channel at the
             * same time we would otherwise risk sending the SSH_MSG_CHANNEL_EOF after the SSH_MSG_CHANNEL_CLOSE
             * message causing the server to disconnect the session.
             */

            lock (_messagingLock)
            {
                // Send EOF message first the following conditions are met:
                // * we have not sent a SSH_MSG_CHANNEL_EOF message
                // * remote party has not already sent a SSH_MSG_CHANNEL_EOF message
                // * remote party has not already sent a SSH_MSG_CHANNEL_CLOSE message
                // * the channel is open
                // * the session is connected
                if (!_eofMessageSent && !_closeMessageReceived && !_eofMessageReceived && IsOpen && IsConnected)
                {
                    if (TrySendMessage(new ChannelEofMessage(RemoteChannelNumber)))
                    {
                        _eofMessageSent = true;
                    }
                }

                // send message to close the channel on the server when it has not already been sent
                // and the channel is open and the session is connected
                if (!_closeMessageSent && IsOpen && IsConnected)
                {
                    if (TrySendMessage(new ChannelCloseMessage(RemoteChannelNumber)))
                    {
                        _closeMessageSent = true;
                        return true;
                    }
                }

                return false;
            }
        }

        /// <summary>
        /// Marks the channel closed and raises <see cref="Closed"/> when both sides have closed.
        /// </summary>
        private void CompleteClose()
        {
            lock (_messagingLock)
            {
                if (IsOpen)
                {
                    // mark sure the channel is marked closed before we raise the Closed event
                    // this also ensures don't raise the Closed event more than once
                    IsOpen = false;

                    if (_closeMessageReceived)
                    {
                        // raise event signaling that both ends of the channel have been closed
                        Closed?.Invoke(this, new ChannelEventArgs(LocalChannelNumber));
                    }
                }
            }
        }

        protected virtual void OnDisconnected()
        {
        }

        protected virtual void OnErrorOccurred(Exception exp)
        {
        }

        private void Session_Disconnected(object sender, EventArgs e)
        {
            IsOpen = false;

            // The server's close is never coming; a CloseAsync in flight must not wait for it. The
            // synchronous path survives this only because Session.TryWait also watches the session's
            // own demise - this is the asynchronous equivalent.
            _ = _channelClosedCompletion.TrySetResult(false);

            try
            {
                OnDisconnected();
            }
            catch (Exception ex)
            {
                OnChannelException(ex);
            }
        }

        /// <summary>
        /// Called when an <see cref="Exception"/> occurs while processing a channel message.
        /// </summary>
        /// <param name="ex">The <see cref="Exception"/>.</param>
        /// <remarks>
        /// This method will in turn invoke <see cref="OnErrorOccurred(System.Exception)"/>, and
        /// raise the <see cref="Exception"/> event.
        /// </remarks>
        protected void OnChannelException(Exception ex)
        {
            OnErrorOccurred(ex);
            RaiseExceptionEvent(ex);
        }

        private void Session_ErrorOccurred(object sender, ExceptionEventArgs e)
        {
            _ = _channelClosedCompletion.TrySetResult(false);

            try
            {
                OnErrorOccurred(e.Exception);
            }
            catch (Exception ex)
            {
                RaiseExceptionEvent(ex);
            }
        }

        private void OnChannelWindowAdjust(object sender, MessageEventArgs<ChannelWindowAdjustMessage> e)
        {
            if (e.Message.LocalChannelNumber == LocalChannelNumber)
            {
                try
                {
                    OnWindowAdjust(e.Message.BytesToAdd);
                }
                catch (Exception ex)
                {
                    OnChannelException(ex);
                }
            }
        }

        private void OnChannelData(object sender, MessageEventArgs<ChannelDataMessage> e)
        {
            if (e.Message.LocalChannelNumber == LocalChannelNumber)
            {
                try
                {
                    OnData(new ArraySegment<byte>(e.Message.Data, e.Message.Offset, e.Message.Size));
                }
                catch (Exception ex)
                {
                    OnChannelException(ex);
                }
            }
        }

        private void OnChannelExtendedData(object sender, MessageEventArgs<ChannelExtendedDataMessage> e)
        {
            if (e.Message.LocalChannelNumber == LocalChannelNumber)
            {
                try
                {
                    OnExtendedData(e.Message.Data, e.Message.DataTypeCode);
                }
                catch (Exception ex)
                {
                    OnChannelException(ex);
                }
            }
        }

        private void OnChannelEof(object sender, MessageEventArgs<ChannelEofMessage> e)
        {
            if (e.Message.LocalChannelNumber == LocalChannelNumber)
            {
                try
                {
                    OnEof();
                }
                catch (Exception ex)
                {
                    OnChannelException(ex);
                }
            }
        }

        private void OnChannelClose(object sender, MessageEventArgs<ChannelCloseMessage> e)
        {
            if (e.Message.LocalChannelNumber == LocalChannelNumber)
            {
                try
                {
                    OnClose();
                }
                catch (Exception ex)
                {
                    OnChannelException(ex);
                }
            }
        }

        private void OnChannelRequest(object sender, MessageEventArgs<ChannelRequestMessage> e)
        {
            if (e.Message.LocalChannelNumber == LocalChannelNumber)
            {
                try
                {
                    if (_session.ConnectionInfo.ChannelRequests.TryGetValue(e.Message.RequestName, out var requestInfo))
                    {
                        // Load request specific data
                        requestInfo.Load(e.Message.RequestData);

                        // Raise request specific event
                        OnRequest(requestInfo);
                    }
                    else
                    {
                        var unknownRequestInfo = new UnknownRequestInfo(e.Message.RequestName);
                        unknownRequestInfo.Load(e.Message.RequestData);

                        if (unknownRequestInfo.WantReply)
                        {
                            var reply = new ChannelFailureMessage(RemoteChannelNumber);
                            SendMessage(reply);
                        }
                    }
                }
                catch (Exception ex)
                {
                    OnChannelException(ex);
                }
            }
        }

        private void OnChannelSuccess(object sender, MessageEventArgs<ChannelSuccessMessage> e)
        {
            if (e.Message.LocalChannelNumber == LocalChannelNumber)
            {
                try
                {
                    OnSuccess();
                }
                catch (Exception ex)
                {
                    OnChannelException(ex);
                }
            }
        }

        private void OnChannelFailure(object sender, MessageEventArgs<ChannelFailureMessage> e)
        {
            if (e.Message.LocalChannelNumber == LocalChannelNumber)
            {
                try
                {
                    OnFailure();
                }
                catch (Exception ex)
                {
                    OnChannelException(ex);
                }
            }
        }

        /// <summary>
        /// Accounts for data received from the remote party, and credits the window again unless
        /// crediting has been deferred to the consumer.
        /// </summary>
        /// <param name="count">The number of bytes received.</param>
        private void AdjustDataWindow(int count)
        {
            uint credit;

            lock (_localWindowLock)
            {
                var received = (uint)count;

                // The remote party sending more than the window allows is a protocol violation. Left
                // undetected the subtraction wraps, turning "window exceeded" into "window enormous"
                // and disabling the flow control entirely - invisible at a 2 GiB window, immediate at
                // a small one.
                if (received > LocalWindowSize)
                {
                    throw new SshException(string.Format(
                        System.Globalization.CultureInfo.InvariantCulture,
                        "The remote party sent {0} bytes on channel {1} with only {2} bytes of window remaining.",
                        received,
                        LocalChannelNumber,
                        LocalWindowSize));
                }

                LocalWindowSize -= received;

                if (DeferWindowCredit)
                {
                    // The consumer credits this back through ReleaseReceivedData once it has taken
                    // the bytes; until then the window is genuinely consumed.
                    return;
                }

                if (LocalWindowSize >= LocalPacketSize)
                {
                    return;
                }

                credit = _initialWindowSize - LocalWindowSize;
                LocalWindowSize = _initialWindowSize;
            }

            // Outside the lock: sending can block for the duration of a key exchange.
            SendMessage(new ChannelWindowAdjustMessage(RemoteChannelNumber, credit));
        }

        /// <summary>
        /// Records that the consumer has taken bytes off the channel, so that the window may be
        /// credited back to the remote party.
        /// </summary>
        /// <param name="count">The number of bytes consumed.</param>
        /// <returns>
        /// <see langword="true"/> if enough has accumulated that <see cref="FlushWindowCredit"/>
        /// should be called; otherwise, <see langword="false"/>.
        /// </returns>
        /// <remarks>
        /// Deliberately does not send anything. Crediting emits a message, and sending blocks while a
        /// key exchange is in progress; a consumer draining a buffer must not inherit that. The
        /// caller records here and flushes from whichever thread it has designated for sending.
        /// </remarks>
        public bool ReleaseReceivedData(int count)
        {
            if (!DeferWindowCredit || count <= 0)
            {
                return false;
            }

            lock (_localWindowLock)
            {
                _uncreditedBytes += (uint)count;

                // Whichever comes first, half the window or three packets' worth. OpenSSH's
                // channel_check_window uses the same pair, and the second condition is what matters
                // once the window is large: crediting only at half of a 2 MiB window would leave the
                // remote party stalled for a round trip every megabyte, while three packets keeps
                // the window near full without sending a message per read.
                var threshold = Math.Min(_initialWindowSize / 2, LocalPacketSize * 3);
                return _uncreditedBytes >= threshold;
            }
        }

        /// <summary>
        /// Sends the window credit accumulated by <see cref="ReleaseReceivedData"/>.
        /// </summary>
        /// <remarks>
        /// Blocks for the duration of a key exchange, like any other send. Call it from the thread
        /// that owns sending on the session.
        /// </remarks>
        public void FlushWindowCredit()
        {
            uint credit;

            lock (_localWindowLock)
            {
                if (_uncreditedBytes == 0)
                {
                    return;
                }

                credit = _uncreditedBytes;
                _uncreditedBytes = 0;
                LocalWindowSize += credit;
            }

            if (IsOpen)
            {
                SendMessage(new ChannelWindowAdjustMessage(RemoteChannelNumber, credit));

                DirectTcpipStream.CountWindowCredit(credit);
            }
        }

        /// <summary>
        /// Determines the length of data that currently can be sent in a single message.
        /// </summary>
        /// <param name="messageLength">The length of the message that must be sent.</param>
        /// <returns>
        /// The actual data length that currently can be sent.
        /// </returns>
        private int GetDataLengthThatCanBeSentInMessage(int messageLength)
        {
            var dataLength = Math.Min(RemotePacketSize, (uint)messageLength);

            do
            {
                // Captured once per iteration: Dispose nulls this field and disposes the handle, so
                // reading it twice can hand WaitOnHandle a handle that was disposed in between.
                var windowAdjusted = _channelServerWindowAdjustWaitHandle;
                if (windowAdjusted is null)
                {
                    return 0;
                }

                lock (_serverWindowSizeLock)
                {
                    var serverWindowSize = RemoteWindowSize;
                    if (serverWindowSize == 0U && dataLength > 0)
                    {
                        // Allow us to be signalled when remote window size is adjusted
                        _ = windowAdjusted.Reset();
                    }
                    else
                    {
                        dataLength = Math.Min(dataLength, serverWindowSize);
                        RemoteWindowSize -= dataLength;
                        return (int)dataLength;
                    }
                }

                // Wait for remote window size to change
                WaitOnHandle(windowAdjusted);
            }
            while (true);
        }

        private static InvalidOperationException CreateRemoteChannelInfoNotAvailableException()
        {
            throw new InvalidOperationException("The channel has not been opened, or the open has not yet been confirmed.");
        }

        private static InvalidOperationException CreateChannelClosedException()
        {
            throw new InvalidOperationException("The channel is closed.");
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to release both managed and unmanaged resources; <see langword="false"/> to release only unmanaged resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing && Interlocked.Exchange(ref _isDisposed, 1) == 0)
            {
                Close();

                var session = _session;
                if (session is not null)
                {
                    session.ChannelWindowAdjustReceived -= OnChannelWindowAdjust;
                    session.ChannelDataReceived -= OnChannelData;
                    session.ChannelExtendedDataReceived -= OnChannelExtendedData;
                    session.ChannelEofReceived -= OnChannelEof;
                    session.ChannelCloseReceived -= OnChannelClose;
                    session.ChannelRequestReceived -= OnChannelRequest;
                    session.ChannelSuccessReceived -= OnChannelSuccess;
                    session.ChannelFailureReceived -= OnChannelFailure;
                    session.ErrorOccured -= Session_ErrorOccurred;
                    session.Disconnected -= Session_Disconnected;
                }

                var channelClosedWaitHandle = _channelClosedWaitHandle;
                if (channelClosedWaitHandle is not null)
                {
                    _channelClosedWaitHandle = null;
                    channelClosedWaitHandle.Dispose();
                }

                var channelServerWindowAdjustWaitHandle = _channelServerWindowAdjustWaitHandle;
                if (channelServerWindowAdjustWaitHandle is not null)
                {
                    _channelServerWindowAdjustWaitHandle = null;
                    channelServerWindowAdjustWaitHandle.Dispose();
                }
            }
        }
    }
}
