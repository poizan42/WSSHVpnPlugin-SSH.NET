using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using Moq;

using Renci.SshNet.Channels;
using Renci.SshNet.Common;
using Renci.SshNet.Messages;
using Renci.SshNet.Messages.Connection;
using Renci.SshNet.Tests.Common;

namespace Renci.SshNet.Tests.Classes.Channels
{
    /// <summary>
    /// Covers walking away from an open that has not settled, and closing without parking a thread.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The assertions are about the <em>server-visible</em> outcome, not local disposal: a naive
    /// implementation that disposes a cancelled open passes any local-only test while leaking the
    /// channel - and the server's TCP connection to the destination - for the life of the session,
    /// because disposing unsubscribes from a confirmation that is still on its way.
    /// </para>
    /// </remarks>
    [TestClass]
    public class ChannelDirectTcpipTest_AbandonAndCloseAsync : TestBase
    {
        private Mock<ISession> _sessionMock;
        private Mock<IConnectionInfo> _connectionInfoMock;
        private ConcurrentQueue<Message> _sentMessages;
        private uint _localChannelNumber;
        private uint _localWindowSize;
        private uint _localPacketSize;
        private string _remoteHost;
        private uint _port;
        private uint _remoteWindowSize;
        private uint _remotePacketSize;
        private uint _remoteChannelNumber;

        protected override void OnInit()
        {
            base.OnInit();

            var random = new Random();

            _localWindowSize = (uint)random.Next(2000, 3000);
            _localPacketSize = (uint)random.Next(1000, 2000);
            _remoteHost = random.Next().ToString(CultureInfo.InvariantCulture);
            _port = (uint)random.Next(IPEndPoint.MinPort, IPEndPoint.MaxPort);
            _localChannelNumber = (uint)random.Next(0, int.MaxValue);
            _remoteWindowSize = (uint)random.Next(0, int.MaxValue);
            _remotePacketSize = (uint)random.Next(100, 200);
            _remoteChannelNumber = (uint)random.Next(0, int.MaxValue);
            _sentMessages = new ConcurrentQueue<Message>();

            _connectionInfoMock = new Mock<IConnectionInfo>(MockBehavior.Loose);
            _ = _connectionInfoMock.Setup(p => p.ChannelCloseTimeout).Returns(TimeSpan.Zero);

            _sessionMock = new Mock<ISession>(MockBehavior.Strict);
            _ = _sessionMock.Setup(p => p.SessionLoggerFactory).Returns(NullLoggerFactory.Instance);
            _ = _sessionMock.Setup(p => p.IsConnected).Returns(true);
            _ = _sessionMock.Setup(p => p.ConnectionInfo).Returns(_connectionInfoMock.Object);
            _ = _sessionMock.Setup(p => p.WaitOnHandle(It.IsAny<EventWaitHandle>()))
                            .Callback<WaitHandle>(p => p.WaitOne());
            _ = _sessionMock.Setup(p => p.TryWait(It.IsAny<EventWaitHandle>(), It.IsAny<TimeSpan>()))
                            .Returns(WaitResult.TimedOut);
            _ = _sessionMock.Setup(p => p.TrySendMessage(It.IsAny<Message>()))
                            .Returns<Message>(m =>
                            {
                                _sentMessages.Enqueue(m);
                                return true;
                            });
        }

        private ChannelDirectTcpip CreateChannel()
        {
            return new ChannelDirectTcpip(_sessionMock.Object, _localChannelNumber, _localWindowSize, _localPacketSize);
        }

        private void RespondToOpenWith(Action<ChannelOpenMessage> respond)
        {
            _ = _sessionMock.Setup(p => p.SendMessage(It.IsAny<ChannelOpenMessage>()))
                            .Callback<Message>(m => respond((ChannelOpenMessage)m));
        }

        private void ConfirmOpen(ChannelOpenMessage open)
        {
            _sessionMock.Raise(p => p.ChannelOpenConfirmationReceived += null,
                               new MessageEventArgs<ChannelOpenConfirmationMessage>(
                                   new ChannelOpenConfirmationMessage(open.LocalChannelNumber,
                                                                      _remoteWindowSize,
                                                                      _remotePacketSize,
                                                                      _remoteChannelNumber)));
        }

        private void RefuseOpen(ChannelOpenMessage open)
        {
            _sessionMock.Raise(p => p.ChannelOpenFailureReceived += null,
                               new MessageEventArgs<ChannelOpenFailureMessage>(
                                   new ChannelOpenFailureMessage(open.LocalChannelNumber,
                                                                 "administratively prohibited",
                                                                 ChannelOpenFailureMessage.AdministrativelyProhibited)));
        }

        /// <summary>
        /// Starts an open, cancels the caller's wait before the server has answered, and returns
        /// both the abandon task and the captured open message so the test can deliver the answer.
        /// </summary>
        private async Task<(ChannelDirectTcpip Channel, Task Abandon, ChannelOpenMessage Open)> CancelAnOpenInFlight()
        {
            ChannelOpenMessage open = null;
            RespondToOpenWith(m => open = m);

            var channel = CreateChannel();

            using (var cancellation = new CancellationTokenSource())
            {
                var openTask = channel.OpenAsync(_remoteHost, _port, "127.0.0.1", 0, cancellation.Token);
                await cancellation.CancelAsync();

                _ = await Assert.ThrowsAsync<TaskCanceledException>(() => openTask);
            }

            Assert.IsNotNull(open, "The open never went on the wire.");

            var abandon = channel.AbandonAsync();
            Assert.IsFalse(abandon.IsCompleted, "The abandon must hold until the server answers.");

            return (channel, abandon, open);
        }

        [TestMethod]
        public async Task AbandonAsync_ConfirmedAfterTheCallerGaveUp_ClosesTheChannelOnTheServer()
        {
            var (channel, abandon, open) = await CancelAnOpenInFlight();

            ConfirmOpen(open);

            await abandon.WaitAsync(TimeSpan.FromSeconds(10));

            Assert.IsFalse(channel.IsOpen);
            Assert.IsTrue(_sentMessages.OfType<ChannelCloseMessage>().Any(),
                          "A confirmed-then-abandoned open must send SSH_MSG_CHANNEL_CLOSE, or the server keeps the channel for the life of the session.");
        }

        [TestMethod]
        public async Task AbandonAsync_RefusedAfterTheCallerGaveUp_DisposesWithoutTouchingTheWire()
        {
            var (channel, abandon, open) = await CancelAnOpenInFlight();

            RefuseOpen(open);

            await abandon.WaitAsync(TimeSpan.FromSeconds(10));

            Assert.IsFalse(channel.IsOpen);
            Assert.IsEmpty(_sentMessages,
                           "A refused open has nothing on the server to close; sending EOF or CLOSE for a channel that never existed is a protocol violation.");
        }

        [TestMethod]
        public async Task AbandonAsync_SessionDisconnectsInstead_Completes()
        {
            var (channel, abandon, _) = await CancelAnOpenInFlight();

            _sessionMock.Raise(p => p.Disconnected += null, EventArgs.Empty);

            await abandon.WaitAsync(TimeSpan.FromSeconds(10));

            Assert.IsFalse(channel.IsOpen);
            Assert.IsEmpty(_sentMessages, "Nothing to close once the session itself is gone.");
        }

        [TestMethod]
        public async Task CloseAsync_ServerAnswers_CompletesAndRaisesClosed()
        {
            // A timeout long enough that reaching the assertions promptly proves the answer was
            // seen, rather than the wait having simply expired.
            _ = _connectionInfoMock.Setup(p => p.ChannelCloseTimeout).Returns(TimeSpan.FromSeconds(30));

            RespondToOpenWith(ConfirmOpen);

            var channel = CreateChannel();
            await channel.OpenAsync(_remoteHost, _port, "127.0.0.1", 0, CancellationToken.None);

            var closedRaised = false;
            channel.Closed += (_, _) => closedRaised = true;

            var close = channel.CloseAsync();

            Assert.IsTrue(_sentMessages.OfType<ChannelCloseMessage>().Any(),
                          "The close sequence goes on the wire before anything is awaited.");

            _sessionMock.Raise(p => p.ChannelCloseReceived += null,
                               new MessageEventArgs<ChannelCloseMessage>(new ChannelCloseMessage(_localChannelNumber)));

            await close.WaitAsync(TimeSpan.FromSeconds(10));

            Assert.IsFalse(channel.IsOpen);
            Assert.IsTrue(closedRaised, "Both sides have closed, so Closed must be raised.");

            channel.Dispose();
        }

        [TestMethod]
        public async Task CloseAsync_SessionDiesWhileWaiting_Completes()
        {
            _ = _connectionInfoMock.Setup(p => p.ChannelCloseTimeout).Returns(TimeSpan.FromSeconds(30));

            RespondToOpenWith(ConfirmOpen);

            var channel = CreateChannel();
            await channel.OpenAsync(_remoteHost, _port, "127.0.0.1", 0, CancellationToken.None);

            var close = channel.CloseAsync();

            _sessionMock.Raise(p => p.Disconnected += null, EventArgs.Empty);

            await close.WaitAsync(TimeSpan.FromSeconds(10));

            Assert.IsFalse(channel.IsOpen);

            channel.Dispose();
        }

        /// <summary>
        /// Data can arrive immediately behind the open confirmation, in the same batch of messages.
        /// The stream subscribes in its constructor, so creating it before the open is what keeps
        /// those bytes: created after - the original shape - they were raised to nobody and lost.
        /// </summary>
        [TestMethod]
        public void CreateDirectTcpipStream_DataArrivingWithTheConfirmation_IsNotLost()
        {
            var payload = new byte[] { 1, 2, 3, 4 };

            RespondToOpenWith(open =>
            {
                ConfirmOpen(open);
                _sessionMock.Raise(p => p.ChannelDataReceived += null,
                                   new MessageEventArgs<ChannelDataMessage>(
                                       new ChannelDataMessage(open.LocalChannelNumber, payload)));
            });

            _ = _sessionMock.Setup(p => p.CreateChannelDirectTcpip(It.IsAny<uint>()))
                            .Returns<uint>(windowSize => new ChannelDirectTcpip(_sessionMock.Object, _localChannelNumber, windowSize, _localPacketSize));

            var stream = new ServiceFactory().CreateDirectTcpipStream(_sessionMock.Object, _remoteHost, _port, bufferSize: 4096, windowSize: 2048);

            try
            {
                Assert.IsTrue(stream.TryRead(out var data), "The bytes that arrived with the confirmation are gone.");
                CollectionAssert.AreEqual(payload, data.ToArray());
            }
            finally
            {
                stream.Dispose();
            }
        }

        [TestMethod]
        public async Task UnopenedStream_DataArrivingWithTheConfirmation_IsNotLost()
        {
            var payload = new byte[] { 5, 6, 7, 8 };

            RespondToOpenWith(open =>
            {
                ConfirmOpen(open);
                _sessionMock.Raise(p => p.ChannelDataReceived += null,
                                   new MessageEventArgs<ChannelDataMessage>(
                                       new ChannelDataMessage(open.LocalChannelNumber, payload)));
            });

            _ = _sessionMock.Setup(p => p.CreateChannelDirectTcpip(It.IsAny<uint>()))
                            .Returns<uint>(windowSize => new ChannelDirectTcpip(_sessionMock.Object, _localChannelNumber, windowSize, _localPacketSize));

            var stream = new ServiceFactory().CreateUnopenedDirectTcpipStream(_sessionMock.Object, bufferSize: 4096, windowSize: 2048);

            try
            {
                await stream.OpenAsync(_remoteHost, _port, CancellationToken.None);

                Assert.IsTrue(stream.TryRead(out var data), "The bytes that arrived with the confirmation are gone.");
                CollectionAssert.AreEqual(payload, data.ToArray());
            }
            finally
            {
                await stream.DisposeAsync();
            }
        }
    }
}
