using System;
using System.Globalization;
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
    /// Covers opening a <c>direct-tcpip</c> channel that is not bound to a <see cref="System.Net.Sockets.Socket"/>.
    /// </summary>
    /// <remarks>
    /// The socket-bound overload exists to forward an accepted connection. A caller that owns the
    /// bytes itself - a user-space TCP/IP stack, say - has no socket to hand over, and needs the
    /// channel as a byte stream instead.
    /// </remarks>
    [TestClass]
    public class ChannelDirectTcpipTest_OpenWithoutSocket : TestBase
    {
        private Mock<ISession> _sessionMock;
        private uint _localChannelNumber;
        private uint _localWindowSize;
        private uint _localPacketSize;
        private string _remoteHost;
        private uint _port;
        private string _originatorAddress;
        private uint _originatorPort;
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
            _originatorAddress = "192.0.2.1";
            _originatorPort = (uint)random.Next(IPEndPoint.MinPort, IPEndPoint.MaxPort);
            _localChannelNumber = (uint)random.Next(0, int.MaxValue);
            _remoteWindowSize = (uint)random.Next(0, int.MaxValue);
            _remotePacketSize = (uint)random.Next(100, 200);
            _remoteChannelNumber = (uint)random.Next(0, int.MaxValue);

            _sessionMock = new Mock<ISession>(MockBehavior.Strict);
            _ = _sessionMock.Setup(p => p.SessionLoggerFactory).Returns(NullLoggerFactory.Instance);
            _ = _sessionMock.Setup(p => p.IsConnected).Returns(true);
            _ = _sessionMock.Setup(p => p.WaitOnHandle(It.IsAny<EventWaitHandle>()))
                            .Callback<WaitHandle>(p => p.WaitOne());

            // Disposing an open channel sends EOF and close. Reporting the send as failed keeps
            // teardown from waiting on a close message no server is going to send here; these tests
            // are about the open, and the close path has its own coverage.
            _ = _sessionMock.Setup(p => p.TrySendMessage(It.IsAny<Message>())).Returns(false);
        }

        private ChannelDirectTcpip CreateChannel()
        {
            return new ChannelDirectTcpip(_sessionMock.Object, _localChannelNumber, _localWindowSize, _localPacketSize);
        }

        /// <summary>
        /// Makes the session answer a channel open with the given reply.
        /// </summary>
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

        [TestMethod]
        public void Open_ChannelIsOpenAndCarriesTheOriginatorWeSupplied()
        {
            ChannelOpenMessage sent = null;

            RespondToOpenWith(open =>
            {
                sent = open;
                ConfirmOpen(open);
            });

            using (var channel = CreateChannel())
            {
                channel.Open(_remoteHost, _port, _originatorAddress, _originatorPort);

                Assert.IsTrue(channel.IsOpen);
            }

            Assert.IsNotNull(sent);
            var info = (DirectTcpipChannelInfo)sent.Info;
            Assert.AreEqual(_remoteHost, info.HostToConnect);
            Assert.AreEqual(_port, info.PortToConnect);
            Assert.AreEqual(_originatorAddress, info.OriginatorAddress);
            Assert.AreEqual(_originatorPort, info.OriginatorPort);
        }

        [TestMethod]
        public void Open_ServerRefuses_Throws()
        {
            RespondToOpenWith(RefuseOpen);

            using (var channel = CreateChannel())
            {
                var ex = Assert.Throws<SshException>(
                    () => channel.Open(_remoteHost, _port, _originatorAddress, _originatorPort));

                Assert.IsTrue(ex.Message.Contains("administratively prohibited", StringComparison.Ordinal), ex.Message);
                Assert.IsFalse(channel.IsOpen);
            }
        }

        [TestMethod]
        public async Task OpenAsync_ChannelIsOpen()
        {
            RespondToOpenWith(ConfirmOpen);

            using (var channel = CreateChannel())
            {
                await channel.OpenAsync(_remoteHost, _port, _originatorAddress, _originatorPort, CancellationToken.None);

                Assert.IsTrue(channel.IsOpen);
            }
        }

        [TestMethod]
        public async Task OpenAsync_ServerRefuses_Throws()
        {
            RespondToOpenWith(RefuseOpen);

            using (var channel = CreateChannel())
            {
                _ = await Assert.ThrowsAsync<SshException>(
                    () => channel.OpenAsync(_remoteHost, _port, _originatorAddress, _originatorPort, CancellationToken.None));

                Assert.IsFalse(channel.IsOpen);
            }
        }

        /// <summary>
        /// A disconnect while the open is outstanding has to end the wait.
        /// </summary>
        /// <remarks>
        /// Open confirmation and open failure are not the only terminal outcomes. Without this the
        /// task never completes, and a caller opening a channel per flow accumulates them silently
        /// whenever the connection drops.
        /// </remarks>
        [TestMethod]
        public async Task OpenAsync_SessionDisconnects_Throws()
        {
            RespondToOpenWith(_ => _sessionMock.Raise(p => p.Disconnected += null, EventArgs.Empty));

            using (var channel = CreateChannel())
            {
                _ = await Assert.ThrowsAsync<SshConnectionException>(
                    () => channel.OpenAsync(_remoteHost, _port, _originatorAddress, _originatorPort, CancellationToken.None));

                Assert.IsFalse(channel.IsOpen);
            }
        }

        [TestMethod]
        public async Task OpenAsync_SessionFails_ThrowsThatFailure()
        {
            var failure = new SshException("the session broke");

            RespondToOpenWith(_ => _sessionMock.Raise(p => p.ErrorOccured += null, new ExceptionEventArgs(failure)));

            using (var channel = CreateChannel())
            {
                var ex = await Assert.ThrowsAsync<SshException>(
                    () => channel.OpenAsync(_remoteHost, _port, _originatorAddress, _originatorPort, CancellationToken.None));

                Assert.AreSame(failure, ex);
                Assert.IsFalse(channel.IsOpen);
            }
        }
    }
}
