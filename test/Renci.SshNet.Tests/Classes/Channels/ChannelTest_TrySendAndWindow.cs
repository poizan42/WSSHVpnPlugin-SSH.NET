using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
    /// Covers sending without blocking, and the window accounting that goes with it.
    /// </summary>
    [TestClass]
    public class ChannelTest_TrySendAndWindow : TestBase
    {
        private Mock<ISession> _sessionMock;
        private List<Message> _sent;
        private uint _localChannelNumber;
        private uint _localWindowSize;
        private uint _localPacketSize;
        private uint _remoteChannelNumber;
        private uint _remotePacketSize;

        protected override void OnInit()
        {
            base.OnInit();

            var random = new Random();

            _localChannelNumber = (uint)random.Next(0, int.MaxValue);
            _localWindowSize = 1000;
            _localPacketSize = 100;
            _remoteChannelNumber = (uint)random.Next(0, int.MaxValue);
            _remotePacketSize = 50;

            _sent = new List<Message>();

            _sessionMock = new Mock<ISession>(MockBehavior.Strict);
            _ = _sessionMock.Setup(p => p.SessionLoggerFactory).Returns(NullLoggerFactory.Instance);
            _ = _sessionMock.Setup(p => p.IsConnected).Returns(true);
            _ = _sessionMock.Setup(p => p.SendMessage(It.IsAny<Message>())).Callback<Message>(_sent.Add);
            _ = _sessionMock.Setup(p => p.TrySendMessage(It.IsAny<Message>())).Returns(false);
        }

        private ChannelStub CreateOpenChannel(uint remoteWindowSize)
        {
            var channel = new ChannelStub(_sessionMock.Object, _localChannelNumber, _localWindowSize, _localPacketSize);
            channel.InitializeRemoteChannelInfo(_remoteChannelNumber, remoteWindowSize, _remotePacketSize);
            channel.SetIsOpen(true);
            return channel;
        }

        /// <summary>
        /// Reads the payload a message actually carries. Data is the caller's whole array; Offset
        /// and Size describe the slice, which is how TrySend avoids copying.
        /// </summary>
        private static byte[] Payload(ChannelDataMessage message)
        {
            return message.Data.Skip(message.Offset).Take(message.Size).ToArray();
        }

        private static byte[] Payload(int size)
        {
            var data = new byte[size];
            for (var i = 0; i < size; i++)
            {
                data[i] = (byte)i;
            }

            return data;
        }

        [TestMethod]
        public void TrySend_ChannelIsNotOpen_ReportsClosed()
        {
            using (var channel = CreateOpenChannel(remoteWindowSize: 1000))
            {
                channel.SetIsOpen(false);

                var result = channel.TrySend(Payload(10), 0, 10, out var written);

                Assert.AreEqual(ChannelSendResult.Closed, result);
                Assert.AreEqual(0, written);
                Assert.AreEqual(0, _sent.Count);
            }
        }

        [TestMethod]
        public void TrySend_WindowIsLargeEnough_SendsEverything()
        {
            using (var channel = CreateOpenChannel(remoteWindowSize: 1000))
            {
                var result = channel.TrySend(Payload(120), 0, 120, out var written);

                Assert.AreEqual(ChannelSendResult.Written, result);
                Assert.AreEqual(120, written);

                // Chunked by the remote packet size: 50 + 50 + 20.
                CollectionAssert.AreEqual(new[] { 50, 50, 20 },
                                          _sent.Cast<ChannelDataMessage>().Select(m => m.Size).ToArray());
            }
        }

        [TestMethod]
        public void TrySend_WindowSmallerThanPayload_SendsWhatFitsAndReportsFull()
        {
            using (var channel = CreateOpenChannel(remoteWindowSize: 60))
            {
                var result = channel.TrySend(Payload(200), 0, 200, out var written);

                Assert.AreEqual(ChannelSendResult.WindowFull, result);
                Assert.AreEqual(60, written);
                Assert.AreEqual(60, _sent.Cast<ChannelDataMessage>().Sum(m => m.Size));
            }
        }

        [TestMethod]
        public void TrySend_WindowIsEmpty_SendsNothingAndReportsFull()
        {
            using (var channel = CreateOpenChannel(remoteWindowSize: 0))
            {
                var result = channel.TrySend(Payload(10), 0, 10, out var written);

                Assert.AreEqual(ChannelSendResult.WindowFull, result);
                Assert.AreEqual(0, written);
                Assert.AreEqual(0, _sent.Count);
            }
        }

        [TestMethod]
        public void TrySend_RespectsOffset()
        {
            using (var channel = CreateOpenChannel(remoteWindowSize: 1000))
            {
                var data = Payload(100);

                _ = channel.TrySend(data, 90, 10, out var written);

                Assert.AreEqual(10, written);
                var message = (ChannelDataMessage)_sent.Single();
                CollectionAssert.AreEqual(data.Skip(90).Take(10).ToArray(), Payload(message));
            }
        }

        [TestMethod]
        public void OnWindowAdjust_RaisesWindowAvailable()
        {
            using (var channel = CreateOpenChannel(remoteWindowSize: 0))
            {
                var raised = 0;
                channel.WindowAvailable += (_, _) => raised++;

                _sessionMock.Raise(p => p.ChannelWindowAdjustReceived += null,
                                   new MessageEventArgs<ChannelWindowAdjustMessage>(
                                       new ChannelWindowAdjustMessage(_localChannelNumber, 500)));

                Assert.AreEqual(1, raised);

                // And the window really did open up.
                var result = channel.TrySend(Payload(10), 0, 10, out var written);
                Assert.AreEqual(ChannelSendResult.Written, result);
                Assert.AreEqual(10, written);
            }
        }

        /// <summary>
        /// A remote party that overruns the window is a protocol violation, and must not be allowed
        /// to wrap the counter into a huge value and disable flow control.
        /// </summary>
        [TestMethod]
        public void ReceivingMoreThanTheWindowAllows_IsReportedAsAChannelException()
        {
            using (var channel = CreateOpenChannel(remoteWindowSize: 1000))
            {
                Exception observed = null;
                channel.Exception += (_, e) => observed = e.Exception;

                _sessionMock.Raise(p => p.ChannelDataReceived += null,
                                   new MessageEventArgs<ChannelDataMessage>(
                                       new ChannelDataMessage(_localChannelNumber, Payload((int)_localWindowSize + 1))));

                Assert.IsInstanceOfType<SshException>(observed);
                Assert.IsTrue(channel.LocalWindowSize <= _localWindowSize,
                              $"window wrapped to {channel.LocalWindowSize}");
            }
        }

        [TestMethod]
        public void DeferWindowCredit_ReceivingDataDoesNotCreditTheWindow()
        {
            using (var channel = CreateOpenChannel(remoteWindowSize: 1000))
            {
                channel.DeferWindowCredit = true;

                _sessionMock.Raise(p => p.ChannelDataReceived += null,
                                   new MessageEventArgs<ChannelDataMessage>(
                                       new ChannelDataMessage(_localChannelNumber, Payload(950))));

                Assert.AreEqual(0, _sent.OfType<ChannelWindowAdjustMessage>().Count(),
                                "the window was credited before anything consumed the bytes");
                Assert.AreEqual(_localWindowSize - 950, channel.LocalWindowSize);
            }
        }

        [TestMethod]
        public void DeferWindowCredit_CreditIsBatchedUntilHalfTheWindowAndThenFlushed()
        {
            using (var channel = CreateOpenChannel(remoteWindowSize: 1000))
            {
                channel.DeferWindowCredit = true;

                _sessionMock.Raise(p => p.ChannelDataReceived += null,
                                   new MessageEventArgs<ChannelDataMessage>(
                                       new ChannelDataMessage(_localChannelNumber, Payload(900))));

                // Below half the window: nothing due yet.
                Assert.IsFalse(channel.ReleaseReceivedData(100));
                Assert.IsFalse(channel.ReleaseReceivedData(200));

                // Crossing half the window: a flush is due.
                Assert.IsTrue(channel.ReleaseReceivedData(300));

                // Recording never sends by itself; sending blocks during a key exchange, and the
                // consumer's thread must not inherit that.
                Assert.AreEqual(0, _sent.OfType<ChannelWindowAdjustMessage>().Count());

                channel.FlushWindowCredit();

                var adjust = _sent.OfType<ChannelWindowAdjustMessage>().Single();
                Assert.AreEqual(600u, adjust.BytesToAdd);
                Assert.AreEqual(_localWindowSize - 900 + 600, channel.LocalWindowSize);
            }
        }

        [TestMethod]
        public void DeferWindowCredit_FlushWithNothingOutstanding_SendsNothing()
        {
            using (var channel = CreateOpenChannel(remoteWindowSize: 1000))
            {
                channel.DeferWindowCredit = true;

                channel.FlushWindowCredit();

                Assert.AreEqual(0, _sent.Count);
            }
        }

        /// <summary>
        /// Without the flag the old behaviour has to be untouched, since every other consumer in the
        /// library relies on it.
        /// </summary>
        [TestMethod]
        public void WithoutDeferWindowCredit_TheWindowIsStillCreditedOnReceipt()
        {
            using (var channel = CreateOpenChannel(remoteWindowSize: 1000))
            {
                _sessionMock.Raise(p => p.ChannelDataReceived += null,
                                   new MessageEventArgs<ChannelDataMessage>(
                                       new ChannelDataMessage(_localChannelNumber, Payload(950))));

                var adjust = _sent.OfType<ChannelWindowAdjustMessage>().Single();
                Assert.AreEqual(950u, adjust.BytesToAdd);
                Assert.AreEqual(_localWindowSize, channel.LocalWindowSize);
                Assert.IsFalse(channel.ReleaseReceivedData(950), "release should be inert when crediting on receipt");
            }
        }

        [TestMethod]
        public void Payload_IsSentUnchanged()
        {
            using (var channel = CreateOpenChannel(remoteWindowSize: 1000))
            {
                var data = Encoding.ASCII.GetBytes("the quick brown fox");

                _ = channel.TrySend(data, 0, data.Length, out _);

                var message = (ChannelDataMessage)_sent.Single();
                CollectionAssert.AreEqual(data, Payload(message));
            }
        }
    }
}
