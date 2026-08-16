using System;
using System.Linq;
using System.Threading;

using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using Moq;

using Renci.SshNet.Channels;
using Renci.SshNet.Common;
using Renci.SshNet.Messages.Connection;
using Renci.SshNet.Tests.Common;

namespace Renci.SshNet.Tests.Classes
{
    /// <summary>
    /// Covers the receive buffer's one hard rule: nothing may mutate, in place, the array a
    /// consumer's peeked segment points into.
    /// </summary>
    /// <remarks>
    /// The rule was broken by compacting on the message listener thread while a consumer was
    /// reading a <c>TryRead</c> segment. The tear is invisible on this side - no exception, no
    /// counter - and surfaces as the far end resetting a perfectly healthy connection, because a
    /// torn TLS record fails its MAC before anything else notices. Space is instead reclaimed in
    /// <see cref="DirectTcpipStream.FlushWindowCredit"/>, on the consumer's own thread, where the
    /// contract already says peeked segments die.
    /// </remarks>
    [TestClass]
    public class DirectTcpipStreamTest_ReceiveBuffer : TestBase
    {
        private Mock<ISession> _sessionMock;
        private uint _localChannelNumber;

        protected override void OnInit()
        {
            base.OnInit();

            _localChannelNumber = 42;

            _sessionMock = new Mock<ISession>(MockBehavior.Strict);
            _ = _sessionMock.Setup(p => p.SessionLoggerFactory).Returns(NullLoggerFactory.Instance);
            _ = _sessionMock.Setup(p => p.IsConnected).Returns(false);
            _ = _sessionMock.Setup(p => p.TryWait(It.IsAny<EventWaitHandle>(), It.IsAny<TimeSpan>()))
                            .Returns(WaitResult.TimedOut);
        }

        private DirectTcpipStream CreateStream(uint windowSize, int bufferSize)
        {
            var channel = new ChannelDirectTcpip(_sessionMock.Object, _localChannelNumber, windowSize, localPacketSize: 8);
            return new DirectTcpipStream(channel, bufferSize);
        }

        private void Deliver(byte[] payload)
        {
            _sessionMock.Raise(p => p.ChannelDataReceived += null,
                               new MessageEventArgs<ChannelDataMessage>(
                                   new ChannelDataMessage(_localChannelNumber, payload)));
        }

        private static byte[] Bytes(int start, int count)
        {
            return Enumerable.Range(start, count).Select(v => (byte)v).ToArray();
        }

        /// <summary>
        /// A consumer that has released and credited some bytes may hold a peeked segment while the
        /// remote party fills the space the credit granted. The append the credit permits must not
        /// move the bytes under the held segment.
        /// </summary>
        [TestMethod]
        public void AHeldSegment_SurvivesTheAppendTheCreditPermits()
        {
            using (var stream = CreateStream(windowSize: 64, bufferSize: 64))
            {
                Deliver(Bytes(0, 32));

                _ = stream.Advance(16);
                stream.FlushWindowCredit();

                Assert.IsTrue(stream.TryRead(out var held));
                CollectionAssert.AreEqual(Bytes(16, 16), held.ToArray());

                // 48 bytes fit the credited window, but not the tail of a buffer whose front 16
                // bytes were released - unless the flush reclaimed them, or the append copies out
                // of place. Compacting in place right here is what tore segments.
                Deliver(Bytes(100, 48));

                CollectionAssert.AreEqual(Bytes(16, 16), held.ToArray(),
                    "The append moved the bytes under a segment the consumer was still holding.");

                // And nothing was lost: the stream now carries the held bytes then the new ones.
                Assert.IsTrue(stream.TryRead(out var everything));
                CollectionAssert.AreEqual(Bytes(16, 16).Concat(Bytes(100, 48)).ToArray(), everything.ToArray());
            }
        }

        /// <summary>
        /// With the buffer sized exactly to the window, releasing and crediting in a loop must keep
        /// working: the space the credit grants has to actually be free at the tail each time.
        /// </summary>
        [TestMethod]
        public void ReleasingAndCrediting_KeepsTheWindowAndTheBufferInStep()
        {
            using (var stream = CreateStream(windowSize: 32, bufferSize: 32))
            {
                for (var round = 0; round < 3; round++)
                {
                    Deliver(Bytes(round * 32, 32));

                    Assert.IsTrue(stream.TryRead(out var data));
                    CollectionAssert.AreEqual(Bytes(round * 32, 32), data.ToArray());

                    _ = stream.Advance(32);
                    stream.FlushWindowCredit();
                }
            }
        }
    }
}
