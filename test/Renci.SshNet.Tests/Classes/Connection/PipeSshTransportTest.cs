using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using Renci.SshNet.Common;
using Renci.SshNet.Connection;

namespace Renci.SshNet.Tests.Classes.Connection
{
    /// <summary>
    /// Establishes the <see cref="PipeSshTransport"/> contract the VPN plug-in's
    /// platform-owned-transport architecture stands on: banner-style byte-at-a-time reads,
    /// timeout semantics, shutdown interrupting a blocked read, and a single reader and writer
    /// running concurrently.
    /// </summary>
    [TestClass]
    public class PipeSshTransportTest
    {
        /// <summary>
        /// Everything here is in-process, so real operations are sub-millisecond. The point of
        /// these tests is to catch a read that never returns; waiting long defeats them.
        /// </summary>
        private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(1);

        private List<byte> _sent;
        private PipeSshTransport _transport;

        [TestInitialize]
        public void SetUp()
        {
            _sent = new List<byte>();
            _transport = new PipeSshTransport(bytes =>
            {
                lock (_sent)
                {
                    _sent.AddRange(bytes.ToArray());
                }
            });
        }

        [TestCleanup]
        public void TearDown()
        {
            _transport?.Dispose();
        }

        [TestMethod]
        [Timeout(1000)]
        public void Read_DeliveredBytes_OneAtATime()
        {
            // The banner exchange reads exactly like this: one byte per call, finite timeout.
            _transport.Deliver(Encoding.ASCII.GetBytes("SSH-2.0-x\r\n"));

            var received = new StringBuilder();
            var one = new byte[1];

            for (var i = 0; i < 11; i++)
            {
                Assert.AreEqual(1, _transport.Read(one, 0, 1, WaitTimeout));
                _ = received.Append((char)one[0]);
            }

            Assert.AreEqual("SSH-2.0-x\r\n", received.ToString());
        }

        [TestMethod]
        [Timeout(1000)]
        public void Read_BeforeDeliver_BlocksUntilDelivery()
        {
            var buffer = new byte[16];
            var read = Task.Run(() => _transport.Read(buffer, 0, buffer.Length, WaitTimeout));

            Assert.IsFalse(read.Wait(TimeSpan.FromMilliseconds(50)), "the read returned with nothing delivered");

            _transport.Deliver(new byte[] { 1, 2, 3 });

            Assert.IsTrue(read.Wait(WaitTimeout));
            Assert.AreEqual(3, read.Result);
            Assert.AreEqual(2, buffer[1]);
        }

        [TestMethod]
        [Timeout(1000)]
        public void Read_Timeout_Throws()
        {
            var buffer = new byte[1];

            _ = Assert.ThrowsExactly<SshOperationTimeoutException>(
                () => _transport.Read(buffer, 0, 1, TimeSpan.FromMilliseconds(20)));
        }

        [TestMethod]
        [Timeout(1000)]
        public void Read_Shutdown_UnblocksToZero()
        {
            var buffer = new byte[1];
            var read = Task.Run(() => _transport.Read(buffer, 0, 1, Timeout.InfiniteTimeSpan));

            Assert.IsFalse(read.Wait(TimeSpan.FromMilliseconds(50)), "the read returned before shutdown");

            _transport.Shutdown();

            Assert.IsTrue(read.Wait(WaitTimeout));
            Assert.AreEqual(0, read.Result);
        }

        [TestMethod]
        [Timeout(1000)]
        public void Read_AfterShutdown_ReturnsZeroImmediately()
        {
            _transport.Shutdown();

            Assert.AreEqual(0, _transport.Read(new byte[4], 0, 4, Timeout.InfiniteTimeSpan));
        }

        [TestMethod]
        [Timeout(1000)]
        public void Read_PendingBytesSurviveShutdownRace_DrainedBeforeZero()
        {
            // Bytes delivered before shutdown are still readable: the session's final messages
            // may already be buffered when teardown begins.
            _transport.Deliver(new byte[] { 42 });
            _transport.Shutdown();

            var buffer = new byte[4];
            Assert.AreEqual(1, _transport.Read(buffer, 0, 4, WaitTimeout));
            Assert.AreEqual(42, buffer[0]);
            Assert.AreEqual(0, _transport.Read(buffer, 0, 4, WaitTimeout));
        }

        [TestMethod]
        [Timeout(1000)]
        public void Write_ForwardsVerbatimToTheDelegate()
        {
            var payload = Encoding.ASCII.GetBytes("0123456789");

            _transport.Write(payload, 2, 5);

            lock (_sent)
            {
                CollectionAssert.AreEqual(Encoding.ASCII.GetBytes("23456"), _sent);
            }
        }

        [TestMethod]
        [Timeout(1000)]
        public void Write_AfterShutdown_Throws()
        {
            _transport.Shutdown();

            _ = Assert.ThrowsExactly<SshConnectionException>(
                () => _transport.Write(new byte[] { 1 }, 0, 1));
        }

        [TestMethod]
        [Timeout(1000)]
        public void Shutdown_Twice_DoesNotThrow()
        {
            _transport.Shutdown();
            _transport.Shutdown();
            _transport.Dispose();
            _transport.Shutdown();
        }

        [TestMethod]
        [Timeout(5000)]
        public void ConcurrentReaderAndWriter_AllBytesArriveInOrder()
        {
            // The transport contract: one reader (the message listener) and one writer running
            // concurrently. 64 KiB through a 64 KiB initial buffer also exercises growth and the
            // slide-down path.
            const int Total = 64 * 1024;
            const int ChunkSize = 256; // divides Total exactly

            var producer = Task.Run(() =>
            {
                var chunk = new byte[ChunkSize];
                var value = 0;

                for (var sent = 0; sent < Total; sent += chunk.Length)
                {
                    for (var i = 0; i < chunk.Length; i++)
                    {
                        chunk[i] = (byte)(value++ & 0xFF);
                    }

                    _transport.Deliver(chunk);
                }
            });

            var buffer = new byte[997];
            var expected = 0;
            var received = 0;

            while (received < Total)
            {
                var read = _transport.Read(buffer, 0, buffer.Length, WaitTimeout);
                Assert.AreNotEqual(0, read, "the transport reported closed mid-stream");

                for (var i = 0; i < read; i++)
                {
                    Assert.AreEqual((byte)(expected++ & 0xFF), buffer[i]);
                }

                received += read;
            }

            Assert.IsTrue(producer.Wait(WaitTimeout));
            Assert.AreEqual(Total, expected);
        }
    }
}
