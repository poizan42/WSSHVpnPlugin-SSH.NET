using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;

using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using Renci.SshNet.Common;
using Renci.SshNet.Connection;
using Renci.SshNet.Tests.Common;

namespace Renci.SshNet.Tests.Classes.Connection
{
    /// <summary>
    /// Establishes what <see cref="StreamSocketSshTransport"/> actually does, in particular at
    /// teardown. The rest of the suite drives <see cref="Session"/> over
    /// <see cref="SocketSshTransport"/>, which the VPN plug-in never uses, so nothing else covers
    /// this implementation.
    /// </summary>
    [TestClass]
    public class StreamSocketSshTransportTest
    {
        /// <summary>
        /// Everything here is loopback, so the real operations are sub-millisecond. This is only
        /// generous enough to absorb scheduling noise; the point of these tests is to catch a read
        /// that never returns, so waiting long defeats them.
        /// </summary>
        private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(1);

        private AsyncSocketListener _server;
        private IPEndPoint _endPoint;
        private ManualResetEventSlim _serverConnected;
        private List<Socket> _serverSockets;
        private List<byte> _serverReceived;
        private StreamSocketSshTransport _transport;

        [TestInitialize]
        public void SetUp()
        {
            _serverConnected = new ManualResetEventSlim(initialState: false);
            _serverSockets = new List<Socket>();
            _serverReceived = new List<byte>();
            _endPoint = new IPEndPoint(IPAddress.Loopback, GetFreePort());

            _server = new AsyncSocketListener(_endPoint);
            _server.Connected += socket =>
            {
                lock (_serverSockets)
                {
                    _serverSockets.Add(socket);
                }

                _serverConnected.Set();
            };
            _server.BytesReceived += (bytes, socket) =>
            {
                lock (_serverReceived)
                {
                    _serverReceived.AddRange(bytes);
                }
            };
            _server.Start();
        }

        [TestCleanup]
        public void TearDown()
        {
            _transport?.Dispose();
            _server?.Dispose();
            _serverConnected?.Dispose();
        }

        [TestMethod]
        [Timeout(1000)]
        public void Connect_Succeeded_IsConnected()
        {
            Connect();

            Assert.IsTrue(_transport.IsConnected);
            Assert.IsNotNull(_transport.Socket);
            Assert.IsTrue(_serverConnected.Wait(WaitTimeout));
        }

        [TestMethod]
        [Timeout(1000)]
        public void Write_BytesArriveAtServer()
        {
            Connect();

            _transport.Write(new byte[] { 0x01, 0x02, 0x03, 0x04 }, 1, 2);

            var deadline = DateTime.UtcNow + WaitTimeout;
            while (DateTime.UtcNow < deadline)
            {
                lock (_serverReceived)
                {
                    if (_serverReceived.Count >= 2)
                    {
                        CollectionAssert.AreEqual(new byte[] { 0x02, 0x03 }, _serverReceived.ToArray());
                        return;
                    }
                }

                Thread.Sleep(20);
            }

            Assert.Fail("The server did not receive the bytes that were written.");
        }

        [TestMethod]
        [Timeout(1000)]
        public void Read_ReturnsBytesSentByServer()
        {
            Connect();
            var serverSocket = WaitForServerSocket();
            _ = serverSocket.Send(new byte[] { 0x0A, 0x0B, 0x0C });

            var buffer = new byte[10];
            var read = _transport.Read(buffer, 2, 8, Timeout.InfiniteTimeSpan);

            Assert.AreEqual(3, read);
            Assert.AreEqual(0x0A, buffer[2]);
            Assert.AreEqual(0x0B, buffer[3]);
            Assert.AreEqual(0x0C, buffer[4]);
        }

        [TestMethod]
        [Timeout(1000)]
        public void Read_ServerClosesGracefully_ReturnsZero()
        {
            Connect();
            var serverSocket = WaitForServerSocket();

            serverSocket.Shutdown(SocketShutdown.Both);
            serverSocket.Close();

            var read = _transport.Read(new byte[16], 0, 16, Timeout.InfiniteTimeSpan);

            Assert.AreEqual(0, read, "A graceful close should surface as end-of-stream.");
            Assert.IsFalse(_transport.IsConnected);
        }

        [TestMethod]
        [Timeout(1000)]
        public void Read_ServerAbortsConnection_ThrowsIOException()
        {
            Connect();
            var serverSocket = WaitForServerSocket();

            // Zero-timeout linger forces an RST rather than an orderly close.
            serverSocket.LingerState = new LingerOption(enable: true, seconds: 0);
            serverSocket.Close();

            // An abort is a genuine failure and propagates, unlike our own teardown. The WinRT
            // stream adapter raises COMException for this; the transport normalises it so callers
            // see the same shape of error whichever transport is in use.
            var exception = Assert.ThrowsExactly<System.IO.IOException>(
                () => _transport.Read(new byte[16], 0, 16, Timeout.InfiniteTimeSpan));

            Assert.IsInstanceOfType<System.Runtime.InteropServices.COMException>(exception.InnerException);
            Assert.IsFalse(_transport.IsConnected);
        }

        [TestMethod]
        [Timeout(1000)]
        public void Read_AfterShutdown_ReturnsZero()
        {
            Connect();
            _ = WaitForServerSocket();

            _transport.Shutdown();

            var read = _transport.Read(new byte[16], 0, 16, Timeout.InfiniteTimeSpan);

            Assert.AreEqual(0, read, "Our own shutdown should surface as a clean close, not an error.");
            Assert.IsFalse(_transport.IsConnected);
        }

        [TestMethod]
        [Timeout(1000)]
        public void Shutdown_InterruptsBlockedRead_WhichReportsCleanClose()
        {
            Connect();
            _ = WaitForServerSocket();

            var readReturned = new ManualResetEventSlim(initialState: false);
            var result = -1;
            Exception thrown = null;

            var reader = new Thread(() =>
            {
                try
                {
                    result = _transport.Read(new byte[16], 0, 16, Timeout.InfiniteTimeSpan);
                }
                catch (Exception ex)
                {
                    thrown = ex;
                }
                finally
                {
                    readReturned.Set();
                }
            });
            reader.Start();

            // Give the read a moment to actually block before interrupting it.
            Thread.Sleep(200);
            _transport.Shutdown();

            Assert.IsTrue(readReturned.Wait(WaitTimeout), "Shutdown did not interrupt the blocked read.");
            reader.Join();

            // This is the path production takes: the message listener is already blocked when the
            // session tears the transport down. Whatever the adapter raises has to be classified as
            // teardown, or the session reports a failure on an orderly disconnect.
            Assert.IsNull(thrown, $"Interrupting the read should report a clean close, but it threw {thrown}.");
            Assert.AreEqual(0, result);
        }

        [TestMethod]
        [Timeout(1000)]
        public void Read_TimeoutWithNoData_ThrowsSshOperationTimeoutException()
        {
            Connect();
            _ = WaitForServerSocket();

            _ = Assert.ThrowsExactly<SshOperationTimeoutException>(
                () => _transport.Read(new byte[16], 0, 16, TimeSpan.FromMilliseconds(200)));
        }

        [TestMethod]
        [Timeout(1000)]
        public void Dispose_IsNotConnected()
        {
            Connect();

            _transport.Dispose();

            Assert.IsFalse(_transport.IsConnected);
        }

        private void Connect()
        {
            _transport = StreamSocketSshTransport.ConnectAsync(
                    _endPoint.Address.ToString(),
                    _endPoint.Port,
                    NullLoggerFactory.Instance,
                    CancellationToken.None)
                .GetAwaiter()
                .GetResult();
        }

        private Socket WaitForServerSocket()
        {
            Assert.IsTrue(_serverConnected.Wait(WaitTimeout), "The server never accepted a connection.");

            lock (_serverSockets)
            {
                return _serverSockets[0];
            }
        }

        private static int GetFreePort()
        {
            using (var probe = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp))
            {
                probe.Bind(new IPEndPoint(IPAddress.Loopback, 0));
                return ((IPEndPoint)probe.LocalEndPoint).Port;
            }
        }
    }
}
