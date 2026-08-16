#nullable enable
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

using Renci.SshNet.Common;

namespace Renci.SshNet
{
    /// <summary>
    /// Provides client connection to SSH server.
    /// </summary>
    public interface ISshClient : IBaseClient
    {
        /// <summary>
        /// Gets the list of forwarded ports.
        /// </summary>
        IEnumerable<ForwardedPort> ForwardedPorts { get; }

        /// <summary>
        /// Adds the forwarded port.
        /// </summary>
        /// <param name="port">The port.</param>
        /// <exception cref="InvalidOperationException">Forwarded port is already added to a different client.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="port"/> is <see langword="null"/>.</exception>
        /// <exception cref="SshConnectionException">Client is not connected.</exception>
        public void AddForwardedPort(ForwardedPort port);

        /// <summary>
        /// Stops and removes the forwarded port from the list.
        /// </summary>
        /// <param name="port">Forwarded port.</param>
        /// <exception cref="ArgumentNullException"><paramref name="port"/> is <see langword="null"/>.</exception>
        public void RemoveForwardedPort(ForwardedPort port);

        /// <summary>
        /// Creates the command to be executed.
        /// </summary>
        /// <param name="commandText">The command text.</param>
        /// <returns><see cref="SshCommand"/> object.</returns>
        /// <exception cref="SshConnectionException">Client is not connected.</exception>
        public SshCommand CreateCommand(string commandText);

        /// <summary>
        /// Creates the command to be executed with specified encoding.
        /// </summary>
        /// <param name="commandText">The command text.</param>
        /// <param name="encoding">The encoding to use for results.</param>
        /// <returns><see cref="SshCommand"/> object which uses specified encoding.</returns>
        /// <remarks>This method will change current default encoding.</remarks>
        /// <exception cref="SshConnectionException">Client is not connected.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="commandText"/> or <paramref name="encoding"/> is <see langword="null"/>.</exception>
        public SshCommand CreateCommand(string commandText, Encoding encoding);

        /// <summary>
        /// Creates and executes the command.
        /// </summary>
        /// <param name="commandText">The command text.</param>
        /// <returns>Returns an instance of <see cref="SshCommand"/> with execution results.</returns>
        /// <remarks>This method internally uses asynchronous calls.</remarks>
        /// <exception cref="ArgumentException">CommandText property is empty.</exception>
        /// <exception cref="SshException">Invalid Operation - An existing channel was used to execute this command.</exception>
        /// <exception cref="InvalidOperationException">Asynchronous operation is already in progress.</exception>
        /// <exception cref="SshConnectionException">Client is not connected.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="commandText"/> is <see langword="null"/>.</exception>
        public SshCommand RunCommand(string commandText);

        /// <summary>
        /// Creates the shell.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        /// <param name="extendedOutput">The extended output.</param>
        /// <param name="terminalName">Name of the terminal.</param>
        /// <param name="columns">The columns.</param>
        /// <param name="rows">The rows.</param>
        /// <param name="width">The width.</param>
        /// <param name="height">The height.</param>
        /// <param name="terminalModes">The terminal mode.</param>
        /// <param name="bufferSize">Size of the internal read buffer.</param>
        /// <returns>
        /// Returns a representation of a <see cref="Shell" /> object.
        /// </returns>
        /// <exception cref="SshConnectionException">Client is not connected.</exception>
        public Shell CreateShell(Stream input, Stream output, Stream extendedOutput, string terminalName, uint columns, uint rows, uint width, uint height, IDictionary<TerminalModes, uint>? terminalModes, int bufferSize);

        /// <summary>
        /// Creates the shell.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        /// <param name="extendedOutput">The extended output.</param>
        /// <param name="terminalName">Name of the terminal.</param>
        /// <param name="columns">The columns.</param>
        /// <param name="rows">The rows.</param>
        /// <param name="width">The width.</param>
        /// <param name="height">The height.</param>
        /// <param name="terminalModes">The terminal mode.</param>
        /// <returns>
        /// Returns a representation of a <see cref="Shell" /> object.
        /// </returns>
        /// <exception cref="SshConnectionException">Client is not connected.</exception>
        public Shell CreateShell(Stream input, Stream output, Stream extendedOutput, string terminalName, uint columns, uint rows, uint width, uint height, IDictionary<TerminalModes, uint> terminalModes);

        /// <summary>
        /// Creates the shell.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        /// <param name="extendedOutput">The extended output.</param>
        /// <returns>
        /// Returns a representation of a <see cref="Shell" /> object.
        /// </returns>
        /// <exception cref="SshConnectionException">Client is not connected.</exception>
        public Shell CreateShell(Stream input, Stream output, Stream extendedOutput);

        /// <summary>
        /// Creates the shell.
        /// </summary>
        /// <param name="encoding">The encoding to use to send the input.</param>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        /// <param name="extendedOutput">The extended output.</param>
        /// <param name="terminalName">Name of the terminal.</param>
        /// <param name="columns">The columns.</param>
        /// <param name="rows">The rows.</param>
        /// <param name="width">The width.</param>
        /// <param name="height">The height.</param>
        /// <param name="terminalModes">The terminal mode.</param>
        /// <param name="bufferSize">Size of the internal read buffer.</param>
        /// <returns>
        /// Returns a representation of a <see cref="Shell" /> object.
        /// </returns>
        /// <exception cref="SshConnectionException">Client is not connected.</exception>
        public Shell CreateShell(Encoding encoding, string input, Stream output, Stream extendedOutput, string terminalName, uint columns, uint rows, uint width, uint height, IDictionary<TerminalModes, uint>? terminalModes, int bufferSize);

        /// <summary>
        /// Creates the shell.
        /// </summary>
        /// <param name="encoding">The encoding.</param>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        /// <param name="extendedOutput">The extended output.</param>
        /// <param name="terminalName">Name of the terminal.</param>
        /// <param name="columns">The columns.</param>
        /// <param name="rows">The rows.</param>
        /// <param name="width">The width.</param>
        /// <param name="height">The height.</param>
        /// <param name="terminalModes">The terminal modes.</param>
        /// <returns>
        /// Returns a representation of a <see cref="Shell" /> object.
        /// </returns>
        /// <exception cref="SshConnectionException">Client is not connected.</exception>
        public Shell CreateShell(Encoding encoding, string input, Stream output, Stream extendedOutput, string terminalName, uint columns, uint rows, uint width, uint height, IDictionary<TerminalModes, uint> terminalModes);

        /// <summary>
        /// Creates the shell.
        /// </summary>
        /// <param name="encoding">The encoding.</param>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        /// <param name="extendedOutput">The extended output.</param>
        /// <returns>
        /// Returns a representation of a <see cref="Shell" /> object.
        /// </returns>
        /// <exception cref="SshConnectionException">Client is not connected.</exception>
        public Shell CreateShell(Encoding encoding, string input, Stream output, Stream extendedOutput);

        /// <summary>
        /// Creates the shell without allocating a pseudo terminal,
        /// similar to the <c>ssh -T</c> option.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        /// <param name="extendedOutput">The extended output.</param>
        /// <param name="bufferSize">Size of the internal read buffer.</param>
        /// <returns>
        /// Returns a representation of a <see cref="Shell" /> object.
        /// </returns>
        /// <exception cref="SshConnectionException">Client is not connected.</exception>
        public Shell CreateShellNoTerminal(Stream input, Stream output, Stream extendedOutput, int bufferSize = -1);

        /// <summary>
        /// Creates the shell stream.
        /// </summary>
        /// <param name="terminalName">The <c>TERM</c> environment variable.</param>
        /// <param name="columns">The terminal width in columns.</param>
        /// <param name="rows">The terminal height in rows.</param>
        /// <param name="width">The terminal width in pixels.</param>
        /// <param name="height">The terminal height in pixels.</param>
        /// <param name="bufferSize">The size of the buffer.</param>
        /// <returns>
        /// The created <see cref="ShellStream"/> instance.
        /// </returns>
        /// <exception cref="SshConnectionException">Client is not connected.</exception>
        /// <remarks>
        /// <para>
        /// The <c>TERM</c> environment variable contains an identifier for the text window's capabilities.
        /// You can get a detailed list of these capabilities by using the ‘infocmp’ command.
        /// </para>
        /// <para>
        /// The column/row dimensions override the pixel dimensions (when nonzero). Pixel dimensions refer
        /// to the drawable area of the window.
        /// </para>
        /// </remarks>
        public ShellStream CreateShellStream(string terminalName, uint columns, uint rows, uint width, uint height, int bufferSize);

        /// <summary>
        /// Creates the shell stream.
        /// </summary>
        /// <param name="terminalName">The <c>TERM</c> environment variable.</param>
        /// <param name="columns">The terminal width in columns.</param>
        /// <param name="rows">The terminal height in rows.</param>
        /// <param name="width">The terminal width in pixels.</param>
        /// <param name="height">The terminal height in pixels.</param>
        /// <param name="bufferSize">The size of the buffer.</param>
        /// <param name="terminalModeValues">The terminal mode values.</param>
        /// <returns>
        /// The created <see cref="ShellStream"/> instance.
        /// </returns>
        /// <exception cref="SshConnectionException">Client is not connected.</exception>
        /// <remarks>
        /// <para>
        /// The <c>TERM</c> environment variable contains an identifier for the text window's capabilities.
        /// You can get a detailed list of these capabilities by using the ‘infocmp’ command.
        /// </para>
        /// <para>
        /// The column/row dimensions override the pixel dimensions (when non-zero). Pixel dimensions refer
        /// to the drawable area of the window.
        /// </para>
        /// </remarks>
        public ShellStream CreateShellStream(string terminalName, uint columns, uint rows, uint width, uint height, int bufferSize, IDictionary<TerminalModes, uint>? terminalModeValues);

        /// <summary>
        /// Creates the shell stream without allocating a pseudo terminal,
        /// similar to the <c>ssh -T</c> option.
        /// </summary>
        /// <param name="bufferSize">The size of the buffer.</param>
        /// <returns>
        /// The created <see cref="ShellStream"/> instance.
        /// </returns>
        /// <exception cref="SshConnectionException">Client is not connected.</exception>
        public ShellStream CreateShellStreamNoTerminal(int bufferSize = -1);

        /// <summary>
        /// Opens a direct-tcpip channel to the given host and port, and returns it as a byte stream.
        /// </summary>
        /// <param name="host">The host to connect to on the far side.</param>
        /// <param name="port">The port to connect to on the far side.</param>
        /// <param name="bufferSize">The size of the receive buffer. Must be at least <paramref name="windowSize"/>.</param>
        /// <param name="windowSize">
        /// The initial size of the local window, which is how much the remote party may send before
        /// the consumer has to release what it has taken.
        /// </param>
        /// <returns>The stream.</returns>
        /// <remarks>
        /// Unlike a forwarded port, nothing is listening locally and no socket is involved: the
        /// caller owns the bytes in both directions. The window defaults to a small value rather than
        /// the session default, because the point of this shape is that the window is real
        /// backpressure.
        /// </remarks>
        public DirectTcpipStream CreateDirectTcpipStream(string host, uint port, int bufferSize = 65536, uint windowSize = 8192);

        /// <summary>
        /// Creates a <see cref="DirectTcpipStream"/> over a channel that has not been opened yet.
        /// </summary>
        /// <param name="bufferSize">The size of the receive buffer. Must be at least <paramref name="windowSize"/>.</param>
        /// <param name="windowSize">
        /// The initial size of the local window, which is how much the remote party may send before
        /// the consumer has to release what it has taken.
        /// </param>
        /// <returns>The stream, ready for <see cref="DirectTcpipStream.OpenAsync"/>.</returns>
        /// <remarks>
        /// The two-step shape exists for callers that open many channels concurrently and cannot
        /// afford to park a thread per open: the open is awaited, a caller that stops waiting hands
        /// the stream to <see cref="DirectTcpipStream.AbandonAsync"/>, and the abandoned open then
        /// costs an object until the server answers rather than a blocked thread.
        /// </remarks>
        public DirectTcpipStream CreateUnopenedDirectTcpipStream(int bufferSize = 65536, uint windowSize = 8192);
    }
}
