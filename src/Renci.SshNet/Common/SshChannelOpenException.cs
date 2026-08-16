using System;
#if NETFRAMEWORK
using System.Runtime.Serialization;
#endif // NETFRAMEWORK

namespace Renci.SshNet.Common
{
    /// <summary>
    /// The exception that is thrown when the server answers a channel open with
    /// SSH_MSG_CHANNEL_OPEN_FAILURE.
    /// </summary>
    /// <remarks>
    /// Distinct from the failures that never got a verdict - a timeout, a dropped session - because
    /// the difference is what a caller may conclude. A refusal is the server's statement about the
    /// destination, and a caller opening channels per connection can reasonably cache it; a local
    /// failure says nothing about the destination at all.
    /// </remarks>
#if NETFRAMEWORK
    [Serializable]
#endif // NETFRAMEWORK
    public class SshChannelOpenException : SshException
    {
        /// <summary>The server's policy forbids this forwarding (SSH_OPEN_ADMINISTRATIVELY_PROHIBITED).</summary>
        public const uint AdministrativelyProhibited = 1;

        /// <summary>The server tried to connect to the destination and could not (SSH_OPEN_CONNECT_FAILED).</summary>
        public const uint ConnectFailed = 2;

        /// <summary>The server does not know the channel type (SSH_OPEN_UNKNOWN_CHANNEL_TYPE).</summary>
        public const uint UnknownChannelType = 3;

        /// <summary>The server is out of resources for new channels (SSH_OPEN_RESOURCE_SHORTAGE).</summary>
        public const uint ResourceShortage = 4;

        /// <summary>
        /// Gets the reason code the server gave, or zero when none was carried.
        /// </summary>
        public uint ReasonCode { get; }

        /// <summary>
        /// Gets a value indicating whether the refusal is a verdict about the destination - its
        /// host and port - rather than about the server or the request.
        /// </summary>
        /// <value>
        /// <see langword="true"/> for <see cref="AdministrativelyProhibited"/> and
        /// <see cref="ConnectFailed"/>; otherwise, <see langword="false"/> - a resource shortage is
        /// about the server's moment, and an unknown channel type is about the request.
        /// </value>
        public bool IsAboutTheDestination
        {
            get { return ReasonCode is AdministrativelyProhibited or ConnectFailed; }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SshChannelOpenException"/> class.
        /// </summary>
        public SshChannelOpenException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SshChannelOpenException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public SshChannelOpenException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SshChannelOpenException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="innerException">The inner exception.</param>
        public SshChannelOpenException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SshChannelOpenException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="reasonCode">The reason code the server gave.</param>
        public SshChannelOpenException(string message, uint reasonCode)
            : base(message)
        {
            ReasonCode = reasonCode;
        }

#if NETFRAMEWORK
        /// <summary>
        /// Initializes a new instance of the <see cref="SshChannelOpenException"/> class.
        /// </summary>
        /// <param name="info">The <see cref="SerializationInfo"/> that holds the serialized object data about the exception being thrown.</param>
        /// <param name="context">The <see cref="StreamingContext"/> that contains contextual information about the source or destination.</param>
        /// <exception cref="ArgumentNullException">The <paramref name="info"/> parameter is <see langword="null"/>.</exception>
        /// <exception cref="SerializationException">The class name is <see langword="null"/> or <see cref="Exception.HResult"/> is zero (0). </exception>
        protected SshChannelOpenException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
#endif // NETFRAMEWORK
    }
}
