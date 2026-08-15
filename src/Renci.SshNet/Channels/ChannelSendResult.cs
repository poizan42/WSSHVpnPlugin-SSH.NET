namespace Renci.SshNet.Channels
{
    /// <summary>
    /// The outcome of a non-blocking send on a channel.
    /// </summary>
    public enum ChannelSendResult
    {
        /// <summary>
        /// All of the data was sent.
        /// </summary>
        Written,

        /// <summary>
        /// The remote window ran out, so only part of the data was sent - possibly none of it. The
        /// caller should retry the remainder when the window is adjusted.
        /// </summary>
        WindowFull,

        /// <summary>
        /// The channel is closed and no further data can be sent.
        /// </summary>
        /// <remarks>
        /// Distinct from <see cref="WindowFull"/> on purpose: a caller that retries on a full window
        /// would spin forever against a closed channel, which is what happens when a send on a closed
        /// channel is reported as sending nothing.
        /// </remarks>
        Closed,
    }
}
