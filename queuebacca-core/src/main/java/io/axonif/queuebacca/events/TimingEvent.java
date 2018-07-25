package io.axonif.queuebacca.events;

import static java.util.Objects.requireNonNull;

import java.time.Instant;

import io.axonif.queuebacca.MessageBin;

/**
 * An event containing timing information for a specific {@link io.axonif.queuebacca.Message} from a {@link MessageBin}
 */
public class TimingEvent {

    private final MessageBin messageBin;
    private final Class<?> messageType;
    private final String messageId;
    private final Instant timestamp;
    private final long duration;

    public TimingEvent(MessageBin messageBin, Class<?> messageType, String messageId, Instant timestamp, long duration) {
        this.messageBin = requireNonNull(messageBin);
        this.messageType = requireNonNull(messageType);
        this.messageId = requireNonNull(messageId);
        this.timestamp = requireNonNull(timestamp);
        this.duration = duration;
    }

    public MessageBin getMessageBin() {
        return messageBin;
    }

    public Class<?> getMessageType() {
        return messageType;
    }

    public String getMessageId() {
        return messageId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public double getDuration() {
        return duration;
    }
}