package io.axonif.queuebacca.events;

import static java.util.Objects.requireNonNull;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import io.axonif.queuebacca.MessageBin;

/**
 * Handles registration of {@link TimingEventListener TimingEventListeners} and firing {@link TimingEvent TimingEvents} for all registered listeners
 */
public class TimingEventSupport {

    private final List<TimingEventListener> timingEventListeners = new ArrayList<>();

    public void addListener(TimingEventListener timingEventListener) {
        requireNonNull(timingEventListener);

        timingEventListeners.add(timingEventListener);
    }

    public void removeListener(TimingEventListener timingEventListener) {
        requireNonNull(timingEventListener);

        timingEventListeners.remove(timingEventListener);
    }

    public void fireEvent(MessageBin messageBin, Class<?> messageType, String messageId, long duration) {
        TimingEvent timingEvent = new TimingEvent(messageBin, messageType, messageId, Instant.now(), duration);
        timingEventListeners.forEach(listener -> listener.handleTimingEvent(timingEvent));
    }
}
