package io.axonif.queuebacca.events;

/**
 * A callback interface for {@link TimingEvent TimingEvents}.
 */
public interface TimingEventListener {

    void handleTimingEvent(TimingEvent event);
}
