package com.mzinx.mongodb.changestream.listener;

import java.util.Map;

import com.mongodb.client.model.changestream.ChangeStreamDocument;

/**
 * Callback invoked for every event of a change stream. Implementations are
 * registered as named Spring beans and referenced by bean name from
 * {@link com.mzinx.mongodb.changestream.model.ChangeStreamConfig#getListener()}.
 *
 * @param <T> the change stream document type
 */
public interface ChangeStreamListener<T> {

    /**
     * Handles a single change stream event.
     *
     * @param streamId   the id of the change stream that produced the event (equal
     *                   to the {@link com.mzinx.mongodb.changestream.model.ChangeStreamConfig}
     *                   id). Lets a single listener bean serve multiple streams and
     *                   look up its own per-stream configuration without hardcoding
     *                   the id.
     * @param attributes the free-form {@code attributes} of the originating
     *                   {@link com.mzinx.mongodb.changestream.model.ChangeStreamConfig},
     *                   snapshotted when the stream was started and delivered on
     *                   every event so a listener needs no per-event database
     *                   lookup to read its configuration. May be {@code null} (e.g.
     *                   for the internal coordination stream, which has no config).
     * @param event      the change stream event
     */
    void onEvent(String streamId, Map<String, Object> attributes, ChangeStreamDocument<T> event);
}
