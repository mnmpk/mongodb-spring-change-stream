package com.mzinx.mongodb.changestream.listener;

import com.mongodb.client.model.changestream.ChangeStreamDocument;

/**
 * Callback invoked for every event of a change stream. Implementations are
 * registered as named Spring beans and referenced by bean name from
 * {@link com.mzinx.mongodb.changestream.model.ChangeStreamConfig#getListener()}.
 */
public interface ChangeStreamListener<T> {

    void onEvent(ChangeStreamDocument<T> event);
}
