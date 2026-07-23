package com.mzinx.mongodb.changestream.listener;

import com.mongodb.client.model.changestream.ChangeStreamDocument;

public interface ChangeStreamListener<T> {
    public void execute(ChangeStreamDocument<T> doc);
}
