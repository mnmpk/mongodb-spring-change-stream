package com.mzinx.changestreamtest;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

import org.bson.Document;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mzinx.mongodb.changestream.listener.ChangeStreamListener;

/**
 * {@link ChangeStreamListener} test double that records every received change
 * stream event so tests can assert on event delivery.
 */
public class TestRecordingListener implements ChangeStreamListener<Document> {

    private final List<ChangeStreamDocument<Document>> events = new CopyOnWriteArrayList<>();

    @Override
    public void execute(ChangeStreamDocument<Document> doc) {
        this.events.add(doc);
    }

    public List<ChangeStreamDocument<Document>> getEvents() {
        return this.events;
    }

    public boolean hasEvent(Predicate<ChangeStreamDocument<Document>> predicate) {
        return this.events.stream().anyMatch(predicate);
    }

    public void clear() {
        this.events.clear();
    }
}
