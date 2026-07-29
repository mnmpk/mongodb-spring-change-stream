package com.mzinx.mongodb.changestream.listener;

import java.util.Set;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mzinx.mongodb.changestream.ChangeStreamRegistry;
import com.mzinx.mongodb.changestream.model.ChangeStreamCoordination;
import com.mzinx.mongodb.changestream.service.ChangeStreamService;

/**
 * Listener of the coordination change stream (watching the coordination
 * collection itself). It is a pure low-latency trigger: it never mutates the
 * local runtime, it only requests an asynchronous reconcile of the affected
 * change stream, which re-reads the coordination document (single source of
 * truth) under the per-stream lock. Pure lease renewals ({@code l.until}) are
 * ignored, so the steady-state renewal traffic of AUTO_RECOVER leaders does
 * not cause reconcile churn on followers.
 * <p>
 * The periodic reconcile loop of the manager provides the same convergence
 * authoritatively, so this stream (and any events it may lose) is not a
 * correctness dependency.
 */
@Component
public class CoordinationChangeListener implements ChangeStreamListener<Document> {

    private static final String LEASE_UNTIL_KEY = ChangeStreamCoordination.LEADER_FIELD + "."
            + ChangeStreamCoordination.LEADER_UNTIL_FIELD;

    private static final Set<String> RELEVANT_KEYS = Set.of(
            ChangeStreamCoordination.LEADER_FIELD,
            ChangeStreamCoordination.MEMBERS_FIELD,
            ChangeStreamCoordination.EPOCH_FIELD,
            ChangeStreamCoordination.TERM_FIELD);

    private final ChangeStreamService<Document> changeStreamService;
    private final ChangeStreamRegistry changeStreamRegistry;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    CoordinationChangeListener(ChangeStreamService<Document> changeStreamService,
            ChangeStreamRegistry changeStreamRegistry) {
        this.changeStreamService = changeStreamService;
        this.changeStreamRegistry = changeStreamRegistry;
    }

    @Override
    public void onEvent(ChangeStreamDocument<Document> event) {
        this.logger.debug("coordination change: " + event);
        String streamId = event.getDocumentKey().getString("_id").getValue();
        if (!this.changeStreamRegistry.contains(streamId)) {
            this.logger.debug("Change stream " + streamId + " is not registered on this instance, ignoring.");
            return;
        }
        if (this.isRelevant(event)) {
            this.logger.debug("Coordination of " + streamId + " changed, requesting reconcile");
            this.changeStreamService.requestReconcile(streamId);
        }
    }

    private boolean isRelevant(ChangeStreamDocument<Document> event) {
        switch (event.getOperationType()) {
            case INSERT:
            case REPLACE:
            case DELETE:
                return true;
            case UPDATE:
                if (event.getUpdateDescription() == null || event.getUpdateDescription().getUpdatedFields() == null)
                    return true;
                // leadership, membership, epoch or term changes matter; pure
                // lease renewals (l.until) and timestamps do not
                return event.getUpdateDescription().getUpdatedFields().keySet().stream()
                        .anyMatch(key -> RELEVANT_KEYS.contains(key)
                                || (key.startsWith(ChangeStreamCoordination.LEADER_FIELD + ".")
                                        && !key.equals(LEASE_UNTIL_KEY)));
            default:
                return false;
        }
    }
}
