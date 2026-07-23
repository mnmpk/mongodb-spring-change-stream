package com.mzinx.mongodb.changestream.listener;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mzinx.mongodb.changestream.config.ChangeStreamProperties;
import com.mzinx.mongodb.changestream.model.ChangeStream.Mode;
import com.mzinx.mongodb.changestream.model.ChangeStreamRegistry;
import com.mzinx.mongodb.changestream.service.ChangeStreamService;

@Component
public class ChangeStreamWatch<T> implements ChangeStreamListener<Document>{
    private final ChangeStreamProperties changeStreamProperties;
    private final ChangeStreamService<Document> changeStreamService;
    private final Map<String, ChangeStreamRegistry<Document>> changeStreams;

    Logger logger = LoggerFactory.getLogger(getClass());

        ChangeStreamWatch(ChangeStreamProperties changeStreamProperties, ChangeStreamService<Document> changeStreamService, Map<String, ChangeStreamRegistry<Document>> changeStreams) {
                this.changeStreamProperties = changeStreamProperties;
                this.changeStreamService = changeStreamService;
                this.changeStreams = changeStreams;
        }
    public void execute(ChangeStreamDocument<Document> e){

                    this.logger.info("change stream changes: " + e);
                    String csId = e.getDocumentKey().getString("_id").getValue();
                    ChangeStreamRegistry<Document> reg = changeStreams.get(csId);
                    if (reg == null) {
                        this.logger.debug("Change stream " + csId + " is not registered on this instance, ignoring.");
                        return;
                    }
                    String leader = reg.getLeader();
                    List<String> instances = new ArrayList<>();
                    if (reg.getInstances() != null)
                        instances.addAll(reg.getInstances());
                    Date changeAt = null;
                    boolean skip = false;
                    switch (e.getOperationType()) {
                        case INSERT:
                            leader = e.getFullDocument().getString("l");
                            instances.clear();
                            instances.addAll(e.getFullDocument().getList("i", String.class));
                            changeAt = e.getFullDocument().getDate("at");
                            break;
                        case UPDATE:

                            if (e.getUpdateDescription().getUpdatedFields().containsKey("l")) {
                                BsonValue leaderValue = e.getUpdateDescription().getUpdatedFields().get("l");
                                leader = leaderValue != null && leaderValue.isString()
                                        ? leaderValue.asString().getValue()
                                        : null;
                            }
                            if (e.getUpdateDescription().getUpdatedFields().containsKey("i")) {
                                instances.clear();
                                instances.addAll(e.getUpdateDescription().getUpdatedFields().getArray("i").stream()
                                        .map(i -> i.asString().getValue()).toList());
                            }
                            if (!e.getUpdateDescription().getUpdatedFields().containsKey("l")
                                    && !e.getUpdateDescription().getUpdatedFields().containsKey("i"))
                                skip = true;
                            if (e.getUpdateDescription().getUpdatedFields().containsKey("at"))
                                changeAt = new Date(
                                        e.getUpdateDescription().getUpdatedFields().getDateTime("at").getValue());

                            break;
                        case DELETE:
                            this.logger.info("Change stream " + csId + " was removed, stop local runner.");
                            reg.setLeader(null);
                            reg.setInstances(List.of());
                            reg.stop();
                            skip = true;
                            break;
                        default:
                            break;
                    }
                    if (!skip) {
                        reg.setLeader(leader);
                        reg.setInstances(instances);
                        if (changeAt != null
                                && (reg.getEarliestChangeAt() == null || changeAt.before(reg.getEarliestChangeAt())))
                            reg.setEarliestChangeAt(changeAt);
                        if (changeStreamProperties.getHostname().equals(leader)) {
                            this.logger.info("I'm the leader, check change stream " + csId + " is running:"
                                    + reg.getChangeStream().isRunning());
                            if (Mode.AUTO_RECOVER == reg.getChangeStream().getMode()) {
                                if (!reg.getChangeStream().isRunning()) {
                                    this.logger.info("change stream " + csId + " is not running, start and take over.");
                                    this.changeStreamService.start(reg);
                                } else {
                                    this.logger.info("Still running the change stream:" + csId);
                                }
                            } else {
                                this.changeStreamService.shouldRun(reg, Mode.AUTO_SCALE == reg.getChangeStream().getMode());
                            }
                        } else {
                            this.logger.info("I'm not the leader");
                            this.changeStreamService.shouldRun(reg, Mode.AUTO_SCALE == reg.getChangeStream().getMode());
                        }
                    }
                
    }
}
