package com.mzinx.mongodb.changestream.service;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import com.mzinx.mongodb.changestream.config.ChangeStreamProperties;
import com.mzinx.mongodb.changestream.model.ChangeStreamConfig;

/**
 * Stores {@link ChangeStreamConfig} documents in the
 * {@code change-stream.changeStreamConfigCollection} collection.
 * <p>
 * The {@link com.mzinx.mongodb.changestream.ChangeStreamManager}
 * fetches these configs and manages the change stream life cycles accordingly.
 */
@Service
public class ChangeStreamConfigService {
    Logger logger = LoggerFactory.getLogger(getClass());

    private final MongoTemplate mongoTemplate;
    private final ChangeStreamProperties changeStreamProperties;

    ChangeStreamConfigService(MongoTemplate mongoTemplate, ChangeStreamProperties changeStreamProperties) {
        this.mongoTemplate = mongoTemplate;
        this.changeStreamProperties = changeStreamProperties;
    }

    /**
     * Creates or updates a change stream config. The running change stream will
     * be started/restarted by the manager on its next refresh.
     */
    public ChangeStreamConfig save(ChangeStreamConfig config) {
        if (config.getId() == null || config.getId().isBlank())
            throw new IllegalArgumentException("Change stream config requires an id");
        config.setUpdatedAt(new Date());
        logger.info("Saving change stream config: " + config.getId());
        return mongoTemplate.save(config, changeStreamProperties.getChangeStreamConfigCollection());
    }

    public ChangeStreamConfig findById(String id) {
        return mongoTemplate.findById(id, ChangeStreamConfig.class,
                changeStreamProperties.getChangeStreamConfigCollection());
    }

    public List<ChangeStreamConfig> findAll() {
        return mongoTemplate.findAll(ChangeStreamConfig.class,
                changeStreamProperties.getChangeStreamConfigCollection());
    }

    /**
     * Removes a change stream config. The running change stream will be stopped
     * by the manager on its next refresh.
     */
    public void delete(String id) {
        logger.info("Deleting change stream config: " + id);
        mongoTemplate.remove(new Query(Criteria.where("_id").is(id)),
                changeStreamProperties.getChangeStreamConfigCollection());
    }
}
