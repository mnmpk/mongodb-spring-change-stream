package com.mzinx.mongodb.changestream.bootstrap;

import java.util.List;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mzinx.mongodb.changestream.config.ChangeStreamProperties;
import com.mzinx.mongodb.changestream.listener.ChangeStreamListener;
import com.mzinx.mongodb.changestream.model.ChangeStream;
import com.mzinx.mongodb.changestream.model.ChangeStream.Mode;
import com.mzinx.mongodb.changestream.model.ChangeStreamRegistry;
import com.mzinx.mongodb.changestream.service.ChangeStreamService;

import jakarta.annotation.PreDestroy;

@Component
public class ChangeStreamManager {
    Logger logger = LoggerFactory.getLogger(getClass());
private final ApplicationContext context;
    private final ChangeStreamProperties changeStreamProperties;

    private final ChangeStreamService<Document> changeStreamService;


    ChangeStream<Document> cs;

    ChangeStreamManager(ApplicationContext context, ChangeStreamProperties changeStreamProperties, ChangeStreamService<Document> changeStreamService) {
        this.context = context;
        this.changeStreamProperties = changeStreamProperties;
        this.changeStreamService = changeStreamService;
    }

    @Scheduled
    private void watch() {
        //TODO: fetch change stream configs and initiate
        /*ChangeStreamListener<Document> changeStreamWatch = context.getBean("changeStreamWatch", ChangeStreamListener.class);

        cs = ChangeStream.of("change-stream", Mode.BOARDCAST,
                List.of(Aggregates.match(
                        Filters.in("operationType", List.of("insert", "update", "delete")))));
        this.changeStreamService.start(ChangeStreamRegistry.<Document>builder()
                .collectionName(changeStreamProperties.getChangeStreamCollection()).listener(changeStreamWatch).changeStream(cs).build());


        ChangeStreamListener<Document> instanceWatch = context.getBean("instanceWatch", ChangeStreamListener.class);
        changeStreamService.start(ChangeStreamRegistry.<Document>builder().collectionName("instance").listener(instanceWatch).changeStream(ChangeStream.of("discovery", Mode.BOARDCAST,
                List.of(Aggregates.match(
                        Filters.in("operationType", List.of("insert", "update", "delete")))))
                .fullDocumentBeforeChange(FullDocumentBeforeChange.REQUIRED)).build());


                ChangeStream.of("message-service", Mode.BOARDCAST,
				List.of());
		changeStreamService.start(
				ChangeStreamRegistry.<Document>builder().collectionName(messagingProperties.getCollection()).listener(messageListener).changeStream(this.cs).build());
                
                
                this.cs = ChangeStream.of("live-data", Mode.BOARDCAST,
					List.of(Aggregates
							.match(Filters.in("ns.coll",
									messagingProperties.getWatchCollections()))));
			this.changeStreamService.start(ChangeStreamRegistry.<Document>builder().listener(new LiveDataListener<>()).changeStream(this.cs).build());
		
                
                */
    }


    @PreDestroy
    private void clear() {
        if (cs != null)
            cs.setRunning(false);
    }

}
