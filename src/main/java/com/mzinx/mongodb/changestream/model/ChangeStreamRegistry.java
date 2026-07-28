package com.mzinx.mongodb.changestream.model;

import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.mzinx.mongodb.changestream.listener.ChangeStreamListener;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChangeStreamRegistry<T> {
	private String collectionName;
    private ChangeStream<T> changeStream;
    private ChangeStreamListener<T> listener;
    private CompletableFuture<Object> completableFuture;

    /**
     * The persisted config this registry was created from, set once the stream
     * has been started successfully. {@code null} for streams not driven by a
     * config (e.g. the coordination stream or programmatically started ones).
     */
    private ChangeStreamConfig config;
    
    private int instanceIndex;
    private int instanceSize;
    private String leader;
    private List<String> instances;

    private Date earliestChangeAt;
    
    public void stop(){
		this.changeStream.setRunning(false);
		if (this.completableFuture != null)
			this.completableFuture.join();
    }
    
}
