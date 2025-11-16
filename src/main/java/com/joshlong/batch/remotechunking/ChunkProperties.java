package com.joshlong.batch.remotechunking;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "bootiful")
public record ChunkProperties(Batch batch) {
	record Batch(Chunk chunk) {
		record Chunk(boolean leader, boolean worker) {
		}
	}
}
