package com.joshlong.batch.remotechunking.worker;

import org.springframework.batch.core.step.item.SimpleChunkProcessor;
import org.springframework.batch.integration.chunk.ChunkProcessorChunkHandler;
import org.springframework.batch.integration.chunk.ChunkRequest;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.DirectChannelSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

@AutoConfiguration
@ConditionalOnProperty(value = "bootiful.batch.chunk.worker", havingValue = "true")
class WorkerChunkAutoConfiguration {

	@Bean
	@WorkerInboundChunkChannel
	DirectChannelSpec workerRequestsMessageChannel() {
		return MessageChannels.direct();
	}

	@Bean
	@WorkerOutboundChunkChannel
	DirectChannelSpec workerRepliesMessageChannel() {
		return MessageChannels.direct();
	}

	@Bean
	@ConditionalOnMissingBean
	@SuppressWarnings("unchecked")
	ChunkProcessorChunkHandler<?> workerChunkProcessorChunkHandler(
			// todo make this optional
			// @WorkerChunkingItemProcessor ObjectProvider<ItemProcessor<Object, Object>>
			// processor,
			@WorkerItemProcessor ItemProcessor<?, ?> processor, @WorkerItemWriter ItemWriter<?> writer) {
		var chunkProcessorChunkHandler = new ChunkProcessorChunkHandler<>();
		chunkProcessorChunkHandler.setChunkProcessor(new SimpleChunkProcessor(processor, writer));
		return chunkProcessorChunkHandler;
	}

	@Bean
	@SuppressWarnings("unchecked")
	IntegrationFlow chunkProcessorChunkHandlerIntegrationFlow(
			@WorkerInboundChunkChannel DirectChannel workerRequestsMessageChannel,
			@WorkerOutboundChunkChannel DirectChannel workerRepliesMessageChannel,
			ChunkProcessorChunkHandler<Object> chunkProcessorChunkHandler) {
		return IntegrationFlow//
			.from(workerRequestsMessageChannel)//
			.handle(message -> {
				try {
					var payload = message.getPayload();
					if (payload instanceof ChunkRequest<?> cr) {
						var chunkResponse = chunkProcessorChunkHandler.handleChunk((ChunkRequest<Object>) cr);
						workerRepliesMessageChannel.send(MessageBuilder.withPayload(chunkResponse).build());
					}
					Assert.state(payload instanceof ChunkRequest<?>,
							"the payload must be an instance of ChunkRequest!");
				} //
				catch (Exception e) {
					throw new RuntimeException(e);
				}
			})//
			.get();
	}

}
