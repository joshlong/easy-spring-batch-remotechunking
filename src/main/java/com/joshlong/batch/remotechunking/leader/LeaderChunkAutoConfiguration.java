package com.joshlong.batch.remotechunking.leader;

import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.integration.chunk.ChunkMessageChannelItemWriter;
import org.springframework.batch.integration.chunk.RemoteChunkHandlerFactoryBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.dsl.DirectChannelSpec;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.dsl.QueueChannelSpec;

import java.util.Set;

@AutoConfiguration
@ConditionalOnProperty(value = "bootiful.batch.chunk.leader", havingValue = "true")
@ImportRuntimeHints(LeaderChunkAutoConfiguration.Hints.class)
class LeaderChunkAutoConfiguration {

	static class Hints implements RuntimeHintsRegistrar {

		@Override
		public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
			Set.of(ChunkMessageChannelItemWriter.class)
				.forEach(c -> hints.reflection().registerType(c, MemberCategory.values()));
		}

	}

	private final static String MESSAGING_TEMPLATE_BEAN_NAME = "leaderChunkMessagingTemplate";

	@Bean
	@LeaderOutboundChunkChannel
	DirectChannelSpec leaderRequestsMessageChannel() {
		return MessageChannels.direct();
	}

	@Bean
	@LeaderInboundChunkChannel
	QueueChannelSpec leaderRepliesMessageChannel() {
		return MessageChannels.queue();
	}

	@Bean
	@ConditionalOnMissingBean
	@Qualifier(MESSAGING_TEMPLATE_BEAN_NAME)
	MessagingTemplate leaderChunkMessagingTemplate(DirectChannel leaderRequestsMessageChannel) {
		var template = new MessagingTemplate();
		template.setDefaultChannel(leaderRequestsMessageChannel);
		template.setReceiveTimeout(2000);
		return template;
	}

	@Bean
	@ConditionalOnMissingBean
	RemoteChunkHandlerFactoryBean<Object> leaderChunkHandler(
			ChunkMessageChannelItemWriter<Object> chunkMessageChannelItemWriterProxy,
			@LeaderChunkStep TaskletStep step) {
		var remoteChunkHandlerFactoryBean = new RemoteChunkHandlerFactoryBean<>();
		remoteChunkHandlerFactoryBean.setChunkWriter(chunkMessageChannelItemWriterProxy);
		remoteChunkHandlerFactoryBean.setStep(step);
		return remoteChunkHandlerFactoryBean;
	}

	@Bean
	@LeaderItemWriter
	// @StepScope
	ChunkMessageChannelItemWriter<?> leaderChunkMessageChannelItemWriter(QueueChannel leaderRepliesMessageChannel,
			@Qualifier(MESSAGING_TEMPLATE_BEAN_NAME) MessagingTemplate template) {
		var chunkMessageChannelItemWriter = new ChunkMessageChannelItemWriter<>();
		chunkMessageChannelItemWriter.setMessagingOperations(template);
		chunkMessageChannelItemWriter.setReplyChannel(leaderRepliesMessageChannel);
		return chunkMessageChannelItemWriter;
	}

}
