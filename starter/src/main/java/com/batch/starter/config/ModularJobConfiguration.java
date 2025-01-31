package com.batch.starter.config;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.support.ApplicationContextFactory;
import org.springframework.batch.core.configuration.support.GenericApplicationContextFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing(modular = true)
public class ModularJobConfiguration {

	@Bean
	public ApplicationContextFactory singleThreadChunkJob() {
		return new GenericApplicationContextFactory(SingleThreadChunkConfig.class);
	}

	@Bean
	public ApplicationContextFactory multiThreadChunkJob() {
		return new GenericApplicationContextFactory(MultiThreadChunkConfig.class);
	}

	@Bean
	public ApplicationContextFactory sequentialStepsJob() {
		return new GenericApplicationContextFactory(SequentialStepsConfig.class);
	}

	@Bean
	public ApplicationContextFactory parallelStepsJob() {
		return new GenericApplicationContextFactory(ParallelStepsConfig.class);
	}
}