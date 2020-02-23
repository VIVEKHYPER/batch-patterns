package com.batch.starter.config;
import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import com.batch.starter.config.dto.UserDetailsDTO;
import com.batch.starter.config.processor.UserDetailsProcessor;
import com.batch.starter.config.reader.DatabaseItemReader;
import com.batch.starter.config.writer.StringHeaderWriter;

@Configuration
@EnableBatchProcessing
public class SingleThreadChunkConfig {

	private static final String QUERY_ONE = "select user_id userID,userName,first_name firstName,"
			+ "last_name lastName,gender,password,status from user_details";

	@Bean
	ItemReader<UserDetailsDTO> databaseItemReaderOne(DataSource dataSource) {
		return new DatabaseItemReader(dataSource, QUERY_ONE);
	}

	@Bean
	ItemProcessor<UserDetailsDTO, UserDetailsDTO> processor() {
		return new UserDetailsProcessor();
	}

	@Bean
	ItemWriter<UserDetailsDTO> csvWriterOne() {
		FlatFileItemWriter<UserDetailsDTO> csvFileWriter = new FlatFileItemWriter<>();

		String exportFileHeader = "USERID;USERNAME;FIRSTNAME;LASTNAME;GENDER;PASSWORD;STATUS";
		StringHeaderWriter headerWriter = new StringHeaderWriter(exportFileHeader);
		csvFileWriter.setHeaderCallback(headerWriter);

		String exportFilePath = "/tmp/studentsOne.csv";
		csvFileWriter.setResource(new FileSystemResource(exportFilePath));
		DelimitedLineAggregator<UserDetailsDTO> lineAggregator1 = new DelimitedLineAggregator<>();
		lineAggregator1.setDelimiter(";");
		BeanWrapperFieldExtractor<UserDetailsDTO> extractor = new BeanWrapperFieldExtractor<>();
		extractor.setNames(new String[] {"userID","userName","firstName","lastName","gender","password","status"});

		FieldExtractor<UserDetailsDTO> fieldExtractor = extractor;
		lineAggregator1.setFieldExtractor(fieldExtractor);

		LineAggregator<UserDetailsDTO> lineAggregator = lineAggregator1;
		csvFileWriter.setLineAggregator(lineAggregator);
		return csvFileWriter;
	}

	@Bean
	public Step stepOne(StepBuilderFactory factory){
		return factory.get("stepOne")
				.<UserDetailsDTO,UserDetailsDTO> chunk(1000)
				.reader(databaseItemReaderOne(null))
				.processor(processor())
				.writer(csvWriterOne())
				.build();
	}

	@Bean
	public Job createUserDetailsSTCJob(JobBuilderFactory jobBuilderFactory) {
		return jobBuilderFactory
				.get("createUserDetailsSTCJob")
				.incrementer(new RunIdIncrementer())
				.flow(stepOne(null))
				.end()
				.build();
	}

}