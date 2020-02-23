package com.batch.starter.config;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.batch.starter.config.dto.UserDetailsDTO;
import com.batch.starter.config.processor.UserDetailsProcessor;
import com.batch.starter.config.reader.DatabaseItemReader;
import com.batch.starter.config.writer.StringHeaderWriter;
 
@Configuration
@EnableBatchProcessing
public class BatchConfig {
    
	private static final String QUERY_ONE = "select user_id userID,userName,first_name firstName,"
			+ "last_name lastName,gender,password,status from user_details";

	private static final String QUERY_TWO = "select user_id userID,first_name firstName,"
			+ "last_name lastName from user_details_temp";
	
	@Bean
    ItemReader<UserDetailsDTO> databaseItemReaderOne(DataSource dataSource) {
        return new DatabaseItemReader(dataSource, QUERY_ONE);
    }

	@Bean
    ItemReader<UserDetailsDTO> databaseItemReaderTwo(DataSource dataSource) {
        return new DatabaseItemReader(dataSource, QUERY_TWO);
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
	ItemWriter<UserDetailsDTO> csvWriterTwo() {
	      FlatFileItemWriter<UserDetailsDTO> csvFileWriter = new FlatFileItemWriter<>();
	      
	        String exportFileHeader = "USERID;FIRSTNAME;LASTNAME;";
	        StringHeaderWriter headerWriter = new StringHeaderWriter(exportFileHeader);
	        csvFileWriter.setHeaderCallback(headerWriter);
	 
	        String exportFilePath = "/tmp/studentsTwo.csv";
	        csvFileWriter.setResource(new FileSystemResource(exportFilePath));
			DelimitedLineAggregator<UserDetailsDTO> lineAggregator1 = new DelimitedLineAggregator<>();
			lineAggregator1.setDelimiter(";");
			BeanWrapperFieldExtractor<UserDetailsDTO> extractor = new BeanWrapperFieldExtractor<>();
			extractor.setNames(new String[] {"userID","firstName","lastName"});
			
			FieldExtractor<UserDetailsDTO> fieldExtractor = extractor;
			lineAggregator1.setFieldExtractor(fieldExtractor);

	        LineAggregator<UserDetailsDTO> lineAggregator = lineAggregator1;
	        csvFileWriter.setLineAggregator(lineAggregator);
			return csvFileWriter;
	}
	
	@Bean ItemWriter<UserDetailsDTO> dbWriter(DataSource dataSource) {
		JdbcBatchItemWriterBuilder<UserDetailsDTO> dbWriter = new JdbcBatchItemWriterBuilder<>();
		ItemPreparedStatementSetter<UserDetailsDTO> itemPreparedStatementSetter = new ItemPreparedStatementSetter<UserDetailsDTO>() {
			@Override
			public void setValues(UserDetailsDTO item, PreparedStatement ps) throws SQLException {
				ps.setString(1, item.getFirstName());
				ps.setString(2, item.getLastName());
			}
		};
		return dbWriter.dataSource(dataSource).sql("INSERT INTO user_details_temp(first_name,last_name)"
				+ " values(?, ?) ").itemPreparedStatementSetter(itemPreparedStatementSetter).build();
	}
	
    @Bean
    public Step stepOne(StepBuilderFactory factory){
        return factory.get("stepOne")
                .<UserDetailsDTO,UserDetailsDTO> chunk(1000)
                .reader(databaseItemReaderOne(null))
                .processor(processor())
                .writer(dbWriter(null))
//                .taskExecutor(threadPoolTaskExecutor())
                .build();
    }

    @Bean
    public Step stepTwo(StepBuilderFactory factory){
        return factory.get("stepTwo")
                .<UserDetailsDTO,UserDetailsDTO> chunk(1000)
                .reader(databaseItemReaderTwo(null))
                .processor(processor())
                .writer(csvWriterTwo())
//                .taskExecutor(threadPoolTaskExecutor())
                .build();
    }
    
    @Bean
    public Step stepThree(StepBuilderFactory factory){
        return factory.get("stepThree")
                .<UserDetailsDTO,UserDetailsDTO> chunk(1000)
                .reader(databaseItemReaderOne(null))
                .processor(processor())
                .writer(csvWriterOne())
//                .taskExecutor(threadPoolTaskExecutor())
                .build();
    }
    
    @Bean
	public Job createUserDetailsJob(JobBuilderFactory jobBuilderFactory) {
		return jobBuilderFactory
		  .get("createUserDetailsJob")
		  .incrementer(new RunIdIncrementer())
		  .flow(stepOne(null))
		  .next(splitFlow())
		  .end()
		  .build();
	}

    @Bean
    public Flow splitFlow() {
        return new FlowBuilder<SimpleFlow>("splitFlow")
            .split(threadPoolTaskExecutor())
            .add(flowTwo(), flowThree())
            .build();
    }

    
    @Bean
    public Flow flowTwo() {
        return new FlowBuilder<SimpleFlow>("flowTwo")
            .start(stepTwo(null))
            .build();
    }
    
    @Bean
    public Flow flowThree() {
        return new FlowBuilder<SimpleFlow>("flowThree")
            .start(stepThree(null))
            .build();
    }
    
    @Bean
    public ThreadPoolTaskExecutor threadPoolTaskExecutor() {
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setCorePoolSize(4);
		taskExecutor.setMaxPoolSize(4);
		taskExecutor.afterPropertiesSet();
		return taskExecutor;
	}
    
    @Bean
    public SimpleAsyncTaskExecutor simpleAsyncTaskExecutor() {
    	SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
    	taskExecutor.setConcurrencyLimit(4);
		return taskExecutor;
	}
    
}