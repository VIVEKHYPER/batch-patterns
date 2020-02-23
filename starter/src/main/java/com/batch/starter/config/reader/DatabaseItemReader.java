package com.batch.starter.config.reader;

import javax.sql.DataSource;

import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import com.batch.starter.config.dto.UserDetailsDTO;

public class DatabaseItemReader implements ItemReader<UserDetailsDTO> {

	JdbcCursorItemReader<UserDetailsDTO> databaseReader = new JdbcCursorItemReader<>();


	public DatabaseItemReader(DataSource dataSource,String query) {
		databaseReader.setDataSource(dataSource);
		databaseReader.setSql(query);
		databaseReader.setRowMapper(new BeanPropertyRowMapper<>(UserDetailsDTO.class));
	}

	@BeforeStep
	public void initialize() {
		databaseReader.open(new ExecutionContext());
	}

	@Override
	public synchronized UserDetailsDTO read()
			throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		return databaseReader.read();
	}

	@AfterStep
	public void readFinish() {
		databaseReader.close();
	}

}
