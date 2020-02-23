package com.batch.starter;

import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class StarterApplication implements CommandLineRunner{

	@Autowired
	private JobRegistry jobRegistry;

	@Autowired
	private JobLauncher jobLauncher;

	public static void main(String[] args) {
		SpringApplication.run(StarterApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		//		jobLauncher.run(jobRegistry.getJob("createUserDetailsSTCJob"), new JobParametersBuilder()
		//				.addString("JobID", String.valueOf(System.currentTimeMillis())).toJobParameters());
		//		jobLauncher.run(jobRegistry.getJob("createUserDetailsMTCJob"), new JobParametersBuilder()
		//				.addString("JobID", String.valueOf(System.currentTimeMillis())).toJobParameters());
		//		jobLauncher.run(jobRegistry.getJob("createUserDetailsPSJob"), new JobParametersBuilder()
		//				.addString("JobID", String.valueOf(System.currentTimeMillis())).toJobParameters());
		jobLauncher.run(jobRegistry.getJob("createUserDetailsSSJob"), new JobParametersBuilder()
				.addString("JobID", String.valueOf(System.currentTimeMillis())).toJobParameters());
	}

}
