package com.batch.starter.config.processor;

import org.springframework.batch.item.ItemProcessor;

import com.batch.starter.config.dto.UserDetailsDTO;

public class UserDetailsProcessor  implements ItemProcessor<UserDetailsDTO, UserDetailsDTO>{

	@Override
	public UserDetailsDTO process(UserDetailsDTO item) throws Exception {
		Thread.sleep(2);
		return item;
	}

}
