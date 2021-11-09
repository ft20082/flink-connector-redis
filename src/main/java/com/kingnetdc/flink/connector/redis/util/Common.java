package com.kingnetdc.flink.connector.redis.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Common {

	private static final Logger log = LoggerFactory.getLogger(Common.class);

	public static void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (Exception e) {
			log.warn("sleep error", e);
		}
	}

}
