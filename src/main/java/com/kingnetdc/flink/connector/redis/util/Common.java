package com.kingnetdc.flink.connector.redis.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class Common {

	private static final Logger log = LoggerFactory.getLogger(Common.class);

	public static void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (Exception e) {
			log.warn("sleep error", e);
		}
	}

	/**
	 * map convert to k1=v1,k2=v2,k3=v3 string
	 * @param map map
	 * @return
	 */
	public static String mapToString(Map<String, String> map) {
		StringBuilder sb = new StringBuilder();
		map.forEach((key, value) -> {
			sb.append(key).append("=").append(value).append(",");
		});
		sb.delete(sb.length() - 1, sb.length());
		return sb.toString();
	}

}
