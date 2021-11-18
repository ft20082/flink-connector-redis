package org.apache.flink.connector.redis.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

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

	public static Map<String, String> stringToMap(String string) {
		Map<String, String> ret = new HashMap<>();
		if (string != null && string.length() > 0) {
			ret = Arrays.stream(string.split(",")).filter(item -> item.indexOf("=") > 0)
					.map(item -> item.split("=")).collect(Collectors.toMap(item -> item[0], item -> item[1]));
		}
		return ret;
	}

	public static void closeJedis(Jedis jedis) {
		try {
			if (jedis != null) {
				jedis.close();
			}
		} catch (Exception e) {
			log.info("close jedis error", e);
		}
	}
}
