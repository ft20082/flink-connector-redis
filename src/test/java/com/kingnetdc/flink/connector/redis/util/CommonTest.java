package com.kingnetdc.flink.connector.redis.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class CommonTest {

	@Test
	public void testMapToString() {
		Map<String, String> map = new HashMap<>();
		map.put("a", "b");
		map.put("c", "d");
		map.put("e", "f");
		Assert.assertEquals("a=b,c=d,e=f", Common.mapToString(map));
	}

}
