package org.opencloudb.redis;

public class RedisUtil {
	public static String genKey(String table,String key){
		return table+".."+key;
	}
}
