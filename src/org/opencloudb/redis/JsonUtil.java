package org.opencloudb.redis;

import java.io.IOException;
import java.util.HashMap;

import org.codehaus.jackson.map.ObjectMapper;

public class JsonUtil {
	/**
	 * 将json转为对象
	 * 
	 * @param str
	 * @param clazz
	 * @return
	 */
	public static HashMap fromJSONStr(String str ) {
		ObjectMapper json = new ObjectMapper();
		HashMap obj = null;
		try {
			obj = json.readValue(str, HashMap.class);

		} catch (IOException e) {
		 
		}
		return obj;
	}
	
	/**
	 * Map转为Json字符串
	 * @param body
	 * @return
	 */
	public static String toJSONStr(HashMap<String ,String>  body) {
		String data = "{}";
		ObjectMapper json = new ObjectMapper();
		try {
			data =  json.writeValueAsString(body);
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return data;
	}


}
