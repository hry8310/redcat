package org.opencloudb.redis;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisQueueMng {

	private Map<String,RedisQueue> queueMap;
	static private RedisQueueMng redisQueue=new RedisQueueMng(); 
	 
	public RedisQueueMng(){
		queueMap =new ConcurrentHashMap<String,RedisQueue>();
	}
	static public RedisQueueMng getInstance(){
		return redisQueue;
	}
	/*
	 * 0-首次
	 * */
	public void removeRow(String table,String key ,long exe4){
		synchronized(this.queueMap){
			queueMap.remove(RedisUtil.genKey(table,key));
		}
	}
	
	/*
	 * 0-首次
	 * */
	public RedisQueue addRow(String table,String key,HashMap m){
		RedisQueue q=null;
		
		synchronized(this.queueMap){
			q=queueMap.get(RedisUtil.genKey(table,key));
			if(q==null){
				q=new RedisQueue();
				queueMap.put(RedisUtil.genKey(table,key), q);
			}
			q.addWait(m,table,key);
		}
		
		
		return q;
	}
	
	
}
