package org.opencloudb.redis;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class RedisQueue {

	private Map waitMap;
	private String table;
	private String key;
	private Map reqMap;
	//0-首次、1-正在获取  2-正在写入，3-写入完成
	public  volatile  long exe;  
	
	public RedisQueue(){
		waitMap=new HashMap();
		exe=0;
	}
	
	public  void addWait(HashMap w,String _table,String _key){
		table=_table;
		key=_key;
		synchronized(this.waitMap){
		waitMap.putAll(w);
		}
	}
	
	public synchronized Map pushWait(){
		synchronized(this.waitMap){
			Map tmp=waitMap;
			//waitMap=new HashMap();
			return tmp;
		}
	}
	
	public synchronized Map getReqMap(){
		synchronized(this.reqMap){ 
			return reqMap;
		}
	}
	
	public synchronized void setReqMap(Map m){
		synchronized(this.reqMap){ 
			  reqMap=m;
		}
	}
	
	public long getExe(){
		return exe;
	}
	
	
	public long putExe( ){
		long i=exe;
		long ii= exe++;
		return ii-i;
	}
}
