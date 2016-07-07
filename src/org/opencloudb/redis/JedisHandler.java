package org.opencloudb.redis;

import java.util.HashMap;

import redis.clients.jedis.Jedis;  
import redis.clients.jedis.JedisPool;  
import redis.clients.jedis.JedisPoolConfig;  
public class JedisHandler {
	 JedisPool pool;  
	 Jedis jedis;  
	 public void setUp() {  
	 
	   jedis =new Jedis("127.0.0.1");  
	}
	 
	public HashMap select(String table,Object key){
		
		String str=    jedis.hget(table, key.toString());
		if(str==null||str.trim().isEmpty()){
			return null;
		}
		return JsonUtil.fromJSONStr(str);
	}
	
	public void deleteAll(String table){
		jedis.hdel(table);
	}
	
	
	public void delete(String table,Object key){
		
		jedis.hdel(table,key.toString());
	}
	
	public void update(String table,Object key,HashMap row){
		HashMap h=select(table,key);
		if(h==null){
			return ;
		}
		RedisQueueMng mng=RedisQueueMng.getInstance();
		RedisQueue q=mng.addRow(table,key.toString(),h );
		long exe=q.getExe();
		q.addWait(row,table,key.toString());
		long exe2=q.getExe();
		if(exe2-exe<=0){
			
			HashMap m=(HashMap)q.pushWait();
			long a=q.putExe();
			if(a>1){
				return;
			}
			long exe3=q.getExe();
			String str=JsonUtil.toJSONStr(m);
			jedis.hset(table, key.toString(),str);
			long exe4=q.getExe();
			if(exe3-exe4==0){
				mng.removeRow(table,key.toString(),exe4);
			}
		}
		
	}
	
	public boolean updateToRedis( String table,Object key,HashMap row){
		HashMap h=select(table,key);
		if(h==null){
			return false;
		}
		RedisQueueMng mng=RedisQueueMng.getInstance();
		RedisQueue q=mng.addRow(table,key.toString(),h );
		long exe=q.getExe();
		q.addWait(row,table,key.toString());
		long exe2=q.getExe();
		if(exe2-exe<=0){
			
			HashMap m=(HashMap)q.pushWait();
			long a=q.putExe();
			if(a>1){
				return false;
			}
			long exe3=q.getExe();
			String str=JsonUtil.toJSONStr(m);
			jedis.hset(table, key.toString(),str);
			long exe4=q.getExe();
			if(exe3-exe4==0){
				mng.removeRow(table,key.toString(),exe4);
			}
		}
		return true;
	}
	
	
	public void selectTo(String table,Object key,HashMap row){
		HashMap h=select(table,key);
		if(h==null){
			h=row;
		}else{
			h.putAll(row);
		}
		String str=JsonUtil.toJSONStr(h);
		jedis.hset(table, key.toString(),str);
	}
	
	
	
	 
	
}
