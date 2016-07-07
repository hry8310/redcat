package org.opencloudb.redis;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


public class DBQueueMng {

	private Map<String,DBQueue> queueMap;
	static private DBQueueMng dbQueue=new DBQueueMng(); 
	
	private   ThreadPoolExecutor poolExecutor=null;
	private final static int size = 5;
	
	 
    private final ReentrantLock takeLock = new ReentrantLock();
    private final Condition notEmpty = takeLock.newCondition();
    private final ReentrantLock putLock = new ReentrantLock();
    private final Condition notFull = putLock.newCondition();
	 
	public DBQueueMng(){
		queueMap =new  HashMap<String,DBQueue>();
		try{
		poolExecutor = new ThreadPoolExecutor(size, size * 4, 60L,
				TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
		 
		new Thread(){
			public void run(){
				UpdateTask task=new UpdateTask();
				System.out.println("i-dd-poolExecutor "+poolExecutor);
				poolExecutor.submit(task);
			}
		}.start();	
		}catch(Exception e){
			System.out.println("e");
			e.printStackTrace();
		}
	}
	static public DBQueueMng getInstance(){
		DBQueueMng tmp= dbQueue;
		return tmp;
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
	public   DBQueue addRow(String table,String key,RedisParser red){
		takeLock.lock();
		DBQueue q=null;
 		try{
		
			q=queueMap.get(RedisUtil.genKey(table,key));
			System.out.println("add。。。"+q);
			if(q==null){
				q=new DBQueue();
				queueMap.put(RedisUtil.genKey(table,key), q);	
			}
			if(!queueMap.isEmpty()){
				System.out.println("queueMap-empt");
			}
			q.addWait(table,key,red);
			notEmpty.signal();
 		}finally{
 			takeLock.unlock();
 		}
			return q;
	}
	
	/*
	 * 0-首次
	 * */
	public  Map<String,DBQueue> getAllRow(){
		Map<String,DBQueue> tmp=new HashMap<String,DBQueue>();
		takeLock.lock();
		try{
			if( this.queueMap.isEmpty()){
				notEmpty.wait();
			}
			tmp.putAll(this.queueMap);
			this.queueMap =new  HashMap<String,DBQueue>();
			
		}catch(Exception e){
			
		}finally{
			takeLock.unlock();
		}
			return tmp;
		 
	}
	
	  static public  class UpdateTask  implements Runnable{
		  DBQueueMng mng= DBQueueMng.getInstance();
		  public void run(){
			  while(true){
				  try{
					  Map<String,DBQueue> map= mng.getAllRow();
					  
					  Iterator<DBQueue> it = map.values().iterator();
					  while(it.hasNext()){
						  DBQueue db=it.next();
						  db.exeSql();
						
					  }
				  }catch(Exception e){
					  e.printStackTrace();
				  }
			  }
		  }
	}
	
	
	
	
	
}
