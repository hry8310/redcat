package org.opencloudb.redis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.opencloudb.MycatServer;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.config.model.DBHostConfig;
import org.opencloudb.config.model.DataHostConfig;
import org.opencloudb.route.RouteResultsetNode;

public class DBQueue {

	private Map waitMap;
	private String table;
	private String db;
	private String key;
	protected RedisParser redisParser;
	MycatServer server=MycatServer.getInstance();
	protected int port;
	//0-首次、1-正在获取  2-正在写入，3-写入完成
	public  volatile  long exe;  
	 
	
	public DBQueue(){
		try{
			Class.forName("com.mysql.jdbc.Driver");//
		}catch(Exception e){
		
		}
		waitMap=new HashMap();
		exe=0;
	}
	
	public  void addWait(String _table,String _key,RedisParser w){
		table=_table;
		key=_key;
		redisParser=w;
		
		
		synchronized(this.waitMap){
		 	waitMap.putAll(w.getRowObj());
		}
		
	}
	
	public synchronized Map pushWait(){
		synchronized(this.waitMap){
			Map tmp=waitMap;
			//waitMap=new HashMap();
			return tmp;
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
	
	public DBHostConfig getDbCfg(){
		
		Map<String, PhysicalDBPool>  dataHosts=server.getConfig().getDataHosts();
		Map<String, PhysicalDBNode>  nodes=server.getConfig().getDataNodes();
		RouteResultsetNode[] ns=redisParser.getRrs().getNodes();
		
		db=nodes.get(ns[0].getName()).getDatabase();
;		String name=nodes.get(ns[0].getName()).getDbPool().getHostName();
		PhysicalDBPool dbpool=dataHosts.get(name);
		DBHostConfig cfg =dbpool.getSource().getConfig();
		System.out.println("db  >>>  "+db);
		return cfg;
	}
	
	private String genSql(){
		String sql="";
		String set="";
		Iterator<String> it=waitMap.keySet().iterator();
		while(it.hasNext()){
			String key=it.next();
			String o=(String)waitMap.get(key);
			set = " "+key+" = "+o+" ,";
		}
		if(!set.equals("")){
			set=set.substring(0,set.length()-1);
		}
		sql="update "+table+" set "+set+ "where "+redisParser.getKeyField()+" = "+redisParser.getKeyValue().toString();
		return sql;
	}
	
	public void exeSql(){
		String sql=genSql();
		DBHostConfig cfg =getDbCfg();
		try{
			System.out.println("db  >>>  "+sql);
			Class.forName("com.mysql.jdbc.Driver");
			Connection connection = DriverManager.getConnection("jdbc:mysql://"+cfg.getUrl()+"/"+db, cfg.getUser(), cfg.getPassword());
			connection.createStatement().execute(sql);
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
}
