package org.opencloudb.redis;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.opencloudb.PrintTrace;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.mpp.ColMeta;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.stat.QueryResult;
import org.opencloudb.stat.QueryResultDispatcher;

public class RedisConnectHandler {

	private RedisParser redisParser;
	private JedisHandler jedisHandler;
	public RedisConnectHandler(RedisParser redisParser) {
		this.redisParser = redisParser;
		this.redisParser.setHandler(this);
		jedisHandler=new JedisHandler();
		jedisHandler.setUp();
	}

	byte[] header = { 1, 0, 0, 1, 2 };
	byte[] eof = { 5, 0, 0, 4, -2, 0, 0, 34, 0 };
	byte[] fieldBegin = { 21, 0, 0, 2, 3, 100, 101, 102, 0, 0, 0 };
	byte[] fieldEnd = { 0, 12, 63, 0, 33, 0, 0, 0, -10, -128, 0, 5, 0, 0 };

	byte[] valueBegin = { 13, 0, 0, 89 };
	byte[] roweof = { 5, 0, 0, 5, -2, 0, 0, 34, 0 };
	byte[] ok={48,0,0,2,0,1,0,2,0,0,0,40,82,111,119,115,32,109,97,116,99,104,101,100,58,32,49,32,32,67,104,97,110,103,101,100,58,32,48,32,32,87,97,114,110,105,110,103,115,58,32,48};

	public byte[] getHeader(int size) {

		byte[] _header = Arrays.copyOf(header, header.length);
		_header[4] = (byte) size;
		return _header;
	}

	public byte[] getField(String name) {
		byte[] ns = name.getBytes();
		byte[] r = new byte[fieldBegin.length + ns.length + fieldEnd.length + 1];
		System.arraycopy(fieldBegin, 0, r, 0, fieldBegin.length);
		r[0] = (byte) (ns.length + 22);
		System.arraycopy(ns, 0, r, fieldBegin.length + 1, ns.length);
		r[fieldBegin.length] = (byte) (ns.length);
		System.arraycopy(fieldEnd, 0, r, ns.length + fieldBegin.length + 1,
				fieldEnd.length);

		return r;
	}

	public List<byte[]> getFields(List<String> fs) {
		List<byte[]> fields = new ArrayList<>();
		for (String n : fs) {
			fields.add(getField(n));
		}
		return fields;
	}

	public void getPack(RowValue rv, Object obj) {

		byte[] vs = obj.toString().getBytes();
		byte[] vss = new byte[vs.length + 1];
		vss[0] = (byte) vs.length;
		System.arraycopy(vs, 0, vss, 1, vs.length);
		if (rv.getValue() == null) {

			rv.setSize(vs.length + 1);
			rv.setValue(vss);
			return;
		}
		byte[] rvs = rv.getValue();
		byte[] vsss = new byte[rvs.length + vss.length];
		System.arraycopy(rvs, 0, vsss, 0, rvs.length);

		System.arraycopy(vss, 0, vsss, rvs.length, vss.length);
		rv.setSize(vs.length + 1 + rvs.length);
		rv.setValue(vsss);
	}

	public byte[] getValues(List<Object> values) {
		RowValue rv = new RowValue();
		for (Object obj : values) {
			getPack(rv, obj);
		}
		byte[] b = rv.getValue();
		byte[] ret = new byte[valueBegin.length + b.length];
		System.arraycopy(valueBegin, 0, ret, 0, valueBegin.length);

		System.arraycopy(b, 0, ret, valueBegin.length, b.length);
		ret[0] = (byte) rv.getSize();
		return ret;
	}

	private HashMap getRedisRow(){
		return jedisHandler.select(redisParser.getTableName(), redisParser.getKeyValue());
	}
	public boolean select() {
		HashMap hs =getRedisRow();
		if(hs==null){
			return false;
		}
		Iterator it = hs.entrySet().iterator();
		if (redisParser.getFields().size() == 0) {
			while (it.hasNext()) {
				Entry entry = (Entry) it.next();
				System.out
						.println("hry_ddddddentry.getKey() " + entry.getKey());
				redisParser.getFields().add((String) entry.getKey());
				redisParser.getValues().add(entry.getValue());
			}
		} else {
			for (String v : redisParser.getFields()) {
				redisParser.getValues().add(hs.get(v));
			}
		}
		System.out.println("hry_ddddd " + redisParser.getFields().size());
		List<byte[]> _fields = getFields(redisParser.getFields());
		byte[] _row = getValues(redisParser.getValues());
		byte[] _header = getHeader(redisParser.getFields().size());
	 
		 
	 
		fieldEofResponse(_header, _fields, eof);
		rowResponse(_row);
		rowEofResponse(roweof);
		return true;
	}
	
	
	public boolean updateToRedis(){
		boolean f= 	jedisHandler.updateToRedis(redisParser.getTableName(),redisParser.getKeyValue() ,redisParser.getRow());
		if(f==false){
			return f;
		}
		DBQueueMng.getInstance().addRow(redisParser.getTableName(),redisParser.getKeyValue().toString(), redisParser);
		
		okResponse(ok);
		return true;
	}
	
	byte packetId=0;
	public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof) {
		ServerConnection source = null;
		try {

			header[3] = ++packetId;
			source = redisParser.getConn().getSession2().getSource();
			ByteBuffer buffer = source.allocate();
			buffer = source.writeToBuffer(header, buffer);
			int fieldCount = fields.size();

			String primaryKey = null;

			Map<String, ColMeta> columToIndx = new HashMap<String, ColMeta>(
					fieldCount);

			for (int i = 0, len = fieldCount; i < len; ++i) {
				byte[] field = fields.get(i);
				// PrintTrace.prints(field);

				field[3] = ++packetId;
				buffer = source.writeToBuffer(field, buffer);
			}
			eof[3] = ++packetId;
			buffer = source.writeToBuffer(eof, buffer);
			source.write(buffer);

		} catch (Exception e) {
			System.out.println("hry_serverConnetion_send" + e);
		}
	}

	public void rowResponse(byte[] row) {

		row[3] = ++packetId;
		// this.writeToBuffer(row, this.allocate());
		 redisParser.getConn().write( redisParser.getConn().writeToBuffer(row,  redisParser.getConn().allocate()));
	}

	public void rowEofResponse(byte[] eof) {

		// byte[] eof = MultiNodeQueryHandler.roweof;

		eof[3] = ++packetId;

		redisParser.getConn().write(redisParser.getConn().writeToBuffer(eof, redisParser.getConn().allocate()));
	}
	
	public void okResponse(byte[] data) {        
			ServerConnection source = null;
			source = redisParser.getConn().getSession2().getSource();
			OkPacket ok = new OkPacket();
			PrintTrace.prints(data);
			ok.read(data);
			ok.packetId = ++packetId;// OK_PACKET
			ok.serverStatus = source.isAutocommit() ? 2 : 1;
			ok.write(source);
		}
	 
	

	class RowValue {
		private int size = 0;
		private byte[] value;

		public int getSize() {
			return size;
		}

		public void setSize(int size) {
			this.size = size;
		}

		public byte[] getValue() {
			return value;
		}

		public void setValue(byte[] value) {
			this.value = value;
		}

	}

	private void clearRow() {
		jedisHandler.deleteAll(redisParser.getTableName());
	}
	
	private void deleteRow() {
		jedisHandler.delete(redisParser.getTableName(),redisParser.getKeyValue());
	}


	private void updateRow() {
		jedisHandler.update(redisParser.getTableName(),redisParser.getKeyValue() ,redisParser.getRow());
	}

	 

	public void ddlFlushToRedis() {
		if (!redisParser.isRedis()) {
			return;
		}
		if (redisParser.isDel()) {
			if (redisParser.getAll() == 1) {
				clearRow();
			} else {
				deleteRow();
			}
			
		}
		
		else if (redisParser.isUpd()) {
			if (redisParser.getAll() == 1) {
				clearRow();
			} else {
				updateRow();
			}
		}

	}

	private boolean chkSelToRedis() {

		return redisParser.chkBackNeedRedis() > 0;
	}

	private void dqlFlushToRedis0() {
		jedisHandler.selectTo(redisParser.getTableName(),redisParser.getKeyValue() ,redisParser.getRow());
	}

	public void dqlFlushToRedis() {
		if (chkSelToRedis()) {
			dqlFlushToRedis0();
		}
	}
}
