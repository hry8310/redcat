package org.opencloudb.redis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.parser.ServerParse;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat.Column;
import com.alibaba.druid.stat.TableStat.Condition;

public class RedisParser {

	private int need = 0;
	private int all = 0;
	private int sqlType = -10;
	private String keyField;
	private String sql;
	private MySqlSchemaStatVisitor visitor;
	private List<String> fields = new ArrayList<String>();
	private List<String> dbFields = new ArrayList<String>();
	private List<Object> values = new ArrayList<Object>();
	private String tableName;
	private Object keyValue;
	private SchemaConfig schema;
	private ServerConnection conn;
	private int selectFiledX=0;
	
	private RouteResultset rrs;

	private RedisConnectHandler handler;

	public RedisParser(SchemaConfig s) {
		schema = s;
		handler = new RedisConnectHandler(this);
	}

	public int getNeed() {
		return need;
	}

	public void setNeed(int need) {
		this.need = need;
	}

	public int getAll() {
		return all;
	}

	public void setAll(int all) {
		this.all = all;
	}

	public int getSqlType() {
		return sqlType;
	}

	public void setSqlType(int sqlType) {
		this.sqlType = sqlType;
	}

	public String getKeyField() {
		return keyField;
	}

	public void setKeyField(String keyField) {
		this.keyField = keyField;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public MySqlSchemaStatVisitor getVisitor() {
		return visitor;
	}

	public void setVisitor(MySqlSchemaStatVisitor visitor) {
		this.visitor = visitor;
	}

	public List<String> getFields() {
		return fields;
	}

	public void setFields(List<String> fields) {
		this.fields = fields;
	}

	public List<String> getDbFields() {
		return dbFields;
	}

	public void setDbFields(List<String> dbFields) {
		this.dbFields = dbFields;
	}

	public List<Object> getValues() {
		return values;
	}

	public void setValues(List<Object> values) {
		this.values = values;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Object getKeyValue() {
		return keyValue;
	}

	public void setKeyValue(Object keyValue) {
		this.keyValue = keyValue;
	}

	public SchemaConfig getSchema() {
		return schema;
	}

	public void setSchema(SchemaConfig schema) {
		this.schema = schema;
	}

	public ServerConnection getConn() {
		return conn;
	}

	public void setConn(ServerConnection conn) {
		this.conn = conn;
	}

	public boolean isRedis() {
		return need != -1;
	}

	public RedisConnectHandler getHandler() {
		return handler;
	}

	public void setHandler(RedisConnectHandler handler) {
		this.handler = handler;
	}
	
	public boolean getX(){
		return selectFiledX==1;
	}

	public void parse(String stmt) {
		this.sql = stmt;
		MySqlStatementParser parser = new MySqlStatementParser(stmt);
		SQLStatement statement;
		try {
			statement = parser.parseStatement();

		} catch (Throwable t) {
			System.out.println("hry_redis_parse" + t.getMessage());
			need = -1;
			return;
		}
		visitor = new MySqlSchemaStatVisitor();
		statement.accept(visitor);
		if (visitor.getTables().size() != 1) {
			tableName = "";
			need = -1;
			return;

		}

		tableName = visitor.getTables().keySet().iterator().next().getName();

		if (statement instanceof SQLSelectStatement) {
			sqlType = ServerParse.SELECT;
			parseSelect(statement);
		} else if (statement instanceof SQLUpdateStatement) {
			parseUpdate(statement);
			sqlType = ServerParse.UPDATE;
		} else if (statement instanceof SQLInsertStatement) {
			sqlType = ServerParse.INSERT;

		} else if (statement instanceof SQLDeleteStatement) {
			parseDelete(statement);
			sqlType = ServerParse.DELETE;
		} else {
			sqlType = ServerParse.OTHER;
			need = -1;
			return;
		}

		if (need != -1) {
			need = 1;
		}

	}

	private void parseDelete(SQLStatement statement) {
		SQLDeleteStatement delStmt = (SQLDeleteStatement) statement;

		parseDDLCondition();
	}

	private void parseInsert(SQLStatement statement) {
		SQLInsertStatement inserStmt = (SQLInsertStatement) statement;
		for (SQLExpr p : inserStmt.getValues().getValues()) {
			String cn = p.toString();
			cn = fixSqlOpr(cn);
			values.add(cn);
		}
		for (SQLExpr p : inserStmt.getColumns()) {
			String cn = p.toString();
			cn = fixSqlOpr(cn);
			fields.add(cn);
		}
	}

	private void parseUpdate(SQLStatement statement) {
		SQLUpdateStatement updateStmt = (SQLUpdateStatement) statement;
		for (SQLUpdateSetItem item : updateStmt.getItems()) {
			System.out.println("hry_redis_update :  " + item.getValue());
			values.add(item.getValue());
			String cn = item.getColumn().toString();
			cn = fixSqlOpr(cn);
			fields.add(cn);
		}
		parseDDLCondition();
	}

	private void parseDDLCondition() {
		int cSize = visitor.getConditions().size();
		System.out.println("hry_parseUpdate_cSize :  " + tableName);
		if (cSize > 1) {
			all = 1;
		}
		if (cSize == 0) {
			keyField = "";
			all = 1;
		} else {
			Condition c = visitor.getConditions().get(0);
			if (!c.getOperator().equals("=")) {
				all = 1;
			}

			TableConfig tc = schema.getTables().get(tableName.toUpperCase());
			if (tc == null) {
				need = -1;
				return;
			}
			keyField=tc.getPrimaryKey();
			
			String kf = c.getColumn().getName();

			if (!kf.equalsIgnoreCase(tc.getPrimaryKey())) {

				all = 1;
			}
			
			if (c.getValues().size() != 1) {
				all = 1;
			}
			keyValue = c.getValues().get(0);

		}
	}

	private int parseSelect(SQLStatement statement) {
		int cSize = visitor.getConditions().size();
		if (cSize > 1) {
			if (sqlType == ServerParse.SELECT) {
				need = -1;
				return need;
			}

		}
		if (cSize == 0) {
			keyField = "";
			need = -1;
			return need;
		} else {
			Condition c = visitor.getConditions().get(0);
			if (!c.getOperator().equals("=")) {
				if (sqlType == ServerParse.SELECT) {
					need = -1;
					return need;
				}

			}
			TableConfig tc = schema.getTables().get(tableName.toUpperCase());
			if (tc == null) {
				need = -1;
				return need;
			}
			String kf = c.getColumn().getName();
			if (!kf.equalsIgnoreCase(tc.getPrimaryKey())) {
				if (sqlType == ServerParse.SELECT) {
					need = -1;
					return need;
				}

			}
			if (c.getValues().size() != 1) {
				if (sqlType == ServerParse.SELECT) {
					need = -1;
					return need;
				}

			}
			keyValue = c.getValues().get(0);

		}

		Iterator<Column> colIt = visitor.getColumns().iterator();
		while (colIt.hasNext()) {
			Column col = colIt.next();

			if (col.getName().equalsIgnoreCase("*")) {
				fields.clear();
				selectFiledX=1;
				break;
			}
			fields.add(col.getName());

		}
		return 0;
	}

	public boolean isDDL() {
		return sqlType == ServerParse.INSERT || sqlType == ServerParse.DELETE
				|| sqlType == ServerParse.UPDATE;
	}
	
	public boolean isDel() {
		return sqlType == ServerParse.DELETE;
	}

	public boolean isUpd() {
		return sqlType == ServerParse.UPDATE;
	}
	
	public boolean isIns() {
		return sqlType == ServerParse.INSERT;
	}
	
	public boolean isDQL() {
		return sqlType == ServerParse.SELECT;
	}

	public void ddlFlushToRedis() {
		handler.ddlFlushToRedis();
	}

	public void dqlFlushToRedis() {
		if(chkBackNeedRedis() > 0){
			handler.dqlFlushToRedis();
			fields = new ArrayList<String>();
			dbFields = new ArrayList<String>();
			values = new ArrayList<Object>();
		}
	}

	public int findField(String f) {
		if(selectFiledX==1){
			return 1;
		}
		for (int i = 0; i < fields.size(); i++) {
			String _f = fields.get(i);
			if (_f.equalsIgnoreCase(f)) {
				return i;
			}
		}
		return -1;
	}

	private String fixSqlOpr(String cn) {
		if (cn.startsWith("'") && cn.endsWith("'")) {
			cn = cn.substring(1, cn.length() - 1);
		}
		return cn;
	}

	public void addValue(Object v) {
		if (chkBackNeedRedis() > 0) {
			values.add(v);
		}
	}

	public void addField(String f) {
		if (chkBackNeedRedis() > 0) {
			fields.add(f);
		}
	}

	private void backNeedRedis() {

		if (need == -2) {
			return;
		}
		if (!isDQL()) {
			need = -2;
			return;
		}
		if (tableName == null || tableName.equals("")) {
			need = -2;
			return;
		}
		TableConfig tc = schema.getTables().get(tableName.toUpperCase());
		if (tc == null) {
			need = -2;
			return;
		}
		String key = tc.getPrimaryKey();
		if (key == null || key.equals("")) {
			need = -2;
			return;
		}
		if (findField(key) >= 0) {
			need = 1;
			return;
		}
		need = -2;
		return;
	}

	public int chkBackNeedRedis() {
		backNeedRedis();
		return need;
	}

	public String getPrimaryKey() {
		if (keyField != null && keyField.length() > 0) {
			return keyField;
		}
		if (tableName != null && tableName.length() > 0) {
			return "";
		}
		return schema.getTables().get(tableName.toUpperCase()).getParentKey();

	}
	
	
	public HashMap getRow(){
		HashMap m=new HashMap();
		List<String> _fields= fields;
		if(selectFiledX==1){
			_fields=dbFields;
		}
		
		for(int i=0;i<_fields.size();i++){
			String a= values.get(i).toString();
			if(a.startsWith("'")&&a.endsWith("'")){
				a=a.substring(1, a.length()-2);
			}
			m.put(_fields.get(i),a);
		}
		return m;
	}
	
	public HashMap getRowObj(){
		HashMap m=new HashMap();
		List<String> _fields= fields;
		if(selectFiledX==1){
			_fields=dbFields;
		}
		
		for(int i=0;i<_fields.size();i++){
			String a= values.get(i).toString();
			
			m.put(_fields.get(i),a);
		}
		return m;
	}

	public RouteResultset getRrs() {
		return rrs;
	}

	public void setRrs(RouteResultset rrs) {
		this.rrs = rrs;
	}
	
	
}
