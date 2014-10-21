/**
 * 秀吧网络科技有限公司版权所有
 * Copyright (C) xiu8 Corporation. All Rights Reserved
 */
package test_mongo_aggregate.mongodb;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;




import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.util.Hash;
import com.mongodb.util.JSON;

/**
 * @author WaterHsu@xiu8.com
 * @version 2014年10月16日
 */
public class MongoDao {

	private DB db;
	
	public MongoDao(DB db){
		this.db = db;
	}
	
	/**
	 * 根据集合名获得一个Collection集合
	 * @param collName
	 * @return
	 */
	public DBCollection getSingleCollection(String collName){
		return db.getCollection(collName);
	}
	
	/**
	 * 获得所有集合的名字
	 * @return
	 */
	public Set<String> getCollectionNames(){
		Set<String> collNames = db.getCollectionNames();
		return collNames;
	}
	
	/**
	 * 获得所有的集合
	 * @return
	 */
	public Set<DBCollection> getCollections(){
		Set<String> collNames = db.getCollectionNames();
		Set<DBCollection> collections = new HashSet<DBCollection>();
		for(String collName : collNames){
			collections.add(db.getCollection(collName));
		}
		return collections;
	}
	
	/**
	 * 插入一条map类型数据
	 * @param collName 集合名
	 * @param map  要插入的数据
	 */
	public void addData(String collName, Map<String, Object> map){
		getSingleCollection(collName).insert(new BasicDBObject(map));
	}
	
	/**
	 * 插入一条充值数据
	 * 先根据各个维度(money, count维度除外)查询是否存在相同数据，如果存在，则将money和count增加到已存在的数据记录中，如果不存在，则添加一条数据
	 * @param collName
	 * @param map
	 */
	public void addRechargeData(String collName, Map<String, Object> map){
		Map<String, Object> condition = new HashMap<String, Object>();
		condition.putAll(map);
		condition.remove("money");
		condition.remove("count");
		if(null == findDocumentsWithCondition(collName, condition)){
			addData(collName, map);
		}else{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put("money", map.get("money"));
			values.put("count", map.get("count"));
			updateDocuments(collName, condition, values, "inc");
		}
	}
	
	/**
	 * 插入一条消费数据
	 * 先根据各个维度(in_money, out_money, goodsCount维度除外)查询是否存在相同数据，如果存在，则将in_money,out_money和goodsCount增加到已存在的数据记录中，如果不存在，则添加一条数据
	 * @param collName
	 * @param map
	 */
	public void addConsumeData(String collName, Map<String, Object> map){
		Map<String, Object> condition = new HashMap<String, Object>();
		condition.putAll(map);
		condition.remove("in_money");
		condition.remove("out_money");
		condition.remove("goodsCount");
		if(null == findDocumentsWithCondition(collName, condition)){
			addData(collName, map);
		}else{
			Map<String, Object> values = new HashMap<String, Object>();
			values.put("in_money", map.get("in_money"));
			values.put("out_money", map.get("out_money"));
			values.put("goodsCount", map.get("goodsCount"));
			updateDocuments(collName, condition, values, "inc");
		}
	}
	
	/**
	 * 插入一条json类型数据
	 * @param collName 集合名
	 * @param jsonStr  json格式字符串
	 */
	public void addData(String collName, String jsonStr){
		DBObject dbObject = (DBObject)JSON.parse(jsonStr);
		getSingleCollection(collName).insert(dbObject);
	}
	
	/**
	 * 查询某个集合中的第1条数据
	 * @param collName
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public Map findFirstDocument(String collName){
		 DBCollection collection = getSingleCollection(collName);
		 DBObject dbObject = collection.findOne();
		 return dbObject.toMap();
	}
	
	/**
	 * 查询某个集合中的所有记录
	 * @param collName 集合名
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public List<Map> findAllDocuments(String collName){
		DBCollection collection = getSingleCollection(collName);
		DBCursor cursor = collection.find();
		return returnValue(cursor);
	}
	
	/**
	 * 在某个集合中根据条件查询集合
	 * 类似于 select * from ** where *=* and *=*
	 * @param collName  集合名
	 * @param condition  查询条件  
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public List<Map> findDocumentsWithCondition(String collName, Map<String, Object> condition){
		BasicDBObject query = new BasicDBObject(condition);
		DBCursor cursor = find(collName, query);
		return returnValue(cursor);
	}
	
	/**
	 * 根据条件查询以及排序
	 * 类似于 select * from ** where *=* and *=* order by **
	 * @param collName  集合名
	 * @param condition  查询条件
	 * @param sortName  要排序的字段
	 * @param index  1顺序  -1倒序
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public List<Map> findDocumentsWithCondition(String collName, Map<String, Object> condition, String sortName, int index){
		BasicDBObject query = new BasicDBObject(condition);
		DBCursor cursor = find(collName, query);
		cursor = cursor.sort(new BasicDBObject(sortName, index));
		return returnValue(cursor);
	}
	
	/**
	 * 根据条件查询多少条数据并对这些数据排序  
	 * 根据条件查询排序后选择多少条数据
	 * 类似于 select top 100 * from ** where *=* and *=* order by **
	 * @param collName  集合名
	 * @param condition  查询条件
	 * @param sortName  要排序的字段
	 * @param limit  限制要查询的条数
	 * @param index  1顺序  -1倒序
	 * @param bf  true排序之前选择多少条数据    false排序之后选择多少条数据
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public List<Map> findDocumentsWithCondition(String collName, Map<String, Object> condition, String sortName, int limit, int index, boolean fb){
		BasicDBObject query = new BasicDBObject(condition);
		DBCursor cursor = find(collName, query);
		if(fb){
			cursor = cursor.limit(limit).sort(new BasicDBObject(sortName, index));
		}else{
			cursor = cursor.sort(new BasicDBObject(sortName, index)).limit(limit);
		}
		return returnValue(cursor);
	}
	
	/**
	 * 利用aggregate聚合框架来实现对房间，用户，家族，充值渠道，充值代理的充值金额的week, month, total的统计
	 * @param collName  集合名
	 * @param condition  匹配条件
	 * @param timeArray  时间段  用于找week  month
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public Map findRechargeDocument(String collName, Map<String, Object> condition, int[] timeArray){
		BasicDBObject bdb1 = null;
		String key = null;
		for(Entry entry : condition.entrySet()){
			bdb1 = new BasicDBObject(entry.getKey().toString(), entry.getValue());
			key = entry.getKey().toString();
		}
		
		DBObject match = null;
		//拼装匹配条件  where 
		if(null != timeArray && timeArray.length == 2){
			BasicDBObject[] array = {bdb1, new BasicDBObject("time", new BasicDBObject("$gte", timeArray[0])), 
				new BasicDBObject("time", new BasicDBObject("$lte", timeArray[1]))};
			BasicDBObject cond = new BasicDBObject();
			cond.put("$and", array);
			match = new BasicDBObject("$match", cond);
		}else{
			match = new BasicDBObject("$match", bdb1); 
		}
		System.out.println("match: " + match.toMap());
		//拼装从数据库中找出的字段
		DBObject fields = new BasicDBObject();
		fields.put(key, 1);
		fields.put("money", 1);
		fields.put("count", 1);
		DBObject project = new BasicDBObject("$project", fields);
		System.out.println("project: " + project.toMap());
		DBObject _group = new BasicDBObject();
		_group.put(key, "$" + key);
		_group.put("money", "$money");
		DBObject groupFields = new BasicDBObject("_id", _group);
		groupFields.put("moneyCount", new BasicDBObject("$sum", "$money"));
		DBObject group = new BasicDBObject("$group", groupFields);
		System.out.println("group: " + group.toMap());
		AggregationOutput output = getSingleCollection(collName).aggregate(match, project, group);
		System.out.println(output.getCommandResult());
		return output.getCommandResult().toMap();
	}
	
	/**
	 * 利用aggregate聚合框架来实现对消费分析的统计
	 * @param collName  集合名
	 * @param condition  匹配条件
	 * @param timeArray  时间段
	 * @param colums  要查询的字段
	 * @param queryValue  需要加和统计的字段
	 * @return
	 */
	/**
	 * 利用aggregate聚合框架来实现对消费分析的统计
	 * @param collName  集合名
	 * @param condition  匹配条件
	 * @param goodsIds   商品id
	 * @param timeArray 时间段
	 * @param colums  要查询的字段
	 * @param queryValue  需要加和统计的字段
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public Map findConsumeDocument(String collName, Map<String, Object> condition, int[] goodsIds, int[] timeArray, String[] colums, String queryValue){
		BasicDBObject[] array = new BasicDBObject[]{};
		BasicDBObject bdb1 = null;
		int i = 0;
		for(Entry entry : condition.entrySet()){
			bdb1 = new BasicDBObject(entry.getKey().toString(), entry.getValue());
			array[i++] = bdb1;
		}
		DBObject match = null;
		//拼装匹配条件  where 
		if(null != timeArray && timeArray.length == 2){
			array[i++] = new BasicDBObject("time", new BasicDBObject("$gte", timeArray[0])); 
			array[i++] = new BasicDBObject("time", new BasicDBObject("$lte", timeArray[1]));
			
		}
		if(null != goodsIds && goodsIds.length > 0){
			array[i++] = new BasicDBObject("goodsId", new BasicDBObject("$in", goodsIds));
		}
		BasicDBObject cond = new BasicDBObject();
		cond.put("$and", array);
		match = new BasicDBObject("$match", cond);
		
		//拼装从数据库中找出的字段
		DBObject fields = new BasicDBObject();
		DBObject _group = new BasicDBObject();
		for(String colum : colums){
			fields.put(colum, 1);
			_group.put(colum, "$" + colum);
		}
		DBObject project = new BasicDBObject("$project", fields);
		DBObject groupFields = new BasicDBObject("_id", _group);
		groupFields.put("countResult", new BasicDBObject("$sum", "$" + queryValue));
		DBObject group = new BasicDBObject("$group", groupFields);
		AggregationOutput output = getSingleCollection(collName).aggregate(match, project, group);
		System.out.println(output.getCommandResult());
		return output.getCommandResult().toMap();
	}
	
	/**
	 * 查询相应的排行榜
	 * @param collName   集合名
	 * @param condition  条件
	 * @param rank       根据这个字段分类
	 * @param goodsIds   商品id
	 * @param timeArray  时间范围
	 * @param colums     要显示的字段
	 * @param queryValue 相加的字段
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public Map findConsumeDocument(String collName, Map<String, Object> condition, String rank, int[] goodsIds, int[] timeArray, String[] colums, String queryValue){
		BasicDBObject[] array = new BasicDBObject[]{};
		BasicDBObject bdb1 = null;
		int i = 0;
		for(Entry entry : condition.entrySet()){
			bdb1 = new BasicDBObject(entry.getKey().toString(), entry.getValue());
			array[i++] = bdb1;
		}
		DBObject match = null;
		//拼装匹配条件  where 
		if(null != timeArray && timeArray.length == 2){
			array[i++] = new BasicDBObject("time", new BasicDBObject("$gte", timeArray[0])); 
			array[i++] = new BasicDBObject("time", new BasicDBObject("$lte", timeArray[1]));
			
		}
		if(null != goodsIds && goodsIds.length > 0){
			array[i++] = new BasicDBObject("goodsId", new BasicDBObject("$in", goodsIds));
		}
		BasicDBObject cond = new BasicDBObject();
		cond.put("$and", array);
		match = new BasicDBObject("$match", cond);
		//拼装从数据库中找出的字段
		DBObject fields = new BasicDBObject();
		DBObject _group = new BasicDBObject();
		for(String colum : colums){
			fields.put(colum, 1);
			_group.put(colum, "$" + colum);
		}
		DBObject project = new BasicDBObject("$project", fields);
		DBObject groupFields = new BasicDBObject("_id", _group);
		groupFields.put("countResult", new BasicDBObject("$sum", "$" + queryValue));
		DBObject group = new BasicDBObject("$group", groupFields);
		DBObject sort = new BasicDBObject("$sort", "$countResult");
		AggregationOutput output = getSingleCollection(collName).aggregate(match, project, group, sort);
		System.out.println(output.getCommandResult());
		return output.getCommandResult().toMap();
	}
	
	/**
	 * 查询某个字段不等于某个值得所有记录
	 * 类似于 select * from ** where *>* and *<*
	 * 类似于 select * from ** where * in *
	 * @param collName
	 * @param condition
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public List<Map> findDocumentsMoreCondition(String collName, String fieldName, List<Object> values, String type){
		if(values.size() < 2){
			System.out.println("参数不够");
			return null;
		}
		BasicDBObject query = new BasicDBObject();
		if("ne".equals(type)){
			query.put(fieldName, new BasicDBObject("$ne", values.get(0)));
		}else if("gt".equals(type)){
			query.put(fieldName, new BasicDBObject("$gt", values.get(0)));
		}else if("gte".equals(type)){
			query.put(fieldName, new BasicDBObject("$gte", values.get(0)));
		}else if("lt".equals(type)){
			query.put(fieldName, new BasicDBObject("$lt", values.get(0)));
		}else if("lte".equals(type)){
			query.put(fieldName, new BasicDBObject("$lte", values.get(0)));
		}else if("gtlt".equals(type)){
			query.put(fieldName, new BasicDBObject("$gt", values.get(0)).append("$lt", values.get(0)));
		}else if("gtelte".equals(type)){
			query.put(fieldName, new BasicDBObject("$gte", values.get(0)).append("$lte", values.get(0)));
		}else if("gtlte".equals(type)){
			query.put(fieldName, new BasicDBObject("$gt", values.get(0)).append("$lte", values.get(0)));
		}else if("gtelt".equals(type)){
			query.put(fieldName, new BasicDBObject("$gte", values.get(0)).append("$lt", values.get(0)));
		}else if("in".equals(type)){
			query.put(fieldName, new BasicDBObject("$in", values.toArray()));
		}else if("nin".equals(type)){
			query.put(fieldName, new BasicDBObject("$nin", values.toArray()));
		}
		
		DBCursor cursor = find(collName, query);
		return returnValue(cursor);
	}
	
	/**
	 * 更新记录
	 * @param collName  集合名
	 * @param condition  要更新的数据
	 * @param values   更新的值
	 * @param operation  更新的动作 inc  set 
	 */
	@SuppressWarnings("rawtypes")
	public void updateDocuments(String collName, Map<String, Object> condition, Map<String, Object> values, String operation){
		BasicDBObject query = new BasicDBObject(condition);
		BasicDBObject valueObject = new BasicDBObject();
		BasicDBObject temp = new BasicDBObject();
		for(Entry entry : values.entrySet()){
			temp.append(entry.getKey().toString(), entry.getValue());
		}
		valueObject.append("$" + operation, temp);
		update(collName, query, valueObject, false, false);
	} 
	
	/**
	 * 公共查询方法
	 * @param collName
	 * @param query
	 * @return
	 */
	private DBCursor find(String collName, BasicDBObject query){
		DBCollection collection = getSingleCollection(collName);
		DBCursor cursor = collection.find(query);
		return cursor;
	}
	
	/**
	 * 公共返回方法
	 * @param dbCursor
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	private List<Map> returnValue(DBCursor cursor){
		if(cursor.size() < 1){
			return null;
		}
		List<Map> list = new ArrayList<Map>();
		while(cursor.hasNext()){
			list.add(cursor.next().toMap());
		}
		return list;
	}
	
	/**
	 * 公共更新方法
	 * @param query
	 * @param value
	 */
	private void update(String collName, BasicDBObject query, BasicDBObject value, boolean updateSet, boolean multi){
		DBCollection collection = getSingleCollection(collName);
		collection.update(query, value, updateSet, multi);
	}
}
