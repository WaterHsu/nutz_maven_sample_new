/**
 * 秀吧网络科技有限公司版权所有
 * Copyright (C) xiu8 Corporation. All Rights Reserved
 */
package test_mongo_aggregate.mongodb;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.MongoURI;

/**
 * @author WaterHsu@xiu8.com
 * @version 2014年10月16日
 */
public class MongoConnector {
	
//	private Mongo mongo;
	
	private MongoClient mongoClient;
	
	private String user;
	
	private String password;
	
	public MongoConnector(MongoClient mongoClient){
//		this.mongo = mongo;
		this.mongoClient = mongoClient;
	}
	
	/**
	 * 连接mongo
	 * @param host 数据库主机名
	 * @param port 数据库端口
	 * @throws UnknownHostException
	 * @throws MongoException
	 */
	public MongoConnector(String host, int port) throws UnknownHostException, MongoException{
//		this.mongo = new Mongo(host, port);
		this.mongoClient = new MongoClient(host, port);
	}
	
	/**
	 * 使用MongoURI创建复杂的单个连接,以支持RepSet
	 * @param mongoURI  连接mongo的uri
	 * @throws UnknownHostException
	 * @throws MongoException
	 */
	public MongoConnector(String mongoURI) throws UnknownHostException, MongoException{
//		this.mongo = new Mongo(new MongoURI(mongoURI));
		this.mongoClient = new MongoClient(new MongoClientURI(mongoURI));
	}
	
	/**
	 * 数据库访问接口
	 * @param dbname 数据库名
	 * @return
	 */
	public DB getDB(String dbname){
	//	DB db = mongo.getDB(dbname);
		DB db = mongoClient.getDB(dbname);
		if(user != null){
			db.authenticate(user, password.toCharArray());
		}	
		return db;
	}
	
	/**
	 * 获得mongodao 对mongo基本操作的一个小封装
	 * @param dbname
	 * @return
	 */
	public MongoDao getDao(String dbname){
		DB db = mongoClient.getDB(dbname);
		if(user != null){
			db.authenticate(user, password.toCharArray());
		}	
		return new MongoDao(db);
	}
	
	/**
	 * 注销一个MongoDB的连接
	 */
	public void close(){
//		mongo.close();
		mongoClient.close();
	}
	
	public static void close(MongoClient mongoClient){
		mongoClient.close();
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public MongoClient getMongoClient() {
		return mongoClient;
	}
}
