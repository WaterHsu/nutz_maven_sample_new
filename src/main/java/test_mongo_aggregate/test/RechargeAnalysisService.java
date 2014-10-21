package test_mongo_aggregate.test;

import java.math.BigDecimal;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;





import org.nutz.json.Json;
import org.nutz.log.Log;
import org.nutz.log.Logs;









import test_mongo_aggregate.mongodb.MongoConnector;
import test_mongo_aggregate.mongodb.MongoDao;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.mongodb.MongoException;

/**
 * @author WaterHsu@xiu8.com
 * @version 2014年10月16日
 */
public class RechargeAnalysisService {

	private static Log log = Logs.get();
	private static MongoConnector mongoConn; // new MongoConnector("127.0.0.1", 27017);
	private static MongoDao mongoDao; //mongoConn.getDao("xiuba_recharge");
	
	
	
	static{
		try {
			mongoConn = new MongoConnector("127.0.0.1", 27017);
			mongoDao = mongoConn.getDao("test_xiuba_analysis");
		} catch (UnknownHostException e) {
			log.error("mongodb连接出错! 找不到mongodb所在服务器", e);
		} catch (MongoException e) {
			log.error("mongodb连接出错！", e);
		}
	}
	
	/**
	 * 将充值消息提取相关信息存入到mongodb中
	 * 存储了userId  roomId  familyId channleId  proxyId  money  time
	 * @param recharge
	 */
	public void addRechargeData(RechargeMessage recharge){
		if(!"1".equals(recharge.msgCode)){
			log.info("充值不成功，不进行统计");
			System.out.println("充值不成功，不进行统计");
			return;
		}
		Map<String, Object> exist = new HashMap<String, Object>();
		exist.put("orderId", recharge.orderId);
		if(null != mongoDao.findDocumentsWithCondition("recharge_analysis_record", exist)){
			log.info("该消息已经处理");
			System.out.println("该消息已经处理");
			return;
		}
		Map<String, Object> base_data = new HashMap<String, Object>();
		base_data.put("userId", recharge.userId);
		base_data.put("roomId", recharge.roomId);
		base_data.put("familyId", 1203);
		base_data.put("channelId", recharge.channelId);
		base_data.put("proxyId", recharge.proxyId);
		base_data.put("money", recharge.rechargeRmb.intValue());
		base_data.put("time", DateUtils.getDay(recharge.time));
		base_data.put("count", recharge.rechargeCount);
		mongoDao.addRechargeData("recharge_analysis", base_data);
		exist.clear();
		exist.put("orderId", recharge.orderId);
		exist.put("order_time", recharge.time);
		exist.put("analysis_time", System.currentTimeMillis());
		mongoDao.addData("recharge_analysis_record", exist);
		log.info("消息添加成功");
		System.out.println("消息添加成功");
	}
	
	/**
	 * 查询每个房间的充值金额(day, week, month, total)
	 * @param roomId
	 * @param time
	 * @param type
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public Map queryRoomRecharge(long roomId, long time, String type){
		Map<String, Object> condition = new HashMap<String, Object>();
		condition.put("roomId", roomId);
		return queryMongoDB(condition, time, type);
	}
	
	/**
	 * 查询每个用户的充值金额(day, week, month, total)
	 * @param userId
	 * @param time
	 * @param type
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public Map queryUserRecharge(long userId, long time, String type){
		Map<String, Object> condition = new HashMap<String, Object>();
		condition.put("userId", userId);
		return queryMongoDB(condition, time, type);
	}
	
	/**
	 * 查询每个代理的充值金额(day, week, month, total)
	 * @param proxyId
	 * @param time
	 * @param type
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public Map queryProxyRecharge(long proxyId, long time, String type){
		Map<String, Object> condition = new HashMap<String, Object>();
		condition.put("proxyId", proxyId);
		return queryMongoDB(condition, time, type);
	}
	
	/**
	 * 查询每个渠道的充值金额(day, week, month, total)
	 * @param channelId
	 * @param time
	 * @param type
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public Map queryChannelRecharge(int channelId, long time, String type){
		Map<String, Object> condition = new HashMap<String, Object>();
		condition.put("channelId", channelId);
		return queryMongoDB(condition, time, type);
	}
	
	/**
	 * 查询mongodb的内部接口
	 * @param condition
	 * @param time
	 * @param type
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	private Map queryMongoDB(Map<String, Object> condition, long time, String type){
		Map map = null;
		if("day".equals(type)){
			condition.put("time", DateUtils.getDay(time));
			List<Map> list = mongoDao.findDocumentsWithCondition("recharge_analysis", condition);
			if(null == list || list.size() < 1){
				map = null;
			}else{
				map = list.get(0);
			}
		}else if("week".equals(type)){
			int[] timeArray = DateUtils.getThisWeekInt(new Date(time));
			map = mongoDao.findRechargeDocument("recharge_analysis", condition, timeArray);
		}else if("month".equals(type)){
			int[] timeArray = DateUtils.getThisMonth(new Date(time));
			map = mongoDao.findRechargeDocument("recharge_analysis", condition, timeArray);
		}else if("total".equals(type)){
			map = mongoDao.findRechargeDocument("recharge_analysis", condition, null);
		}else{
			log.error("统计的时间类型不正确");
		}
		return map;
	}
	
	public static void main(String args[]){
	/*	RechargeMessage message = new RechargeMessage();
		message.userId = 10014;
		message.roomId = 2001;
		message.channelId = 3;
		message.proxyId = 10004;
		message.rechargeRmb = BigDecimal.valueOf(100000);
		message.rechargeCount = 1;
		message.time = System.currentTimeMillis();
		message.orderId = "eefe5151515";
		message.msgCode = "1";
		
		RechargeAnalysisService analysis = new RechargeAnalysisService();
		analysis.addRechargeData(message);
		System.out.println("success!!");*/
		
		RechargeAnalysisService analysis = new RechargeAnalysisService();
		Map map = analysis.queryUserRecharge(10014, System.currentTimeMillis(), "week");
		System.out.println(map);
		System.out.println();
		System.out.println(map.get("result"));
		Json.toJson(map.get("result"));
		List list = Json.fromJsonAsList(List.class, Json.toJson(map.get("result")));
		System.out.println(list);
	}
}
