package test_mongo_aggregate.test;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.nutz.ioc.loader.annotation.Inject;
import org.nutz.log.Log;
import org.nutz.log.Logs;




import test_mongo_aggregate.mongodb.MongoConnector;
import test_mongo_aggregate.mongodb.MongoDao;

import com.mongodb.MongoException;

/**
 * @author WaterHsu@xiu8.com
 * @version 2014年10月16日
 */
public class ConsumeAnalysisService {
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
	 * 添加消费数据
	 * @param buyMessage
	 */
	public void addConsumeData(BuyMessage buyMessage){
		if(!"1".equals(buyMessage.msgCode)){
			log.info("消费不成功，不进行统计");
			return;
		}
		Map<String, Object> exist = new HashMap<String, Object>();
		exist.put("activeId", buyMessage.activityId);
		exist.put("senderId", buyMessage.buyerId);
		exist.put("receiverId", buyMessage.geterId);
		exist.put("roomId", buyMessage.roomId);
		exist.put("consume_time", buyMessage.time);
		if(null != mongoDao.findDocumentsWithCondition("consume_analysis_record", exist)){
			log.info("该消息已处理");
			return;
		}
		Map<String, Object> base_data = new HashMap<String, Object>();
		base_data.put("senderId", buyMessage.buyerId);
		base_data.put("receiverId", buyMessage.geterId);
		//这个role可能会有问题
		base_data.put("role", 2);
		base_data.put("roomId", buyMessage.roomId);
		base_data.put("senderFamilyId", 1201);
		base_data.put("receiverFamilyId", 1205);
		base_data.put("goodsId", buyMessage.goodsId);
		base_data.put("goodsCount", buyMessage.count);
		base_data.put("out_money", buyMessage.salePrice * buyMessage.count);
		base_data.put("in_money", buyMessage.canRecycle ? buyMessage.recyclePrice * buyMessage.count : 0);
		base_data.put("time", DateUtils.getDay(buyMessage.time));
		mongoDao.addConsumeData("consume_analysis", base_data);
		exist.clear();
		exist.put("activeId", buyMessage.activityId);
		exist.put("senderId", buyMessage.buyerId);
		exist.put("receiverId", buyMessage.geterId);
		exist.put("roomId", buyMessage.roomId);
		exist.put("consume_time", buyMessage.time);
		exist.put("analyse_time", System.currentTimeMillis());
		mongoDao.addData("consume_analysis_record", exist);
	}
	
	/**
	 * 每个用户送出各种幸运礼物总价
	 * @param userId 用户id
	 * @param goodsId  商品id(幸运礼物)
	 * @param time  查询时间
	 * @param type 查询类型(week)
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public Map queryUserLuckyGiftTotalMoney(long senderId, int[] goodsId, long time, String type){
		Map<String, Object> condition = new HashMap<String, Object>();
		condition.put("senderId", senderId);
		String[] colums = new String[]{"senderId", "goodsId", "out_money"};
		String queryValue = "out_money";
		return queryMongoDB(condition, goodsId, time, type, colums, queryValue);
	}
	
	/**
	 * 每个用户送出每种幸运礼物多少个
	 * @param userId  用户id
	 * @param goodsId  商品id(幸运礼物)
	 * @param time  查询时间
	 * @param type  查询类型(week)
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public Map queryUserLuckyGiftTotalCount(long senderId, int[] goodsId, long time, String type){
		Map<String, Object> condition = new HashMap<String, Object>();
		condition.put("senderId", senderId);
		String[] colums = new String[]{"senderId", "goodsId", "goodsCount"};
		String queryValue = "goodsCount";
		return queryMongoDB(condition, goodsId, time, type, colums, queryValue);
	}
	
	/**
	 * 每个用户送出某个礼物多少个给某人
	 * @param senderId  送礼用户id
	 * @param getterId  收礼用户id
	 * @param goodsId  商品id
	 * @param time  查询时间
	 * @param type  查询时间类型(week)
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public Map queryUserGift2SenderCount(long senderId, long getterId, int[] goodsId, long time, String type){
		Map<String, Object> condition = new HashMap<String, Object>();
		condition.put("senderId", senderId);
		condition.put("receiverId", getterId);
		String[] colums = new String[]{"senderId", "receiverId", "goodsId", "goodsCount"};
		String queryValue = "goodsCount";
		return queryMongoDB(condition, goodsId, time, type, colums, queryValue);
	}
	
	/**
	 * 每个用户总共送出总额多少的礼物
	 * @param senderId  送礼用户id
	 * @param goodsId  商品id
	 * @param time   查询时间
	 * @param type   查询时间类型(day, week,month, total)
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public Map queryUserAllGiftTotalMoney(long senderId, int[] goodsId, long time, String type){
		Map<String, Object> condition = new HashMap<String, Object>();
		condition.put("senderId", senderId);
		String[] colums = new String[]{"senderId", "goodsId", "out_money"};
		String queryValue = "out_money";
		return queryMongoDB(condition, goodsId, time, type, colums, queryValue);
	}
	
	/**
	 * 每个用户收到某个礼物多少个
	 * @param receiverId  收礼者id
	 * @param goodsId   商品id
	 * @param time  查询时间
	 * @param type  查询时间类型(week)
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public Map queryReceiverReceiveGiftCount(long receiverId, int[] goodsId, long time, String type){
		Map<String, Object> condition = new HashMap<String, Object>();
		condition.put("receiverId", receiverId);
		String[] colums = new String[]{"receiverId", "goodsId", "goodsCount"};
		String queryValue = "goodsCount";
		return queryMongoDB(condition, goodsId, time, type, colums, queryValue);
	}
	
	/**
	 * 某个用户消费多少金额
	 * @param senderId 消费用户id
	 * @param time  查询时间
	 * @param type  查询类型(total)
	 * @return 
	 */
	@SuppressWarnings("rawtypes")
	public Map queryUserTotalConsumeMoney(long senderId, long time, String type){
		Map<String, Object> condition = new HashMap<String, Object>();
		condition.put("senderId", senderId);
		String[] colums = new String[]{"senderId", "out_money"};
		String queryValue = "out_money";
		return queryMongoDB(condition, null, time, type, colums, queryValue);
	}
	
	/**
	 * 某个家族消费的金额
	 * @param familyId  家族id
	 * @param time   查询时间
	 * @param type   查询时间类型(day, week, month, total)
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public Map queryFamilyConsumeMoney(int familyId, long time, String type){
		Map<String, Object> condition = new HashMap<String, Object>();
		condition.put("senderFamilyId", familyId);
		String[] colums = new String[]{"senderFamilyId", "out_money"};
		String queryValue = "out_money";
		return queryMongoDB(condition, null, time, type, colums, queryValue);
	}
	
	/**
	 * 家族中某人消费金额
	 * @param familyId   家族id
	 * @param senderId   家族中成员id
	 * @param time       查询时间
	 * @param type       查询的时间类型(day, week, month, total)
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public Map queryFamilyUserConsumeMoney(int familyId, long senderId, long time, String type){
		Map<String, Object> condition = new HashMap<String, Object>();
		condition.put("senderFamilyId", familyId);
		condition.put("senderId", senderId);
		String[] colums = new String[]{"senderFamilyId", "senderId", "out_money"};
		String queryValue = "out_money";
		return queryMongoDB(condition, null, time, type, colums, queryValue);
	}
	
	/**
	 * 每个用户收入金额
	 * @param receiverId  收礼用户id
	 * @param role        收礼用户角色
	 * @param time        查询时间
	 * @param type        查询的时间类型(day, week, month, total)
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public Map queryReceiverIncomeMoney(int receiverId, int role, long time, String type){
		Map<String, Object> condition = new HashMap<String, Object>();
		condition.put("receiverId", receiverId);
		condition.put("role", role);
		String[] colums = new String[]{"receiverId", "role", "in_money"};
		String queryValue = "in_money";
		return queryMongoDB(condition, null, time, type, colums, queryValue);
	}
	
	/**
	 * 家族中每个主播的收入
	 * @param familyId      家族id
	 * @param receiverId    收礼用户id 
	 * @param role          角色(主播)
	 * @param time          查询时间
	 * @param type          查询的时间类型(day, week, month, total)
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public Map queryFamilyEachUserIncome(int familyId, long receiverId, int role, long time, String type){
		Map<String, Object> condition = new HashMap<String, Object>();
		condition.put("receiverId", receiverId);
		condition.put("receiverFamilyId", familyId);
		condition.put("role", role);
		String[] colums = new String[]{"receiverFamilyId", "receiverId", "role", "in_money"};
		String queryValue = "in_money";
		return queryMongoDB(condition, null, time, type, colums, queryValue);
	}
	
	/**
	 * 家族中所有主播的收入总和
	 * @param familyId    家族id
	 * @param role        收礼用户角色(主播)
	 * @param time        查询时间
	 * @param type        查询的时间类型(day, week, month, total)
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public Map queryFamilyAllUserIncome(int familyId, int role, long time, String type){
		Map<String, Object> condition = new HashMap<String, Object>();
		condition.put("receiverFamilyId", familyId);
		condition.put("role", role);
		String[] colums = new String[]{"receiverFamilyId", "role", "in_money"};
		String queryValue = "in_money";
		return queryMongoDB(condition, null, time, type, colums, queryValue);
	}
	
	/**
	 * 房间消费总额
	 * @param roomId    房间id
	 * @param time      查询时间
	 * @param type      查询的时间类型(day, week, month, total)
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public Map queryRoomTotalConsume(long roomId, long time, String type){
		Map<String, Object> condition = new HashMap<String, Object>();
		condition.put("roomId", roomId);
		String[] colums = new String[]{"roomId", "out_money"};
		String queryValue = "out_money";
		return queryMongoDB(condition, null, time, type, colums, queryValue);
	}
	
	/**
	 * 房间内每个用户的消费金额
	 * @param roomId      房间id
	 * @param senderId    送礼用户id
	 * @param time        查询时间
	 * @param type        查询时间类型(day, week, month, total)
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public Map queryRoomEachUserTotalConsume(long roomId, long senderId, long time, String type){
		Map<String, Object> condition = new HashMap<String, Object>();
		condition.put("roomId", roomId);
		condition.put("senderId", senderId);
		String[] colums = new String[]{"roomId", "senderId", "out_money"};
		String queryValue = "out_money";
		return queryMongoDB(condition, null, time, type, colums, queryValue);
	}
	
	
	//public Map queryRoomEachUserTotal
	
	/**
	 * 内部查询数据的方法
	 * @param condition         集合名
	 * @param goodsCondition    商品集合
	 * @param time              查询时间
	 * @param type              查询的时间类型
	 * @param colums            要查询的字段
	 * @param queryValue        要统计加和的字段
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	private Map queryMongoDB(Map<String, Object> condition, int[] goodsCondition, long time, String type, String[] colums, String queryValue){
		Map map = null;
		if("day".equals(type)){
			condition.put("time", DateUtils.getDay(time));
			List<Map> list = mongoDao.findDocumentsWithCondition("consume_analysis", condition);
			if(null == list || list.size() < 1){
				map = null;
			}else{
				map = list.get(0);
			}
		}else if("week".equals(type)){
			int[] timeArray = DateUtils.getThisWeekInt(new Date(time));
			map = mongoDao.findConsumeDocument("consume_analysis", condition, goodsCondition, timeArray, colums, queryValue);
		}else if("month".equals(type)){
			int[] timeArray = DateUtils.getThisMonth(new Date(time));
			map = mongoDao.findConsumeDocument("consume_analysis", condition, goodsCondition, timeArray, colums, queryValue);
		}else if("total".equals(type)){
			map = mongoDao.findConsumeDocument("consume_analysis", condition, goodsCondition, null, colums, queryValue);
		}else{
			log.error("时间类型不对");
		}
		return map;
	}
}
