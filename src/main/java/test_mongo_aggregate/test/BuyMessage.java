package test_mongo_aggregate.test;



/**
 * 消费请求的实体类
* @ClassName: BuyMessage
* @author 张飞 zhangfei@xiu8.com
* @date 2014年7月17日 上午10:58:38
* @version V1.0
 */
public class BuyMessage extends ActivityMessage {
	
	/**
	 * 消费类型,如送礼,送千纸鹤
	 */
	public String consumeType;
	/**
	 * 消息类型
	 */
	public String msgType;
	/**
	 * 商品ID
	 */
	public int goodsId;
	
	/**
	 * 商品类型
	 */
	public String goodsType;
	
	/**
	 * 下订单的时间戳
	 */
	public long time;
	
	/**
	 * 购买个数
	 */
	public int count;
	
	/**
	 * 销售价格
	 */
	public int salePrice;
	
	/**
	 * 回收价格
	 */
	public int recyclePrice;
	
	/**
	 * 商品名称
	 */
	public String goodsName;
	
	/**
	 * 付款者ID
	 */
	public long buyerId;
	
	/**
	 * 获得者ID
	 */
	public long geterId;
	
	/**
	 * 来自背包
	 */
	public boolean fromBag;
	
	/**
	 * 消费代码
	 */
	public String msgCode;
	
	/**
	 * 错误消息
	 */
	public String errorMsg;
	
	/**
	 * 处理状态
	 */
	public int state;
	
	/**
	 * 房间ID
	 */
	public long roomId;
	
	/**
	 * 能否回收
	 */
	public boolean canRecycle;
	
	/**
	 * 是否是幸运礼物
	 */
	public boolean isLucky;
	
	/**
	 * 背包剩余个数
	 */
	public int bagBalance;
	
	/**
	 * base goods id
	 */
	public String baseGoodsId;
	
	/**
	 * 赠送的是否是千纸鹤
	 */
	public boolean isPaperCrane;
	
	/**
	 * 是否是超级礼物
	 */
	public boolean isSuperGift;
	
	/**
	 * 是否是趣味礼物
	 */
	public boolean isInterestGift;
	
	/**
	 * 剩余秀币数
	 */
	public int xiuBiBalance;
	
	public BuyMessage(){
		
	}
	
	public BuyMessage(int goodsId,String goodsType,long buyerId,long geterId,long roomId){
		this.goodsId = goodsId;
		this.goodsType = goodsType;
		this.buyerId = buyerId;
		this.geterId = geterId;
		this.roomId = roomId;
	}
	
	public BuyMessage(int goodsId, String goodsType,long time, int salePrice,long buyerId, long geterId, long roomId, String msgCode,int state) {
		this.goodsId = goodsId;
		this.goodsType = goodsType;
		this.buyerId = buyerId;
		this.geterId = geterId;
		this.roomId = roomId;
		this.time = time;
		this.salePrice = salePrice;
		this.msgCode = msgCode;
		this.state = state;
	}
	
}
