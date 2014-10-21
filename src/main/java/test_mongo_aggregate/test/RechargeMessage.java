package test_mongo_aggregate.test;

import java.math.BigDecimal;

/**
 * 充值成功信息
 * 
 * @author 张飞
 */
public class RechargeMessage extends ActivityMessage{

	/**
	 * 充值金额
	 */
	public BigDecimal rechargeRmb;

	/**
	 * 充值秀币
	 */
	public int rechargeXiuBi;

	/**
	 * 用户ID
	 */
	public long userId;

	/**
	 * 消息代码 1成功
	 */
	public String msgCode;

	/**
	 * 代理ID
	 */
	public long proxyId;

	/**
	 * 充值时间戳
	 */
	public long time;

	/**
	 * 活动标识
	 */
	public String activityMark;

	/**
	 * 房间ID
	 */
	public long roomId;

	/**
	 * 渠道ID
	 */
	public int channelId;
	
	/**
	 * 充值次数
	 */
	public int rechargeCount;

	@Override
	public String toString() {
		return "RechargeSuccessMessage [orderId=" + orderId + ", roomId="
				+ roomId + ", rechargeRmb=" + rechargeRmb + ", rechargeXiuBi="
				+ rechargeXiuBi + ", msgCode=" + msgCode + ", userId=" + userId
				+ ", proxyId=" + proxyId + ", time=" + time + ", activityMark="
				+ activityMark + ", channelId=" + channelId + "]";
	}

}
