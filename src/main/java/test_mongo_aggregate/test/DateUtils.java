package test_mongo_aggregate.test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DateUtils {
	
	private static Log logger = LogFactory.getLog(DateUtils.class);
	public static long getLongTime(int timeType, int time) {
		Calendar cal = Calendar.getInstance();
		switch (timeType) {
			case 1:
				cal.set(Calendar.MINUTE, time > 0 ? time : 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				break;
			case 2:
				cal.set(Calendar.HOUR_OF_DAY, time > 0 ? time : 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				break;
			case 3:
				cal.set(Calendar.DAY_OF_WEEK, time > 0 ? time : 1);
				cal.set(Calendar.HOUR_OF_DAY, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				break;
			case 4:
				cal.set(Calendar.DAY_OF_MONTH, time > 0 ? time : 1);
				cal.set(Calendar.HOUR_OF_DAY, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				break;
		}
		return cal.getTimeInMillis();
	}
	
	
	public static long getyyyyMMddHHmmssSSS(long date){
		SimpleDateFormat t = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		try {
			String s = t.format(date);
			return Long.parseLong(s);
		} catch (Exception e) {
			logger.error(e);
			return 0;
		} 
	}
	
	/**
	 * 获取一个月的第一天的时间戳
	 * @return
	 */
	public static long getFirstDayOfMonth(){
		Calendar c = Calendar.getInstance();
		c.set(Calendar.DAY_OF_MONTH, 1);
		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);
		
		long re = c.getTimeInMillis();
		
		return re;
	}
	
	/**
	 * 获取当月的最后一天末的时间戳
	 * @return
	 */
	public static long getLastTimeOfMonth(){
		Calendar c = Calendar.getInstance();
		c.set(Calendar.DAY_OF_MONTH, 1);
		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);
		
		c.add(Calendar.MONTH, 1);
		long re = c.getTimeInMillis();		
		re -= 1;
		
		return re;
	}
	
	
	/**
	 * 根据时间得到天 格式yyyyMMdd
	 * @param time 时间
	 * @return
	 */
	public static int getDay(long time){
		String date = new SimpleDateFormat("yyyyMMdd").format(time);
		return Integer.parseInt(date);
	}
	
	/**
	 * 获取昨天最后时间的时间戳    结束时间yyyy-MM-dd 23:59:59
	 * 
	 * @return
	 */
	public static long getToDayLastTime() {
		Calendar cal = Calendar.getInstance();
		// 设置时间
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.add(Calendar.DAY_OF_MONTH,1);
		cal.add(Calendar.SECOND, -1);
		return cal.getTime().getTime();
	}
	
	/**
	 * 得到yyyy-MM-dd格式的日期
	 * 
	 * @param date
	 * @return
	 */
	public static String getYear_Month_Day(Date date) {
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		return format.format(date);
	}
	
	/**
	 * 得到yyyy-MM-dd HH:mm:ss格式的日期
	 * 
	 * @param date
	 * @return
	 */
	public static String getDateString(Date date) {
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return format.format(date);
	}
	

	/**
	 * 根据传入的时间戳取得当天凌晨的时间戳 
	 * 如    ：2013-09-30 14:32:58.687  1380522778687(参数)
	 * 得到：2013-09-30 00:00:00.000  1380470400000(返回值)
	 * @param currentstamp
	 * @return
	 */
	public static long getZeroTimestamp(long currentstamp){
		Date date12=new Date(currentstamp);
		SimpleDateFormat f= new SimpleDateFormat("yyyyMMdd");
		try{
			String yyyyMMdd=f.format(date12);//20130923
			Date date = f.parse(yyyyMMdd);//
			return date.getTime();
		}catch (Exception e) {
			// TODO: handle exception
		}
		return 0;
	}
	/**
	 * 根据传入的时间戳取得当天午夜的时间戳 
	 * 如    ：2013-09-30 14:32:58.687  1380522778687(参数)
	 * 得到：2013-09-30 23:59:59.999  1380556799999(返回值)
	 * @param currentstamp
	 * @return
	 */
	public static long getMidnightTimestamp(long currentstamp){
		Date date12=new Date(currentstamp);
		SimpleDateFormat f= new SimpleDateFormat("yyyyMMdd");
		try{
			String yyyyMMdd=f.format(date12);//20130923
			Date date = f.parse(yyyyMMdd);//凌晨时间戳
			return date.getTime()+24*60*60*1000L-1;//午夜时间戳
		}catch (Exception e) {
			// TODO: handle exception
		}
		return 0;
	}
	
	
	/**
	 * 获取上周一到周日的时间段。返回日期类型
	 * 
	 * @return
	 */
	public static Date[] getLastWeek() {
		Date startDate = new Date(System.currentTimeMillis());
		Date endDate = new Date(System.currentTimeMillis());
		int week = startDate.getDay();
		week = week == 0 ? 6 : week - 1;
		int oneDay = 1000 * 60 * 60 * 24;
		endDate.setTime(startDate.getTime() - week * oneDay - oneDay);
		startDate.setTime(endDate.getTime() - 6 * oneDay);
		return new Date[] { startDate, endDate };
	}
	
	
	public static Date[] getThisWeek(){
		Date startDate = new Date(System.currentTimeMillis());
		Date endDate = new Date(System.currentTimeMillis());
		int week = startDate.getDay();
		week = week == 0 ? 6 : week - 1;
		int oneDay = 1000 * 60 * 60 * 24;
		endDate.setTime(startDate.getTime() + (6 - week) * oneDay);
		startDate.setTime(endDate.getTime() - 6 * oneDay);
		return new Date[] { startDate, endDate };
	}
	
	public static Date[] getLastWeek(Date date){
		Date startDate = new Date(date.getTime());
		Date endDate = new Date(date.getTime());
		int week = startDate.getDay();
		week = week == 0 ? 6 : week - 1;
		int oneDay = 1000 * 60 * 60 * 24;
		endDate.setTime(startDate.getTime() - week * oneDay - oneDay);
		startDate.setTime(endDate.getTime() - 6 * oneDay);
		return new Date[] { startDate, endDate };
	}
	
	
	public static Date[] getThisWeek(Date date){
		Date startDate = new Date(date.getTime());
		Date endDate = new Date(date.getTime());
		int week = startDate.getDay();
		week = week == 0 ? 6 : week - 1;
		int oneDay = 1000 * 60 * 60 * 24;
		endDate.setTime(startDate.getTime() + (6 - week) * oneDay);
		startDate.setTime(endDate.getTime() - 6 * oneDay);
		return new Date[]{startDate, endDate};
	}
	
	/**
	 * 传入日期  返回这个星期的第一天后最后一天  以yyyyMMdd形式的int类型返回
	 * @param date
	 * @return
	 */
	public static int[] getThisWeekInt(Date date){
		Date startDate = new Date(date.getTime());
		Date endDate = new Date(date.getTime());
		int week = startDate.getDay();
		week = week == 0 ? 6 : week - 1;
		int oneDay = 1000 * 60 * 60 * 24;
		endDate.setTime(startDate.getTime() + (6 - week) * oneDay);
		startDate.setTime(endDate.getTime() - 6 * oneDay);
		
		return new int[]{getDay(startDate.getTime()), getDay(endDate.getTime())};
	}
	
	/**
	 * 返回具体某个月的00 - 32  
	 * yyyymm00 - yyyymm32
	 * @param date
	 * @return
	 */
	public static int[] getThisMonth(Date date){
		SimpleDateFormat dayFormat = new SimpleDateFormat("yyyyMMdd");
		String mm = dayFormat.format(date); 
		String yyyy = mm.substring(0, 4);
		mm = mm.substring(4, 6);
		return new int[]{Integer.parseInt(yyyy + mm + "00"), Integer.parseInt(yyyy + mm + "32")};
	}
	
	/**
	 * 返回具体某年的00 00 - 13 32
	 * yyyy0000 - yyyy1332
	 * @param date
	 * @return
	 */
	public static int[] getThisYear(Date date){
		SimpleDateFormat dayFormat = new SimpleDateFormat("yyyyMMdd");
		String yyyy = dayFormat.format(date).substring(0, 4);
		return new int[]{Integer.parseInt(yyyy + "00" + "00"), Integer.parseInt(yyyy + "13" + "32")};
	}
  
	public static void main(String[] args) {
		int[] retInt = getThisMonth(new Date());
		for(int tt : retInt){
			System.out.println(tt);
		}
	}
	

}
