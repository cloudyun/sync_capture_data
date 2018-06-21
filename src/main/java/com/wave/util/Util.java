package com.wave.util;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import com.wave.util.LoadConfig.Config;
import com.wave.vo.CaptureBase;

/**  
 * @Title:  Util.java   
 * @Package com.antelope.util   
 * @Description:    (工具类)   
 * @author: gaoyun     
 * @edit by: 
 * @date:   2018年5月29日 上午11:28:35   
 * @version V1.0 
 */ 
/**  
 * @Title:  Util.java   
 * @Package com.wave.util   
 * @Description:    (工具类)   
 * @author: gaoyun     
 * @edit by: 
 * @date:   2018年6月5日 上午10:59:32   
 * @version V1.0 
 */ 
public class Util {

	private final static SimpleDateFormat FULL_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");

	private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
	
	/**
	 * 分区开始时间戳
	 */
	public static long start;

	/**
	 * 分区结束时间戳
	 */
	public static long end;
	
	/**
	 * 每个分区的时间戳最大差值
	 */
	public static long section;
	
	/**
	 * 分区数量
	 */
	public static long partition_count;

	/**
	 * 静态加载分区相关信息
	 */
	static {
		Config config = LoadConfig.getInstance("task");
		try {
			start = DATE_FORMAT.parse(config.get("partition.start.date")).getTime();
			end = DATE_FORMAT.parse(config.get("partition.end.date")).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		int day_count = Integer.parseInt(config.get("partition.day.count"));//每个分区天数
		section = day_count * 86400000;
		partition_count = ((end - start) / section) + 1;
	}
	
	public static String getCurrentDay() {
		return DATE_FORMAT.format(new Date());
	}
	
	public static String getCurrentRecord() {
		return FULL_FORMAT.format(new Date());
	}
	
	public static String getDefaultRecord() {
		Calendar instance = Calendar.getInstance();
		instance.add(Calendar.YEAR, -1);
		return FULL_FORMAT.format(instance.getTime());
	}

	public static Long[] getLongArr(String value) {
		if (value == null) {
			return null;
		}
		String[] split = value.replaceAll("\\{|\\}", "").split(",");
		Long[] arr = new Long[split.length];
		for (int x = 0; x < split.length; x++) {
			String sv = split[x];
			if (sv == null || !sv.matches("-?\\d+")) {
				continue;
			}
			arr[x] = Long.parseLong(sv);
		}
		return arr;
	}

	public static Double[] getDoubleArr(String value) {
		if (value == null) {
			return null;
		}
		String[] split = value.replaceAll("\\{|\\}", "").split(",");
		Double[] arr = new Double[split.length];
		for (int x = 0; x < split.length; x++) {
			arr[x] = Double.parseDouble(split[x]);
		}
		return arr;
	}
	
	public static <T extends CaptureBase<T>> String ids(List<T> vos) {
		StringBuilder build = new StringBuilder();
		for (T vo : vos) {
			build.append("'" + vo.getId() + "',");
		}
		return build.toString();
	}
	
	/**
	 * 0 : 表示在本次分区之外
	 * @param ts
	 * @return
	 */
	public static Integer getPartition(Timestamp ts) {
		if (ts == null) {
			return null;
		}
		long time = ts.getTime();
		if (time < start || time >= end) {
			return 0;
		}
		for (int x = 1; x <= partition_count; x++) {
			if (start > time - x * section) {
				return x;
			}
		}
		return 0;
	}
	
	public static String getDate(int x) {
		Calendar instance = Calendar.getInstance();
		instance.add(Calendar.DAY_OF_MONTH, x);
		return DATE_FORMAT.format(instance.getTime());
	}
}