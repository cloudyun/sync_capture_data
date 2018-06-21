package com.wave.task;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.wave.common.Constant;
import com.wave.dao.SyncCaptureDataDao;
import com.wave.orc.CaptureBodyWriter;
import com.wave.orc.CaptureFaceWriter;
import com.wave.orc.CaptureImgWriter;
import com.wave.orc.ORCWriter;
import com.wave.util.HadoopUtil;
import com.wave.util.LoadConfig;
import com.wave.util.LoadConfig.Config;
import com.wave.util.Util;
import com.wave.vo.CaptureBase;
import com.wave.vo.CaptureBody;
import com.wave.vo.CaptureFace;
import com.wave.vo.CaptureImg;

/**  
 * @Title:  SyncCaptureBase.java   
 * @Package com.wave.task   
 * @Description:    (同步数据基类)   
 * @author: gaoyun     
 * @edit by: 
 * @date:   2018年5月30日 下午3:36:43   
 * @version V1.0 
 */ 
public class SyncCaptureBase {

	private final static Logger LOG = Logger.getLogger(SyncCaptureBase.class);
	
	private SyncCaptureDataDao syncDao = SyncCaptureDataDao.getInstance();
	
	protected String table;
	
	protected String min_id;
	
	protected String max_id;
	
	protected String record;
	
	protected String current_day;
	
	protected String current;
	
	protected int file_count = 0;
	
	protected Config config = LoadConfig.getInstance("task");
	
	protected int batch = 2048;
	
	protected AtomicBoolean success = new AtomicBoolean(false);

	public void setTable(String table) {
		this.table = table;
	}
	
	protected boolean generate() {
		Map<String, Object> data = syncDao.getMinMaxID(table, current, record);
		if (data == null || data.isEmpty()) {
			return false;
		}
		Object min_id_object = data.get("min_id");
		Object max_id_object = data.get("max_id");
		if (min_id_object != null && max_id_object != null) {
			min_id = min_id_object.toString();
			max_id = max_id_object.toString();
			return true;
		}
		return false;
	}
	
	/**
	 * @param table
	 * @param current
	 * @param record
	 * @param id
	 * @return
	 */
	protected String getQuerySql(String id, boolean first) {
		StringBuilder build = new StringBuilder();
		build.append("select * from " + table + " where ");
		build.append("id " + (first ? ">=" : ">") + " '" + id + "' and id <= '" + max_id + "' ");
		build.append("and update_time < '" + current + "'");
		build.append(record == null ? "" : " and update_time > '" + record + "'");
		build.append(" order by id asc limit " + batch);
		String sql = build.toString();
//		LOG.info("capture_" + type + " >> sql : " + sql);
		return sql;
	}
	
	/**
	 * @param table
	 * @return
	 */
	protected String getQuerySql() {
		String sql = "";
		if (record == null) {
			sql = "select * from " + table + " where update_time < '" + current + "'";
		} else {
			sql = "select * from " + table + " where update_time > '" + record + "' and update_time < '" + current + "'";
		}
		return sql;
	}

	/**
	 * @return
	 */
	protected String getRecord() {
		return syncDao.getRecord(table);
	}

	/**
	 * @param record
	 */
	protected void svaeRecord(String record) {
		String sql = "INSERT INTO public.sync_date_record(name, date, day, success, file_count) VALUES "
		+ "('" + table + "', '" + record + "', '" + current_day + "', 1, " + file_count + ")";
		syncDao.execute(sql);
	}
	
	/**  
	 * @Title:  SyncCaptureBase.java   
	 * @Package com.wave.task   
	 * @Description:    (查询线程)   
	 * @author: gaoyun     
	 * @edit by: 
	 * @date:   2018年6月1日 上午11:15:12   
	 * @version V1.0 
	 */ 
	class QueryThread<T extends CaptureBase<T>> extends Thread {
		
		private BlockingQueue<T> queue;
		
		private T empty;
		
		public QueryThread(BlockingQueue<T> queue, T empty) {
			this.queue = queue;
			this.empty = empty;
		}
		
		@Override
		public void run() {
			try {
				if (!generate()) {
					LOG.info("昨日无数据");
					queue.put(empty);
					return;
				}
				String sql = getQuerySql(min_id, true);
				int total = 0;
				CaptureBase<T> rowMapper = getRowMapper();
				while (true) {
					List<T> list = syncDao.query(sql, rowMapper);
					String id = null;
					for (T data : list) {
						if (data == null) {
							continue;
						}
						id = data.getId();
						queue.put(data);
					}
					total += list.size();
					if (total % (5 * batch) == 0) {
						LOG.info("" + table + " total : " + total);
					}
					if (list.size() < batch) {
						LOG.info("" + table + " : 最后一批次，跳出查询");
						break;
					}
					sql = getQuerySql(id, false);
				}
				LOG.info("" + table + " : 读取pg库数据线程结束!!!");
				success.set(true);
				queue.put(empty);
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	public <T extends CaptureBase<T>> CaptureBase<T> getRowMapper() {
		if (Constant.IMG_TABLE_NAME.equals(table)) {
			return (CaptureBase<T>) new CaptureImg(true);
		}
		if (Constant.FACE_TABLE_NAME.equals(table)) {
			return (CaptureBase<T>) new CaptureFace(true);
		}
		if (Constant.BODY_TABLE_NAME.equals(table)) {
			return (CaptureBase<T>) new CaptureBody(true);
		}
		return null;
	}
	
	/**  
	 * @Title:  SyncCaptureBase.java   
	 * @Package com.wave.task   
	 * @Description:    (带分区的写线程)   
	 * @author: gaoyun     
	 * @edit by: 
	 * @date:   2018年6月1日 上午11:35:01   
	 * @version V1.0 
	 */ 
	class WritesThread<T extends CaptureBase<T>> extends Thread {
		
		private BlockingQueue<T> queue;
		
		private Map<Integer, ORCWriter<T>> writers;
		
		private Map<Integer, List<T>> lists;
		
		private Map<Integer, Integer> counts;
		
		private ReentrantLock lock;
		
		public WritesThread(BlockingQueue<T> queue, ReentrantLock lock) {
			this.queue = queue;
			this.lock = lock;
			this.lists = new HashMap<Integer, List<T>>();
			this.counts = new HashMap<Integer, Integer>();
			this.writers = new HashMap<Integer, ORCWriter<T>>();
		}

		@Override
		public void run() {
			try {
				while (true) {
					T take = queue.take();
					if (take.isEmpty()) {
						queue.put(take);
						break;
					}
					Timestamp ct = take.getCapture_time_date();
					Integer partition = Util.getPartition(ct);
					if (partition == null) {
						continue;
					}
					List<T> list = lists.get(partition);
					if (list == null) {
						list = new ArrayList<T>();
					}
					list.add(take);
					
					ORCWriter<T> writer = writers.get(partition);
					if (writer == null) {
						writer = getWriter(getName(), partition);
						file_count++;
					}
					if (list.size() >= 1024) {
						writer.saveAll(list, lock);
						list = new ArrayList<T>();
					}
					Integer count = counts.get(partition);
					count = count == null ? 1 : count + 1;
					if (writer != null && count >= writer.getFile_count()) {
						writer.rebuild(getName(), partition);
						count = 0;
					}
					lists.put(partition, list);
					counts.put(partition, count);
					writers.put(partition, writer);
				}
				Set<Integer> partitions = lists.keySet();
				for (Integer partition : partitions) {
					List<T> list = lists.get(partition);
					if (list == null || list.size() <= 0) {
						continue;
					}
					ORCWriter<T> writer = writers.get(partition);
					if (writer == null) {
						writer = getWriter(getName(), partition);
					}
					writer.saveAll(list, lock);
					writer.close();
				}
			} catch (InterruptedException e) {
				LOG.error(e.getMessage(), e);
			}
		}
		
		
	}
	
	/**  
	 * @Title:  SyncCaptureBase.java   
	 * @Package com.wave.task   
	 * @Description:    (不带分区的写线程)   
	 * @author: gaoyun     
	 * @edit by: 
	 * @date:   2018年6月1日 上午11:35:01   
	 * @version V1.0 
	 */ 
	class WriteThread<T extends CaptureBase<T>> extends Thread {
		
		private BlockingQueue<T> queue;
		
		private ORCWriter<T> writer;
		
		private ReentrantLock lock;
		
		public WriteThread(BlockingQueue<T> queue, ReentrantLock lock, String name) {
			this.queue = queue;
			this.writer = getWriter(name);
			this.lock = lock;
		}

		@Override
		public void run() {
			try {
				List<T> vos = new ArrayList<T>();
				int count = 1;
				while (true) {
					T take = queue.take();
					if (take.isEmpty()) {
						queue.put(take);
						break;
					}
					vos.add(take);
					if (vos.size() >= 1024) {
						writer.saveAll(vos, lock);
						vos = new ArrayList<T>();
					}
					if (count++ % writer.getFile_count() == 0) {
						writer.rebuild(this.getName());
					}
				}
				if (vos.size() > 0) {
					writer.saveAll(vos, lock);
					vos = new ArrayList<T>();
				}
				writer.close();
			} catch (InterruptedException e) {
				LOG.error(e.getMessage(), e);
			}
		}
	}
	
	public static void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
		}
	}
	
	/**
	 * @param name 线程名称
	 * @param partition 分区
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <T extends CaptureBase<T>> ORCWriter<T> getWriter(String name, Integer partition) {
		if (Constant.IMG_TABLE_NAME.equals(table)) {
			return (ORCWriter<T>) new CaptureImgWriter(config, name, current_day, partition);
		}
		if (Constant.FACE_TABLE_NAME.equals(table)) {
			return (ORCWriter<T>) new CaptureFaceWriter(config, name, current_day, partition);
		}
		if (Constant.BODY_TABLE_NAME.equals(table)) {
			return (ORCWriter<T>) new CaptureBodyWriter(config, name, current_day, partition);
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public <T extends CaptureBase<T>> ORCWriter<T> getWriter(String name) {
		if (Constant.IMG_TABLE_NAME.equals(table)) {
			return (ORCWriter<T>) new CaptureImgWriter(config, name);
		}
		if (Constant.FACE_TABLE_NAME.equals(table)) {
			return (ORCWriter<T>) new CaptureFaceWriter(config, name);
		}
		if (Constant.BODY_TABLE_NAME.equals(table)) {
			return (ORCWriter<T>) new CaptureBodyWriter(config, name);
		}
		return null;
	}
	
	/**
	 * 备份并删除数据
	 * @param day
	 * @return
	 */
	public boolean backupAndDelete(int day) {
		try {
			String ENDTIME = Util.getDate(day).replaceAll("-", "");
			String DELTIME = Util.getDate(0).replaceAll("-", "");
			String backup = "select * into " + table + "_" + ENDTIME + "_" + DELTIME + " from " + table + " where update_time < '" + ENDTIME + "';";
			String delete = "delete from " + table + " where update_time < '" + ENDTIME + "';";
			System.out.println(backup + delete);
//			syncDao.execute(backup + delete);
			return true;
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			return false;
		}
	}
	
	/**
	 * 判断当天数据是否成功下载
	 * @return
	 */
	protected boolean isSuccess() {
		return syncDao.isSuccess(current_day, table);
	}
	
	protected List<String> getFilePaths() {
		return syncDao.getFilePaths(current_day, table);
	}
	
	protected boolean delete(List<String> paths) {
		return HadoopUtil.delete(paths);
	}
}