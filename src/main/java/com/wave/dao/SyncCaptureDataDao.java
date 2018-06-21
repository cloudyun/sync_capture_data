package com.wave.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.wave.config.SpringUtil;

/**  
 * @Title:  SyncCaptureDataDao.java   
 * @Package com.wave.dao   
 * @Description:    (元数据操作dao)   
 * @author: gaoyun     
 * @edit by: 
 * @date:   2018年6月21日 下午12:24:20   
 * @version V1.0 
 */ 
public class SyncCaptureDataDao {
	
	private JdbcTemplate jdbcTemplate;
	
	private static SyncCaptureDataDao instance;
	
	private SyncCaptureDataDao() {
		jdbcTemplate = (JdbcTemplate) SpringUtil.getBean("jdbcTemplate");
	}
	
	public static SyncCaptureDataDao getInstance() {
		if (instance != null) {
			return instance;
		}
		instance = new SyncCaptureDataDao();
		return instance;
	}
	
	public Map<String, Object> getMinMaxID(String table, String current, String record) {
		StringBuilder build = new StringBuilder();
		build.append("select min(id) as min_id,max(id) as max_id from " + table);
		build.append(" where update_time < '" + current + "'");
		build.append(record == null ? "" : " and update_time > '" + record + "'");
		
		List<Map<String, Object>> list = jdbcTemplate.queryForList(build.toString());
		if (list == null || list.isEmpty()) {
			return null;
		}
		return list.get(0);
	}
	
	public String getRecord(String table) {
		String sql = "select date from public.sync_date_record  where id in "
		+ "(select max(id) from public.sync_date_record where name='" + table + "')";

		List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
		if (list == null || list.isEmpty()) {
			return null;
		}
		return list.get(0).get("date").toString();
	}
	
	public boolean execute(String sql) {
		try {
			jdbcTemplate.execute(sql);
			return true;
		} catch (Exception e) {
			return false;
		}
	}
	
	/**
	 * @param sql
	 * @param rowMapper
	 * @return
	 */
	public <T> List<T> query(String sql, RowMapper<T> rowMapper) {
		return jdbcTemplate.query(sql, rowMapper);
	}
	
	/**
	 * 指定天数据是否成功下载
	 * @param day
	 * @return
	 */
	public boolean isSuccess(String day, String table) {
		String sql = "select * from sync_date_record where day='" + day + "' and name = '" + table + "'";
		List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
		return list != null && !list.isEmpty();
	}
	
	/**
	 * @param day
	 * @param type
	 * @return
	 */
	public List<String> getFilePaths(String day, String table) {
		String sql = "select file from sync_file_record where day='" + day + "' and name = '" + table + "'";
		List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
		if (list == null || list.isEmpty()) {
			return null;
		}
		List<String> paths = new ArrayList<String>();
		for (Map<String, Object> data : list) {
			paths.add(data.get("file").toString());
		}
		return paths;
	}

}