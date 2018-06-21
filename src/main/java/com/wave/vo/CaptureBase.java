package com.wave.vo;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.springframework.jdbc.core.RowMapper;

/**  
 * @Title:  CaptureBase.java   
 * @Package com.wave.vo   
 * @Description:    (抓拍数据实体基类)   
 * @author: gaoyun     
 * @edit by: 
 * @date:   2018年6月11日 上午11:32:12   
 * @version V1.0 
 * @param <T>
 */ 
public class CaptureBase<T> implements RowMapper<T> {
	
	/**
	 * 是否是信号量
	 */
	protected boolean empty;
	
	protected String id;
	
	protected Timestamp capture_time_date;
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Timestamp getCapture_time_date() {
		return capture_time_date;
	}

	public void setCapture_time_date(Timestamp capture_time_date) {
		this.capture_time_date = capture_time_date;
	}

	public CaptureBase(boolean empty) {
		this.empty = empty;
	}
	
	public boolean isEmpty() {
		return empty;
	}
	
	public T translate(ResultSet rs) {
		return null;
	}

	@Override
	public T mapRow(ResultSet rs, int index) throws SQLException {
		return null;
	}
}