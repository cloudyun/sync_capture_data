package com.wave.vo;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.wave.util.Util;

/**  
 * @Title:  CaptureImg.java   
 * @Package com.antelope.vo   
 * @Description:    (图片)   
 * @author: gaoyun     
 * @edit by: 
 * @date:   2018年5月28日 下午4:09:42   
 * @version V1.0 
 */ 
public class CaptureImg extends CaptureBase<CaptureImg> {
	
    private String camera_id;
    
    private String camera_name;
    
    private Long capture_time;
    
    private String address;
    
    private Double longitude;
    
    private Double latitide;
    
    private Long[] installation_location;
    
    private Long[] org_ids;
    
    private Long[] operation_center_ids;
    
    private Long[] camera_tags;
    
    private String scene_path;
    
    private String face_infos;
    
    private String body_infos;
    
    private Timestamp create_time;
    
    private Timestamp update_time;

    private String file_name;
    
    public CaptureImg(boolean empty) {
		super(empty);
	}

	public String getCamera_id() {
		return camera_id;
	}

	public void setCamera_id(String camera_id) {
		this.camera_id = camera_id;
	}

	public String getCamera_name() {
		return camera_name;
	}

	public void setCamera_name(String camera_name) {
		this.camera_name = camera_name;
	}

	public Long getCapture_time() {
		return capture_time;
	}

	public void setCapture_time(Long capture_time) {
		this.capture_time = capture_time;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public Double getLongitude() {
		return longitude;
	}

	public void setLongitude(Double longitude) {
		this.longitude = longitude;
	}

	public Double getLatitide() {
		return latitide;
	}

	public void setLatitide(Double latitide) {
		this.latitide = latitide;
	}

	public Long[] getInstallation_location() {
		return installation_location;
	}

	public void setInstallation_location(Long[] installation_location) {
		this.installation_location = installation_location;
	}

	public Long[] getOrg_ids() {
		return org_ids;
	}

	public void setOrg_ids(Long[] org_ids) {
		this.org_ids = org_ids;
	}

	public Long[] getOperation_center_ids() {
		return operation_center_ids;
	}

	public void setOperation_center_ids(Long[] operation_center_ids) {
		this.operation_center_ids = operation_center_ids;
	}

	public Long[] getCamera_tags() {
		return camera_tags;
	}

	public void setCamera_tags(Long[] camera_tags) {
		this.camera_tags = camera_tags;
	}

	public String getScene_path() {
		return scene_path;
	}

	public void setScene_path(String scene_path) {
		this.scene_path = scene_path;
	}

	public Timestamp getCreate_time() {
		return create_time;
	}

	public void setCreate_time(Timestamp create_time) {
		this.create_time = create_time;
	}

	public Timestamp getUpdate_time() {
		return update_time;
	}

	public void setUpdate_time(Timestamp update_time) {
		this.update_time = update_time;
	}
    
	public String getFace_infos() {
		return face_infos;
	}

	public void setFace_infos(String face_infos) {
		this.face_infos = face_infos;
	}

	public String getBody_infos() {
		return body_infos;
	}

	public void setBody_infos(String body_infos) {
		this.body_infos = body_infos;
	}

	public String getFile_name() {
		return file_name;
	}

	public void setFile_name(String file_name) {
		this.file_name = file_name;
	}

	public CaptureImg translate(ResultSet rs) {
    	try {
    		CaptureImg ing = new CaptureImg(false);
        	ing.setId(rs.getString("id"));
        	ing.setCamera_id(rs.getString("camera_id"));
        	ing.setCamera_name(rs.getString("camera_name"));
        	ing.setCapture_time(rs.getLong("capture_time"));
        	ing.setAddress(rs.getString("address"));
        	ing.setLongitude(rs.getDouble("longitude"));
        	ing.setLatitide(rs.getDouble("latitide"));
        	ing.setInstallation_location(Util.getLongArr(rs.getString("installation_location")));
        	ing.setOrg_ids(Util.getLongArr(rs.getString("org_ids")));
        	ing.setOperation_center_ids(Util.getLongArr(rs.getString("operation_center_ids")));
        	ing.setCamera_tags(Util.getLongArr(rs.getString("camera_tags")));
        	ing.setScene_path(rs.getString("scene_path"));
        	ing.setFace_infos(rs.getString("face_infos"));
        	ing.setBody_infos(rs.getString("body_infos"));
        	ing.setCreate_time(rs.getTimestamp("create_time"));
        	ing.setUpdate_time(rs.getTimestamp("update_time"));
        	ing.setFile_name(rs.getString("file_name"));
        	ing.setCapture_time_date(rs.getTimestamp("capture_time_date"));
        	return ing;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
    }

	@Override
	public CaptureImg mapRow(ResultSet rs, int index) throws SQLException {
		return translate(rs);
	}
}