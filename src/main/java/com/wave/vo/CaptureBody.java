package com.wave.vo;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.wave.util.Util;

/**  
 * @Title:  CaptureBody.java   
 * @Package com.antelope.vo   
 * @Description:    (人体)   
 * @author: gaoyun     
 * @edit by: 
 * @date:   2018年5月28日 下午4:09:03   
 * @version V1.0 
 */ 
public class CaptureBody extends CaptureBase<CaptureBody> {
	
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
    
    private String img_id;
    
    private Long[] vids;
    
    private Double[] vid_scores;
    
    private Timestamp create_time;
    
    private Timestamp update_time;
    
    private Long[] body_tags;
    
    private Double female;
    
    private Double male;
    
    private Double hanNation;
    
    private Long is_female;

    private Long is_male;

    private Long is_hanNation;

    private Double quality;

    private Double confidence;

    private String feature_resource;

    private String feature_version;

    private Double[] body_feature;

    private String body_path;

    private String body_rect;

    private String face_id;
    
    private Long pq_code;
	
	public CaptureBody(boolean empty) {
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

	public String getImg_id() {
		return img_id;
	}

	public void setImg_id(String img_id) {
		this.img_id = img_id;
	}

	public Long[] getVids() {
		return vids;
	}

	public void setVids(Long[] vids) {
		this.vids = vids;
	}

	public Double[] getVid_scores() {
		return vid_scores;
	}

	public void setVid_scores(Double[] vid_scores) {
		this.vid_scores = vid_scores;
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

	public Long[] getBody_tags() {
		return body_tags;
	}

	public void setBody_tags(Long[] body_tags) {
		this.body_tags = body_tags;
	}

	public Double getMale() {
		return male;
	}

	public void setMale(Double male) {
		this.male = male;
	}

	public Double getFemale() {
		return female;
	}

	public void setFemale(Double female) {
		this.female = female;
	}

	public Double getHanNation() {
		return hanNation;
	}

	public void setHanNation(Double hanNation) {
		this.hanNation = hanNation;
	}

	public Long getIs_male() {
		return is_male;
	}

	public void setIs_male(Long is_male) {
		this.is_male = is_male;
	}

	public Long getIs_female() {
		return is_female;
	}

	public void setIs_female(Long is_female) {
		this.is_female = is_female;
	}

	public Long getIs_hanNation() {
		return is_hanNation;
	}

	public void setIs_hanNation(Long is_hanNation) {
		this.is_hanNation = is_hanNation;
	}

	public Double getQuality() {
		return quality;
	}

	public void setQuality(Double quality) {
		this.quality = quality;
	}

	public Double getConfidence() {
		return confidence;
	}

	public void setConfidence(Double confidence) {
		this.confidence = confidence;
	}

	public String getFeature_resource() {
		return feature_resource;
	}

	public void setFeature_resource(String feature_resource) {
		this.feature_resource = feature_resource;
	}

	public String getFeature_version() {
		return feature_version;
	}

	public void setFeature_version(String feature_version) {
		this.feature_version = feature_version;
	}

	public Double[] getBody_feature() {
		return body_feature;
	}

	public void setBody_feature(Double[] body_feature) {
		this.body_feature = body_feature;
	}

	public String getBody_path() {
		return body_path;
	}

	public void setBody_path(String body_path) {
		this.body_path = body_path;
	}

	public String getBody_rect() {
		return body_rect;
	}

	public void setBody_rect(String body_rect) {
		this.body_rect = body_rect;
	}

	public String getFace_id() {
		return face_id;
	}

	public void setFace_id(String face_id) {
		this.face_id = face_id;
	}

	public Long getPq_code() {
		return pq_code;
	}

	public void setPq_code(Long pq_code) {
		this.pq_code = pq_code;
	}
	
	@Override
	public CaptureBody mapRow(ResultSet rs, int index) throws SQLException {
		return translate(rs);
	}

	public CaptureBody translate(ResultSet rs) {
    	try {
    		CaptureBody body = new CaptureBody(false);
        	body.setId(rs.getString("id"));
        	body.setCamera_id(rs.getString("camera_id"));
        	body.setCamera_name(rs.getString("camera_name"));
        	body.setCapture_time(rs.getLong("capture_time"));
        	body.setAddress(rs.getString("address"));
        	body.setLongitude(rs.getDouble("longitude"));
        	body.setLatitide(rs.getDouble("latitide"));
        	body.setInstallation_location(Util.getLongArr(rs.getString("installation_location")));
        	body.setOrg_ids(Util.getLongArr(rs.getString("org_ids")));
        	body.setOperation_center_ids(Util.getLongArr(rs.getString("operation_center_ids")));
        	body.setCamera_tags(Util.getLongArr(rs.getString("camera_tags")));
        	body.setScene_path(rs.getString("scene_path"));
        	body.setImg_id(rs.getString("img_id"));
        	body.setVids(Util.getLongArr(rs.getString("vids")));
        	body.setVid_scores(Util.getDoubleArr(rs.getString("vid_scores")));
        	body.setCreate_time(rs.getTimestamp("create_time"));
        	body.setUpdate_time(rs.getTimestamp("update_time"));
        	body.setBody_tags(Util.getLongArr(rs.getString("body_tags")));
        	body.setFemale(rs.getDouble("female"));
        	body.setMale(rs.getDouble("male"));
        	body.setHanNation(rs.getDouble("hannation"));
        	body.setIs_female(rs.getLong("is_female"));
        	body.setIs_male(rs.getLong("is_male"));
        	body.setIs_hanNation(rs.getLong("is_hannation"));
        	body.setQuality(rs.getDouble("quality"));
        	body.setConfidence(rs.getDouble("confidence"));
        	body.setFeature_resource(rs.getString("feature_resource"));
        	body.setFeature_version(rs.getString("feature_version"));
        	body.setBody_feature(Util.getDoubleArr(rs.getString("body_feature")));
        	body.setBody_path(rs.getString("body_path"));
        	body.setBody_rect(rs.getString("body_rect"));
        	body.setFace_id(rs.getString("face_id"));
        	body.setCapture_time_date(rs.getTimestamp("capture_time_date"));
        	body.setPq_code(rs.getLong("pq_code"));
        	return body;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
    }
}