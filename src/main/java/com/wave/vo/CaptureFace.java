package com.wave.vo;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.wave.util.Util;

/**  
 * @Title:  CaptureFace.java   
 * @Package com.antelope.vo   
 * @Description:    (人脸)   
 * @author: gaoyun     
 * @edit by: 
 * @date:   2018年5月28日 下午4:09:32   
 * @version V1.0 
 */ 
public class CaptureFace extends CaptureBase<CaptureFace> {
	
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
    
    private Long[] face_tags;
    
    private Double eyeglass;
    
    private Double facemask;
    
    private Double male;
    
    private Double female;
    
    private Double hannation;
    
    private Long is_eyeglass;

    private Long is_facemask;

    private Long is_male;

    private Long is_female;

    private Long is_hannation;

    private Long age;

    private String pose;

    private Double quality;

    private Double confidence;

    private String feature_resource;

    private String feature_version;

    private Double[] face_feature;

    private String face_path;

    private String face_rect;

    private String body_id;
    
    public CaptureFace(boolean empty) {
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

	public Long[] getFace_tags() {
		return face_tags;
	}

	public void setFace_tags(Long[] face_tags) {
		this.face_tags = face_tags;
	}

	public Double getEyeglass() {
		return eyeglass;
	}

	public void setEyeglass(Double eyeglass) {
		this.eyeglass = eyeglass;
	}

	public Double getFacemask() {
		return facemask;
	}

	public void setFacemask(Double facemask) {
		this.facemask = facemask;
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

	public Double getHannation() {
		return hannation;
	}

	public void setHannation(Double hannation) {
		this.hannation = hannation;
	}

	public Long getIs_eyeglass() {
		return is_eyeglass;
	}

	public void setIs_eyeglass(Long is_eyeglass) {
		this.is_eyeglass = is_eyeglass;
	}

	public Long getIs_facemask() {
		return is_facemask;
	}

	public void setIs_facemask(Long is_facemask) {
		this.is_facemask = is_facemask;
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

	public Long getIs_hannation() {
		return is_hannation;
	}

	public void setIs_hannation(Long is_hannation) {
		this.is_hannation = is_hannation;
	}

	public Long getAge() {
		return age;
	}

	public void setAge(Long age) {
		this.age = age;
	}

	public String getPose() {
		return pose;
	}

	public void setPose(String pose) {
		this.pose = pose;
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

	public Double[] getFace_feature() {
		return face_feature;
	}

	public void setFace_feature(Double[] face_feature) {
		this.face_feature = face_feature;
	}

	public String getFace_path() {
		return face_path;
	}

	public void setFace_path(String face_path) {
		this.face_path = face_path;
	}

	public String getFace_rect() {
		return face_rect;
	}

	public void setFace_rect(String face_rect) {
		this.face_rect = face_rect;
	}

	public String getBody_id() {
		return body_id;
	}

	public void setBody_id(String body_id) {
		this.body_id = body_id;
	}
    
    public CaptureFace translate(ResultSet rs) {
    	try {
        	CaptureFace face = new CaptureFace(false);
        	face.setId(rs.getString("id"));
        	face.setCamera_id(rs.getString("camera_id"));
        	face.setCamera_name(rs.getString("camera_name"));
        	face.setCapture_time(rs.getLong("capture_time"));
        	face.setAddress(rs.getString("address"));
        	face.setLongitude(rs.getDouble("longitude"));
        	face.setLatitide(rs.getDouble("latitide"));
        	face.setInstallation_location(Util.getLongArr(rs.getString("installation_location")));
        	face.setOrg_ids(Util.getLongArr(rs.getString("org_ids")));
        	face.setOperation_center_ids(Util.getLongArr(rs.getString("operation_center_ids")));
        	face.setCamera_tags(Util.getLongArr(rs.getString("camera_tags")));
        	face.setScene_path(rs.getString("scene_path"));
        	face.setImg_id(rs.getString("img_id"));
        	face.setVids(Util.getLongArr(rs.getString("vids")));
        	face.setVid_scores(Util.getDoubleArr(rs.getString("vid_scores")));
        	face.setCreate_time(rs.getTimestamp("create_time"));
        	face.setUpdate_time(rs.getTimestamp("update_time"));
        	face.setFace_tags(Util.getLongArr(rs.getString("face_tags")));
        	face.setEyeglass(rs.getDouble("eyeglass"));
        	face.setFacemask(rs.getDouble("facemask"));
        	face.setMale(rs.getDouble("male"));
        	face.setFemale(rs.getDouble("female"));
        	face.setHannation(rs.getDouble("hannation"));
        	face.setIs_eyeglass(rs.getLong("is_eyeglass"));
        	face.setIs_facemask(rs.getLong("is_facemask"));
        	face.setIs_male(rs.getLong("is_male"));
        	face.setIs_female(rs.getLong("is_female"));
        	face.setIs_hannation(rs.getLong("is_hannation"));
        	face.setAge(rs.getLong("age"));
        	face.setPose(rs.getString("pose"));
        	face.setQuality(rs.getDouble("quality"));
        	face.setConfidence(rs.getDouble("confidence"));
        	face.setFeature_resource(rs.getString("feature_resource"));
        	face.setFeature_version(rs.getString("feature_version"));
        	face.setFace_feature(Util.getDoubleArr(rs.getString("face_feature")));
        	face.setFace_path(rs.getString("face_path"));
        	face.setFace_rect(rs.getString("face_rect"));
        	face.setBody_id(rs.getString("body_id"));
        	face.setCapture_time_date(rs.getTimestamp("capture_time_date"));
        	return face;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
    }

	@Override
	public CaptureFace mapRow(ResultSet rs, int index) throws SQLException {
		return translate(rs);
	}
}