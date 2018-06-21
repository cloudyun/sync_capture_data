package com.wave.common;

import com.wave.util.LoadConfig;
import com.wave.util.LoadConfig.Config;

/**  
 * @Title:  Constant.java   
 * @Package com.wave.common   
 * @Description:    (常量)   
 * @author: gaoyun     
 * @edit by: 
 * @date:   2018年6月21日 下午12:23:42   
 * @version V1.0 
 */ 
public class Constant {
	
	public static Config config = LoadConfig.getInstance("task");
	
	public final static String IMG_TABLE_NAME = config.get("orc.capture_img.table");

	public final static String FACE_TABLE_NAME = config.get("orc.capture_face.table");

	public final static String BODY_TABLE_NAME = config.get("orc.capture_body.table");
}
