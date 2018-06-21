package com.wave.util;

import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**  
 * @Title:  HadoopUtil.java   
 * @Package com.wave.util   
 * @Description:    (hadoop工具类)   
 * @author: gaoyun     
 * @edit by: 
 * @date:   2018年6月21日 下午12:27:22   
 * @version V1.0 
 */ 
public class HadoopUtil {

	private final static Logger LOG = Logger.getLogger(HadoopUtil.class);

	/**
	 * 删除文件
	 * @param paths
	 * @return
	 */
	public static boolean delete(List<String> paths) {
		try {
			FileSystem fs = getFileSystem();
			for (String path : paths) {
				fs.deleteOnExit(new Path(path));
			}
			return true;
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			return false;
		}
	}
	
	/**
	 * 获取hadoop文件系统操作类
	 * @return
	 */
	public static FileSystem getFileSystem() {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			return FileSystem.get(new URI("/"), conf, "gpadmin");
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			return null;
		}
	}
}
