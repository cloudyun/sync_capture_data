package com.wave.orc;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import com.wave.dao.SyncCaptureDataDao;
import com.wave.util.DataUtil;
import com.wave.util.LoadConfig.Config;
import com.wave.vo.CaptureBase;

/**  
 * @Title:  ORCWriter.java   
 * @Package com.antelope.orc   
 * @Description:    (ORC writer)   
 * @author: gaoyun     
 * @edit by: 
 * @date:   2018年5月29日 上午11:28:25   
 * @version V1.0 
 */ 
public class ORCWriter<T extends CaptureBase<T>> {

	protected final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");

	protected final static String NAME_FORMAT = "{0}_{1}.orc";
	
	private SyncCaptureDataDao syncDao = SyncCaptureDataDao.getInstance();
	
	protected String table;
	
	protected String file_path;
	
	protected String name_partition_format;

	protected String path;
	
	protected String current_day;

	protected Long stripeSize;

	protected Integer bufferSize;

	protected Long blockSize;

	protected String charset;

	protected TypeDescription schema;

	protected Writer writer;
	
	protected final static Integer DEFAULT_LENGTH = 16;
	
	protected final static Integer FEATURE_LENGTH = 256;

	public ORCWriter(Config config, String table) {
		path = config.get("orc.data.path");
		stripeSize = Long.parseLong(config.get("orc.stripe.size"));
		bufferSize = Integer.parseInt(config.get("orc.buffer.size"));
		blockSize = Long.parseLong(config.get("orc.block.size"));
		charset = config.get("orc.charset");
		this.table = table;
		this.name_partition_format = table + "_1_prt_{0}/{1}_{2}.orc";
	}

	public void set(BytesColumnVector column, String value, int row) {
		if (value == null) {
			value = "";
//			return;
		}
		try {
			column.setVal(row, value.getBytes(charset));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
	
	public void set(LongColumnVector column, Long value, int row) {
		if (value == null) {
			return;
		}
		column.vector[row] = value;
	}
	
	public void set(DoubleColumnVector column, Double value, int row) {
		if (value == null) {
			return;
		}
		column.vector[row] = value;
	}
	
	public void set(TimestampColumnVector column, Timestamp value, int row) {
		if (value == null) {
			return;
		}
		column.set(row, value);
	}
	
	public void setList(ListColumnVector list, LongColumnVector child, Long[] values, int row) {
		if (values == null) {
			return;
		}
		list.offsets[row] = list.childCount;
		list.lengths[row] = values.length;
		list.childCount += values.length;
		int start = (int) list.offsets[row];
		for (int index = start; index < start + values.length; ++index) {
			Long lv = values[index - start];
			if (lv == null) {
				continue;
			}
			child.vector[index] = lv;
		}
	}
	
	public void setList(ListColumnVector list, DoubleColumnVector child, Double[] values, int row) {
		if (values == null) {
			return;
		}
		list.offsets[row] = list.childCount;
		list.lengths[row] = values.length;
		list.childCount += values.length;
		int start = (int) list.offsets[row];
		for (int index = start; index < start + values.length; ++index) {
			child.vector[index] = values[index - start];
		}
	}
	
	public void setFeature(BytesColumnVector column, Double[] face_feature, int row) {
		column.setVal(row, DataUtil.doubleArr2ByteArr(face_feature));
	}

	public void saveAll(List<T> faces, ReentrantLock lock) {}

	public int getFile_count() {
		return 0;
	}

	public void rebuild(String name) {
		try {
			close();
			build(name);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void build(String name) throws Exception {}

	public void rebuild(String name, Integer partition) {
		try {
			close();
			build(name, partition);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void build(String name, Integer partition) throws Exception {}

	public void close() {
		if (writer != null) {
			try {
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public String getFile_path() {
		return file_path;
	}
	
	/**
	 * 记录文件路径
	 */
	public void saveFilePath() {
		syncDao.execute("INSERT INTO sync_file_record (name, day, file) VALUES ('" + table + "', '" + current_day + "', '" + file_path + "')");
	}
}