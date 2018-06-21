package com.wave.orc;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.log4j.Logger;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFile.WriterOptions;
import org.apache.orc.TypeDescription;

import com.wave.common.Constant;
import com.wave.util.LoadConfig.Config;
import com.wave.util.Util;
import com.wave.vo.CaptureBody;

/**  
 * @Title:  CaptureBodyWriter.java   
 * @Package com.antelope.orc   
 * @Description:    (写人体数据)   
 * @author: gaoyun     
 * @edit by: 
 * @date:   2018年5月29日 上午10:14:32   
 * @version V1.0 
 */ 
public class CaptureBodyWriter extends ORCWriter<CaptureBody> {

	private final static Logger LOG = Logger.getLogger(CaptureBodyWriter.class);

	private int file_count;
	
	public CaptureBodyWriter(Config config, String name) {
		super(config, Constant.BODY_TABLE_NAME);
		file_count = Integer.parseInt(config.get("orc." + Constant.BODY_TABLE_NAME + ".count.prefile"));
		try {
			build(name);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @param config 配置
	 * @param name 线程名称
	 * @param current_day 当前天日期
	 * @param partition 分区
	 */
	public CaptureBodyWriter(Config config, String name, String current_day, Integer partition) {
		super(config, Constant.BODY_TABLE_NAME);
		file_count = Integer.parseInt(config.get("orc." + Constant.BODY_TABLE_NAME + ".count.prefile"));
		this.current_day = current_day;
		try {
			build(name, partition);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void build(String name) throws Exception {
		schema = TypeDescription.createStruct()
				.addField("id", TypeDescription.createVarchar().withMaxLength(32))
				.addField("camera_id", TypeDescription.createVarchar().withMaxLength(32))
				.addField("camera_name", TypeDescription.createVarchar().withMaxLength(255))
				.addField("capture_time", TypeDescription.createLong())
				.addField("address", TypeDescription.createVarchar().withMaxLength(255))
				.addField("longitude", TypeDescription.createDouble())
				.addField("latitide", TypeDescription.createDouble())
				.addField("installation_location", TypeDescription.createList(TypeDescription.createLong()))
				.addField("org_ids", TypeDescription.createList(TypeDescription.createLong()))
				.addField("operation_center_ids", TypeDescription.createList(TypeDescription.createLong()))
				.addField("camera_tags", TypeDescription.createList(TypeDescription.createLong()))
				.addField("scene_path", TypeDescription.createVarchar().withMaxLength(512))
				.addField("img_id", TypeDescription.createVarchar().withMaxLength(32))
				.addField("vids", TypeDescription.createList(TypeDescription.createLong()))
				.addField("vid_scores", TypeDescription.createList(TypeDescription.createDouble()))
				.addField("create_time", TypeDescription.createTimestamp())
				.addField("update_time", TypeDescription.createTimestamp())
				.addField("body_tags", TypeDescription.createList(TypeDescription.createLong()))
				.addField("female", TypeDescription.createDouble())
				.addField("male", TypeDescription.createDouble())
				.addField("hanNation", TypeDescription.createDouble())
				.addField("is_female", TypeDescription.createLong())
				.addField("is_male", TypeDescription.createLong())
				.addField("is_hanNation", TypeDescription.createLong())
				.addField("quality", TypeDescription.createDouble())
				.addField("confidence", TypeDescription.createDouble())
				.addField("feature_resource", TypeDescription.createVarchar().withMaxLength(255))
				.addField("feature_version", TypeDescription.createVarchar().withMaxLength(255))
				.addField("body_feature", TypeDescription.createBinary())
				.addField("body_path", TypeDescription.createVarchar().withMaxLength(512))
				.addField("body_rect", TypeDescription.createVarchar().withMaxLength(255))
				.addField("face_id", TypeDescription.createVarchar().withMaxLength(32))
				.addField("capture_time_date", TypeDescription.createTimestamp())
				.addField("pq_code", TypeDescription.createLong());
		
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		FileSystem fs = FileSystem.get(new URI("/"), conf, "gpadmin");
		WriterOptions options = OrcFile.writerOptions(conf)
				.fileSystem(fs)
				.setSchema(schema)
				.stripeSize(stripeSize)
				.bufferSize(bufferSize)
				.blockSize(blockSize)
				.compress(CompressionKind.ZLIB)
				.version(OrcFile.Version.V_0_12);
		file_path = path + table + "/" + MessageFormat.format(NAME_FORMAT, name, DATE_FORMAT.format(new Date()));
		Path dataPath = new Path(file_path);
		writer = OrcFile.createWriter(dataPath, options);
	}

	@Override
	public void build(String name, Integer partition) throws Exception {
		schema = TypeDescription.createStruct()
				.addField("id", TypeDescription.createVarchar().withMaxLength(32))
				.addField("camera_id", TypeDescription.createVarchar().withMaxLength(32))
				.addField("camera_name", TypeDescription.createVarchar().withMaxLength(255))
				.addField("capture_time", TypeDescription.createLong())
				.addField("address", TypeDescription.createVarchar().withMaxLength(255))
				.addField("longitude", TypeDescription.createDouble())
				.addField("latitide", TypeDescription.createDouble())
				.addField("installation_location", TypeDescription.createList(TypeDescription.createLong()))
				.addField("org_ids", TypeDescription.createList(TypeDescription.createLong()))
				.addField("operation_center_ids", TypeDescription.createList(TypeDescription.createLong()))
				.addField("camera_tags", TypeDescription.createList(TypeDescription.createLong()))
				.addField("scene_path", TypeDescription.createVarchar().withMaxLength(512))
				.addField("img_id", TypeDescription.createVarchar().withMaxLength(32))
				.addField("vids", TypeDescription.createList(TypeDescription.createLong()))
				.addField("vid_scores", TypeDescription.createList(TypeDescription.createDouble()))
				.addField("create_time", TypeDescription.createTimestamp())
				.addField("update_time", TypeDescription.createTimestamp())
				.addField("body_tags", TypeDescription.createList(TypeDescription.createLong()))
				.addField("female", TypeDescription.createDouble())
				.addField("male", TypeDescription.createDouble())
				.addField("hanNation", TypeDescription.createDouble())
				.addField("is_female", TypeDescription.createLong())
				.addField("is_male", TypeDescription.createLong())
				.addField("is_hanNation", TypeDescription.createLong())
				.addField("quality", TypeDescription.createDouble())
				.addField("confidence", TypeDescription.createDouble())
				.addField("feature_resource", TypeDescription.createVarchar().withMaxLength(255))
				.addField("feature_version", TypeDescription.createVarchar().withMaxLength(255))
				.addField("body_feature", TypeDescription.createBinary())
				.addField("body_path", TypeDescription.createVarchar().withMaxLength(512))
				.addField("body_rect", TypeDescription.createVarchar().withMaxLength(255))
				.addField("face_id", TypeDescription.createVarchar().withMaxLength(32))
				.addField("capture_time_date", TypeDescription.createTimestamp())
				.addField("pq_code", TypeDescription.createLong());
		
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		FileSystem fs = FileSystem.get(new URI("/"), conf, "gpadmin");
		WriterOptions options = OrcFile.writerOptions(conf)
				.fileSystem(fs)
				.setSchema(schema)
				.stripeSize(stripeSize)
				.bufferSize(bufferSize)
				.blockSize(blockSize)
				.compress(CompressionKind.ZLIB)
				.version(OrcFile.Version.V_0_12);
		file_path = path + table + "/" + MessageFormat.format(name_partition_format, partition, name, DATE_FORMAT.format(new Date()));
		saveFilePath();
//		System.out.println("build name : " + name + " partition : " + partition + " file_path : " + file_path);
		
		Path dataPath = new Path(file_path);
		writer = OrcFile.createWriter(dataPath, options);
	}

	@Override
	public void saveAll(List<CaptureBody> bodys, ReentrantLock lock) {
		lock.lock();
		try {
			VectorizedRowBatch batch = schema.createRowBatch();
			final int BATCH_SIZE = batch.getMaxSize();
			batch.setPartitionInfo(33, 32);
			BytesColumnVector id = (BytesColumnVector) batch.cols[0];
			BytesColumnVector camera_id = (BytesColumnVector) batch.cols[1];
			BytesColumnVector camera_name = (BytesColumnVector) batch.cols[2];
			LongColumnVector capture_time = (LongColumnVector) batch.cols[3];
			BytesColumnVector address = (BytesColumnVector) batch.cols[4];
			DoubleColumnVector longitude = (DoubleColumnVector) batch.cols[5];
			DoubleColumnVector latitide = (DoubleColumnVector) batch.cols[6];
			ListColumnVector installation_location = (ListColumnVector) batch.cols[7];
			LongColumnVector installation_location_child = (LongColumnVector) installation_location.child;
			installation_location_child.ensureSize(DEFAULT_LENGTH * BATCH_SIZE, false);
			ListColumnVector org_ids = (ListColumnVector) batch.cols[8];
			LongColumnVector org_ids_child = (LongColumnVector) org_ids.child;
			org_ids_child.ensureSize(DEFAULT_LENGTH * BATCH_SIZE, false);
			ListColumnVector operation_center_ids = (ListColumnVector) batch.cols[9];
			LongColumnVector operation_center_ids_child = (LongColumnVector) operation_center_ids.child;
			operation_center_ids_child.ensureSize(DEFAULT_LENGTH * BATCH_SIZE, false);
			ListColumnVector camera_tags = (ListColumnVector) batch.cols[10];
			LongColumnVector camera_tags_child = (LongColumnVector) camera_tags.child;
			camera_tags_child.ensureSize(DEFAULT_LENGTH * BATCH_SIZE, false);
			BytesColumnVector scene_path = (BytesColumnVector) batch.cols[11];
			BytesColumnVector img_id = (BytesColumnVector) batch.cols[12];
			ListColumnVector vids = (ListColumnVector) batch.cols[13];
			LongColumnVector vids_child = (LongColumnVector) vids.child;
			vids_child.ensureSize(DEFAULT_LENGTH * BATCH_SIZE, false);
			ListColumnVector vid_scores = (ListColumnVector) batch.cols[14];
			DoubleColumnVector vid_scores_child = (DoubleColumnVector) vid_scores.child;
			vid_scores_child.ensureSize(DEFAULT_LENGTH * BATCH_SIZE, false);
			TimestampColumnVector create_time = (TimestampColumnVector) batch.cols[15];
			TimestampColumnVector update_time = (TimestampColumnVector) batch.cols[16];
			ListColumnVector body_tags = (ListColumnVector) batch.cols[17];
			LongColumnVector body_tags_child = (LongColumnVector) body_tags.child;
			body_tags_child.ensureSize(DEFAULT_LENGTH * BATCH_SIZE, false);
			DoubleColumnVector female = (DoubleColumnVector) batch.cols[18];
			DoubleColumnVector male = (DoubleColumnVector) batch.cols[19];
			DoubleColumnVector hanNation = (DoubleColumnVector) batch.cols[20];
			LongColumnVector is_female = (LongColumnVector) batch.cols[21];
			LongColumnVector is_male = (LongColumnVector) batch.cols[22];
			LongColumnVector is_hanNation = (LongColumnVector) batch.cols[23];
			DoubleColumnVector quality = (DoubleColumnVector) batch.cols[24];
			DoubleColumnVector confidence = (DoubleColumnVector) batch.cols[25];
			BytesColumnVector feature_resource = (BytesColumnVector) batch.cols[26];
			BytesColumnVector feature_version = (BytesColumnVector) batch.cols[27];
			BytesColumnVector body_feature = (BytesColumnVector) batch.cols[28];
			BytesColumnVector body_path = (BytesColumnVector) batch.cols[29];
			BytesColumnVector body_rect = (BytesColumnVector) batch.cols[30];
			BytesColumnVector face_id = (BytesColumnVector) batch.cols[31];
			TimestampColumnVector capture_time_date = (TimestampColumnVector) batch.cols[32];
			LongColumnVector pq_code = (LongColumnVector) batch.cols[33];
			for (CaptureBody body : bodys) {
				int row = batch.size++;
				set(id, body.getId(), row);
				set(camera_id, body.getCamera_id(), row);
				set(camera_name, body.getCamera_name(), row);
				set(capture_time, body.getCapture_time(), row);
				set(address, body.getAddress(), row);
				set(longitude, body.getLongitude(), row);
				set(latitide, body.getLatitide(), row);
				setList(installation_location, installation_location_child, body.getInstallation_location(), row);
				setList(org_ids, org_ids_child, body.getOrg_ids(), row);
				setList(operation_center_ids, operation_center_ids_child, body.getOperation_center_ids(), row);
				setList(camera_tags, camera_tags_child, body.getCamera_tags(), row);
				set(scene_path, body.getScene_path(), row);
				set(img_id, body.getImg_id(), row);
				setList(vids, vids_child, body.getVids(), row);
				setList(vid_scores, vid_scores_child, body.getVid_scores(), row);
				set(create_time, body.getCreate_time(), row);
				set(update_time, body.getUpdate_time(), row);
				setList(body_tags, body_tags_child, body.getBody_tags(), row);
				set(male, body.getMale(), row);
				set(female, body.getFemale(), row);
				set(hanNation, body.getHanNation(), row);
				set(is_male, body.getIs_male(), row);
				set(is_female, body.getIs_female(), row);
				set(is_hanNation, body.getIs_hanNation(), row);
				set(quality, body.getQuality(), row);
				set(confidence, body.getConfidence(), row);
				set(feature_resource, body.getFeature_resource(), row);
				set(feature_version, body.getFeature_version(), row);
				setFeature(body_feature, body.getBody_feature(), row);
				set(body_path, body.getBody_path(), row);
				set(body_rect, body.getBody_rect(), row);
				set(face_id, body.getFace_id(), row);
				set(capture_time_date, body.getCapture_time_date(), row);
				set(pq_code, body.getPq_code(), row);
				if (row == BATCH_SIZE - 1) {
					try {
						writer.addRowBatch(batch);
					} catch (Exception e) {
						LOG.error(table + "批量写入失败!failed ids : [" + Util.ids(bodys) + "]", e);
					}
					batch.reset();
				}
			}
			if (batch.size != 0) {
				writer.addRowBatch(batch);
				batch.reset();
			}
		} catch (IOException e) {
			LOG.error(table + "批量写入失败!failed ids : [" + Util.ids(bodys) + "]", e);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public int getFile_count() {
		return file_count;
	}
}