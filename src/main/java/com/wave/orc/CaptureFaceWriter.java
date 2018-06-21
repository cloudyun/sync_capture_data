package com.wave.orc;

import java.io.IOException;
import java.net.URI;
import java.text.MessageFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

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
import com.wave.vo.CaptureFace;

/**  
 * @Title:  CaptureFaceWriter.java   
 * @Package com.antelope.orc   
 * @Description:    (写人脸数据)   
 * @author: gaoyun     
 * @edit by: 
 * @date:   2018年5月29日 上午10:14:32   
 * @version V1.0 
 */ 
public class CaptureFaceWriter extends ORCWriter<CaptureFace> {

	private final static Logger LOG = Logger.getLogger(CaptureFaceWriter.class);

	private int file_count;
	
	public CaptureFaceWriter(Config config, String name) {
		super(config, Constant.FACE_TABLE_NAME);
		file_count = Integer.parseInt(config.get("orc." + Constant.FACE_TABLE_NAME + ".count.prefile"));
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
	public CaptureFaceWriter(Config config, String name, String current_day, Integer partition) {
		super(config, Constant.FACE_TABLE_NAME);
		file_count = Integer.parseInt(config.get("orc." + Constant.FACE_TABLE_NAME + ".count.prefile"));
		this.current_day = current_day;
		try {
			build(name, partition);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void build(String name) throws Exception {
		schema = TypeDescription.createStruct().addField("id", TypeDescription.createVarchar().withMaxLength(32))
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
				.addField("face_tags", TypeDescription.createList(TypeDescription.createLong()))
				.addField("eyeglass", TypeDescription.createDouble())
				.addField("facemask", TypeDescription.createDouble())
				.addField("male", TypeDescription.createDouble())
				.addField("female", TypeDescription.createDouble())
				.addField("hannation", TypeDescription.createDouble())
				.addField("is_eyeglass", TypeDescription.createLong())
				.addField("is_facemask", TypeDescription.createLong())
				.addField("is_male", TypeDescription.createLong())
				.addField("is_female", TypeDescription.createLong())
				.addField("is_hannation", TypeDescription.createLong())
				.addField("age", TypeDescription.createLong())
				.addField("pose", TypeDescription.createVarchar().withMaxLength(255))
				.addField("quality", TypeDescription.createDouble())
				.addField("confidence", TypeDescription.createDouble())
				.addField("feature_resource", TypeDescription.createVarchar().withMaxLength(255))
				.addField("feature_version", TypeDescription.createVarchar().withMaxLength(255))
				.addField("face_feature", TypeDescription.createBinary())
				.addField("face_path", TypeDescription.createVarchar().withMaxLength(512))
				.addField("face_rect", TypeDescription.createVarchar().withMaxLength(255))
				.addField("body_id", TypeDescription.createVarchar().withMaxLength(32))
				.addField("capture_time_date", TypeDescription.createTimestamp());
		
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
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
		schema = TypeDescription.createStruct().addField("id", TypeDescription.createVarchar().withMaxLength(32))
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
				.addField("face_tags", TypeDescription.createList(TypeDescription.createLong()))
				.addField("eyeglass", TypeDescription.createDouble())
				.addField("facemask", TypeDescription.createDouble())
				.addField("male", TypeDescription.createDouble())
				.addField("female", TypeDescription.createDouble())
				.addField("hannation", TypeDescription.createDouble())
				.addField("is_eyeglass", TypeDescription.createLong())
				.addField("is_facemask", TypeDescription.createLong())
				.addField("is_male", TypeDescription.createLong())
				.addField("is_female", TypeDescription.createLong())
				.addField("is_hannation", TypeDescription.createLong())
				.addField("age", TypeDescription.createLong())
				.addField("pose", TypeDescription.createVarchar().withMaxLength(255))
				.addField("quality", TypeDescription.createDouble())
				.addField("confidence", TypeDescription.createDouble())
				.addField("feature_resource", TypeDescription.createVarchar().withMaxLength(255))
				.addField("feature_version", TypeDescription.createVarchar().withMaxLength(255))
				.addField("face_feature", TypeDescription.createBinary())
				.addField("face_path", TypeDescription.createVarchar().withMaxLength(512))
				.addField("face_rect", TypeDescription.createVarchar().withMaxLength(255))
				.addField("body_id", TypeDescription.createVarchar().withMaxLength(32))
				.addField("capture_time_date", TypeDescription.createTimestamp());
		
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		FileSystem fs = FileSystem.get(new URI("/"), conf, "gpadmin");
		WriterOptions options = OrcFile.writerOptions(conf)
				.fileSystem(fs)
				.setSchema(schema)
				.stripeSize(stripeSize)
				.bufferSize(bufferSize)
				.blockSize(blockSize)
				.compress(CompressionKind.ZLIB)
				.version(OrcFile.Version.V_0_12);
		String current_date = DATE_FORMAT.format(new Date());
		file_path = path + table + "/" + MessageFormat.format(name_partition_format, partition, name, current_date);
		saveFilePath();
		Path dataPath = new Path(file_path);
		writer = OrcFile.createWriter(dataPath, options);
	}

	@Override
	public void saveAll(List<CaptureFace> faces, ReentrantLock lock) {
		lock.lock();
		try {
			VectorizedRowBatch batch = schema.createRowBatch();
			final int BATCH_SIZE = batch.getMaxSize();
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
			ListColumnVector face_tags = (ListColumnVector) batch.cols[17];
			LongColumnVector face_tags_child = (LongColumnVector) face_tags.child;
			face_tags_child.ensureSize(DEFAULT_LENGTH * BATCH_SIZE, false);
			DoubleColumnVector eyeglass = (DoubleColumnVector) batch.cols[18];
			DoubleColumnVector facemask = (DoubleColumnVector) batch.cols[19];
			DoubleColumnVector male = (DoubleColumnVector) batch.cols[20];
			DoubleColumnVector female = (DoubleColumnVector) batch.cols[21];
			DoubleColumnVector hannation = (DoubleColumnVector) batch.cols[22];
			LongColumnVector is_eyeglass = (LongColumnVector) batch.cols[23];
			LongColumnVector is_facemask = (LongColumnVector) batch.cols[24];
			LongColumnVector is_male = (LongColumnVector) batch.cols[25];
			LongColumnVector is_female = (LongColumnVector) batch.cols[26];
			LongColumnVector is_hannation = (LongColumnVector) batch.cols[27];
			LongColumnVector age = (LongColumnVector) batch.cols[28];
			BytesColumnVector pose = (BytesColumnVector) batch.cols[29];
			DoubleColumnVector quality = (DoubleColumnVector) batch.cols[30];
			DoubleColumnVector confidence = (DoubleColumnVector) batch.cols[31];
			BytesColumnVector feature_resource = (BytesColumnVector) batch.cols[32];
			BytesColumnVector feature_version = (BytesColumnVector) batch.cols[33];
			BytesColumnVector face_feature = (BytesColumnVector) batch.cols[34];
			BytesColumnVector face_path = (BytesColumnVector) batch.cols[35];
			BytesColumnVector face_rect = (BytesColumnVector) batch.cols[36];
			BytesColumnVector body_id = (BytesColumnVector) batch.cols[37];
			TimestampColumnVector capture_time_date = (TimestampColumnVector) batch.cols[38];
			for (CaptureFace face : faces) {
				int row = batch.size++;
				set(id, face.getId(), row);
				set(camera_id, face.getCamera_id(), row);
				set(camera_name, face.getCamera_name(), row);
				set(capture_time, face.getCapture_time(), row);
				set(address, face.getAddress(), row);
				set(longitude, face.getLongitude(), row);
				set(latitide, face.getLatitide(), row);
				setList(installation_location, installation_location_child, face.getInstallation_location(), row);
				setList(org_ids, org_ids_child, face.getOrg_ids(), row);
				setList(operation_center_ids, operation_center_ids_child, face.getOperation_center_ids(), row);
				setList(camera_tags, camera_tags_child, face.getCamera_tags(), row);
				set(scene_path, face.getScene_path(), row);
				set(img_id, face.getImg_id(), row);
				setList(vids, vids_child, face.getVids(), row);
				setList(vid_scores, vid_scores_child, face.getVid_scores(), row);
				set(create_time, face.getCreate_time(), row);
				set(update_time, face.getUpdate_time(), row);
				setList(face_tags, face_tags_child, face.getFace_tags(), row);
				set(eyeglass, face.getEyeglass(), row);
				set(facemask, face.getFacemask(), row);
				set(male, face.getMale(), row);
				set(female, face.getFemale(), row);
				set(hannation, face.getHannation(), row);
				set(is_eyeglass, face.getIs_eyeglass(), row);
				set(is_facemask, face.getIs_facemask(), row);
				set(is_male, face.getIs_male(), row);
				set(is_female, face.getIs_female(), row);
				set(is_hannation, face.getIs_hannation(), row);
				set(age, face.getAge(), row);
				set(pose, face.getPose(), row);
				set(quality, face.getQuality(), row);
				set(confidence, face.getConfidence(), row);
				set(feature_resource, face.getFeature_resource(), row);
				set(feature_version, face.getFeature_version(), row);
				setFeature(face_feature, face.getFace_feature(), row);
				set(face_path, face.getFace_path(), row);
				set(face_rect, face.getFace_rect(), row);
				set(body_id, face.getBody_id(), row);
				set(capture_time_date, face.getCapture_time_date(), row);
				if (row == BATCH_SIZE - 1) {
					try {
						writer.addRowBatch(batch);
					} catch (Exception e) {
						LOG.error(table + "批量写入失败!failed ids : [" + Util.ids(faces) + "]", e);
					}
					batch.reset();
				}
			}
			if (batch.size != 0) {
				writer.addRowBatch(batch);
				batch.reset();
			}
		} catch (IOException e) {
			LOG.error(table + "批量写入失败!failed ids : [" + Util.ids(faces) + "]", e);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public int getFile_count() {
		return file_count;
	}
}