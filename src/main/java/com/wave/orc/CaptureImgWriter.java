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
import com.wave.vo.CaptureImg;

/**  
 * @Title:  CaptureImgWriter.java   
 * @Package com.antelope.orc   
 * @Description:    (写img数据)   
 * @author: gaoyun     
 * @edit by: 
 * @date:   2018年5月29日 上午10:14:32   
 * @version V1.0 
 */ 
public class CaptureImgWriter extends ORCWriter<CaptureImg> {

	private final static Logger LOG = Logger.getLogger(CaptureImgWriter.class);

	private int file_count;

	public CaptureImgWriter(Config config, String name) {
		super(config, Constant.IMG_TABLE_NAME);
		file_count = Integer.parseInt(config.get("orc." + Constant.IMG_TABLE_NAME + ".count.prefile"));
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
	public CaptureImgWriter(Config config, String name, String current_day, Integer partition) {
		super(config, Constant.IMG_TABLE_NAME);
		file_count = Integer.parseInt(config.get("orc." + Constant.IMG_TABLE_NAME + ".count.prefile"));
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
				.addField("scene_path", TypeDescription.createVarchar().withMaxLength(256))
				.addField("face_infos", TypeDescription.createVarchar().withMaxLength(9000))
				.addField("body_infos", TypeDescription.createVarchar().withMaxLength(9000))
				.addField("create_time", TypeDescription.createTimestamp())
				.addField("update_time", TypeDescription.createTimestamp())
				.addField("file_name", TypeDescription.createVarchar().withMaxLength(512))
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
				.addField("scene_path", TypeDescription.createVarchar().withMaxLength(256))
				.addField("face_infos", TypeDescription.createVarchar().withMaxLength(9000))
				.addField("body_infos", TypeDescription.createVarchar().withMaxLength(9000))
				.addField("create_time", TypeDescription.createTimestamp())
				.addField("update_time", TypeDescription.createTimestamp())
				.addField("file_name", TypeDescription.createVarchar().withMaxLength(512))
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
		file_path = path + table + "/" + MessageFormat.format(name_partition_format, partition, name, DATE_FORMAT.format(new Date()));
		saveFilePath();
		Path dataPath = new Path(file_path);
		writer = OrcFile.createWriter(dataPath, options);
	}

	@Override
	public void saveAll(List<CaptureImg> imgs, ReentrantLock lock) {
		lock.lock();
		try {
			VectorizedRowBatch batch = schema.createRowBatch();
			final int BATCH_SIZE = batch.getMaxSize();
			int index = 0;
			BytesColumnVector id = (BytesColumnVector) batch.cols[index++];
			BytesColumnVector camera_id = (BytesColumnVector) batch.cols[index++];
			BytesColumnVector camera_name = (BytesColumnVector) batch.cols[index++];
			LongColumnVector capture_time = (LongColumnVector) batch.cols[index++];
			BytesColumnVector address = (BytesColumnVector) batch.cols[index++];
			DoubleColumnVector longitude = (DoubleColumnVector) batch.cols[index++];
			DoubleColumnVector latitide = (DoubleColumnVector) batch.cols[index++];
			ListColumnVector installation_location = (ListColumnVector) batch.cols[index++];
			LongColumnVector installation_location_child = (LongColumnVector) installation_location.child;
			installation_location_child.ensureSize(DEFAULT_LENGTH * BATCH_SIZE, false);
			ListColumnVector org_ids = (ListColumnVector) batch.cols[index++];
			LongColumnVector org_ids_child = (LongColumnVector) org_ids.child;
			org_ids_child.ensureSize(DEFAULT_LENGTH * BATCH_SIZE, false);
			ListColumnVector operation_center_ids = (ListColumnVector) batch.cols[index++];
			LongColumnVector operation_center_ids_child = (LongColumnVector) operation_center_ids.child;
			operation_center_ids_child.ensureSize(DEFAULT_LENGTH * BATCH_SIZE, false);
			ListColumnVector camera_tags = (ListColumnVector) batch.cols[index++];
			LongColumnVector camera_tags_child = (LongColumnVector) camera_tags.child;
			camera_tags_child.ensureSize(DEFAULT_LENGTH * BATCH_SIZE, false);
			BytesColumnVector scene_path = (BytesColumnVector) batch.cols[index++];
			BytesColumnVector face_infos = (BytesColumnVector) batch.cols[index++];
			BytesColumnVector body_infos = (BytesColumnVector) batch.cols[index++];
			TimestampColumnVector create_time = (TimestampColumnVector) batch.cols[index++];
			TimestampColumnVector update_time = (TimestampColumnVector) batch.cols[index++];
			BytesColumnVector file_name = (BytesColumnVector) batch.cols[index++];
			TimestampColumnVector capture_time_date = (TimestampColumnVector) batch.cols[index++];
			for (CaptureImg img : imgs) {
				int row = batch.size++;
				set(id, img.getId(), row);
				set(camera_id, img.getCamera_id(), row);
				set(camera_name, img.getCamera_name(), row);
				set(capture_time, img.getCapture_time(), row);
				set(address, img.getAddress(), row);
				set(longitude, img.getLongitude(), row);
				set(latitide, img.getLatitide(), row);
				setList(installation_location, installation_location_child, img.getInstallation_location(), row);
				setList(org_ids, org_ids_child, img.getOrg_ids(), row);
				setList(operation_center_ids, operation_center_ids_child, img.getOperation_center_ids(), row);
				setList(camera_tags, camera_tags_child, img.getCamera_tags(), row);
				set(scene_path, img.getScene_path(), row);
				set(face_infos, img.getFace_infos(), row);
				set(body_infos, img.getBody_infos(), row);
				set(create_time, img.getCreate_time(), row);
				set(update_time, img.getUpdate_time(), row);
				set(file_name, img.getFile_name(), row);
				set(capture_time_date, img.getCapture_time_date(), row);
				if (row == BATCH_SIZE - 1) {
					try {
						writer.addRowBatch(batch);
					} catch (Exception e) {
						LOG.error(table + "批量写入失败!failed ids : [" + Util.ids(imgs) + "]", e);
					}
					batch.reset();
				}
			}
			if (batch.size != 0) {
				writer.addRowBatch(batch);
				batch.reset();
			}
		} catch (IOException e) {
			LOG.error(table + "批量写入失败!failed ids : [" + Util.ids(imgs) + "]", e);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public int getFile_count() {
		return file_count;
	}
}