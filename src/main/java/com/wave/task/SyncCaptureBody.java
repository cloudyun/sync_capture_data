package com.wave.task;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.wave.common.Constant;
import com.wave.util.LoadConfig;
import com.wave.util.LoadConfig.Config;
import com.wave.util.Util;
import com.wave.vo.CaptureBody;

/**
 * @Title: SyncCaptureBody.java
 * @Package com.antelope.service
 * @Description: (同步人体数据)
 * @author: gaoyun
 * @edit by:
 * @date: 2018年5月28日 下午3:55:33
 * @version V1.0
 */
public class SyncCaptureBody extends SyncCaptureBase implements Job {

	private final static Logger LOG = Logger.getLogger(SyncCaptureBody.class);
	
	private BlockingQueue<CaptureBody> queue;
	
	private ReentrantLock lock;

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		setTable(Constant.BODY_TABLE_NAME);
		current_day = Util.getCurrentDay();
		if (isSuccess()) {
			return;
		}
		List<String> paths = getFilePaths();
		if (paths != null && !paths.isEmpty()) {
			delete(paths);
		}
		
		
		LOG.info(table + " : 开始从pg库同步数据到HAWQ...");
		lock = new ReentrantLock();
		queue = new ArrayBlockingQueue<CaptureBody>(1024);
		Config config = LoadConfig.getInstance("task");
		
		record = getRecord();
		current = Util.getCurrentRecord();
		LOG.info(table + " : 启动读取pg库数据线程...");
		CaptureBody empty = new CaptureBody(true);
		new QueryThread<CaptureBody>(queue, empty).start();

		LOG.info(table + " : 启动写orc文件线程...");
		int thread_count = Integer.parseInt(config.get("orc." + table + ".thread.count"));
		Thread[] writes = new Thread[thread_count];
		for (int index = 0; index < writes.length; index++) {
			String name = table + "_" + index;
			writes[index] = new WritesThread<CaptureBody>(queue, lock);
			writes[index].setName(name);
			writes[index].start();
		}
		
		for (int index = 0; index < writes.length; index++) {
			try {
				writes[index].join();
				LOG.info(table + " : 写orc文件线程[" + index + "]结束!!!");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		if (success.get()) {
			svaeRecord(current);
			backupAndDelete(-3);//删除三天前的数据
		}

		queue.clear();
		queue = null;
		LOG.info(table + " : 从pg库同步数据到HAWQ结束!!!");
	}
}