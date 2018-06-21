package com.wave.main;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import java.lang.management.ManagementFactory;
import java.util.function.Consumer;

import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.wave.ha.HAMasterWatcher;
import com.wave.ha.ZKMasterWatcher;
import com.wave.task.SyncCaptureBody;
import com.wave.task.SyncCaptureFace;
import com.wave.task.SyncCaptureImg;
import com.wave.util.LoadConfig;
import com.wave.util.LoadConfig.Config;

/**
 * @Title: SyncCaptureDataEntry.java
 * @Package com.wave.main  
 * @Description: (任务入口)
 * @author: gaoyun
 * @edit by:
 * @date: 2018年4月18日 下午4:08:59
 * @version V1.0
 */
@Component
public class SyncCaptureDataEntry implements ApplicationRunner {

	private static final Logger LOGGER = LoggerFactory.getLogger(SyncCaptureDataEntry.class);

	public static String ZK_HOST = LoadConfig.getInstance("task").get("ha.zookeeper");
	public static final int SESSION_TIMEOUT = 2000;
	public static final int CONNECTION_TIMEOUT = 8000;
	public static final String MASTER_NODE_PATH = "/sync_capture_data/master_node";
	public static String thisNode;
	public static String masterNode;

	@Override
	public void run(ApplicationArguments args) throws Exception {

		thisNode = ManagementFactory.getRuntimeMXBean().getName();
		LOGGER.debug("thisNode: {}", thisNode);

		HAMasterWatcher party = new ZKMasterWatcher(ZK_HOST, SESSION_TIMEOUT, CONNECTION_TIMEOUT, MASTER_NODE_PATH,
				1000, 10);
		party.setThisNode(thisNode);
		if (party.becomeMaster() && thisNode.equals(party.getInstantMaster())) {
			LOGGER.info("Confirmed, I am the Active,masterNode;{}", masterNode);
			execute();
		} else {
			LOGGER.info("Confirmed, I am the Standby,masterNode;{}", masterNode);
			shutdown();
		}

		Consumer<String> consumer = msg -> {
			if (msg == null) {
				LOGGER.error("masterNode is down, try to become Master");
				if (party.becomeMaster()) {
					LOGGER.info("Successfully tried to Became Master");
					execute();
				} else {
					LOGGER.info("Failed to try to Became Master");
					shutdown();
				}
			} else {
				LOGGER.info("masterNode:{}", msg);
			}
		};
		party.beginWatch(consumer);

		while (true) {// 防止线程结束
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
				break;
			}
		}
	}

	public void execute() {
		try {
			Config config = LoadConfig.getInstance("task");
			Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();

			if (scheduler.isStarted()) {
				return;
			}
			JobDetail face_now_detail = newJob(SyncCaptureFace.class)
					.withIdentity("face_now_task", "detail_group")
					.build();
			Trigger face_now_trigger = newTrigger()
					.withIdentity("face_now_task", "trigger_group")
					.startNow()// 立即执行一次
					.build();
			scheduler.scheduleJob(face_now_detail, face_now_trigger);

			JobDetail body_now_detail = newJob(SyncCaptureBody.class)
					.withIdentity("body_now_task", "detail_group")
					.build();
			Trigger body_now_trigger = newTrigger()
					.withIdentity("body_now_task", "trigger_group")
					.startNow()// 立即执行一次
					.build();
			scheduler.scheduleJob(body_now_detail, body_now_trigger);

			JobDetail img_now_detail = newJob(SyncCaptureImg.class)
					.withIdentity("img_now_task", "detail_group")
					.build();
			Trigger img_now_trigger = newTrigger()
					.withIdentity("img_now_task", "trigger_group")
					.startNow()// 立即执行一次
					.build();
			scheduler.scheduleJob(img_now_detail, img_now_trigger);

			// 设置定时同步数据
			JobDetail face_day_task_detail = newJob(SyncCaptureFace.class)
					.withIdentity("face_day_task", "detail_group")
					.build();
			Trigger face_day_task_trigger = newTrigger()
					.withIdentity("face_day_task", "trigger_group")
					.withSchedule(cronSchedule(config.get("orc.capture_face.cron")))
					.build();
			scheduler.scheduleJob(face_day_task_detail, face_day_task_trigger);

			// 设置定时同步数据
			JobDetail body_day_task_detail = newJob(SyncCaptureBody.class)
					.withIdentity("body_day_task", "detail_group")
					.build();
			Trigger body_day_task_trigger = newTrigger()
					.withIdentity("body_day_task", "trigger_group")
					.withSchedule(cronSchedule(config.get("orc.capture_body.cron")))
					.build();
			scheduler.scheduleJob(body_day_task_detail, body_day_task_trigger);

			// 设置定时同步数据
			JobDetail img_day_task_detail = newJob(SyncCaptureImg.class)
					.withIdentity("img_day_task", "detail_group")
					.build();
			Trigger img_day_task_trigger = newTrigger()
					.withIdentity("img_day_task", "trigger_group")
					.withSchedule(cronSchedule(config.get("orc.capture_img.cron")))
					.build();
			scheduler.scheduleJob(img_day_task_detail, img_day_task_trigger);
			// 启动所有定时任务
			scheduler.start();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	public void shutdown() {
		try {
			Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
			if (scheduler.isShutdown()) {
				return;
			}
			scheduler.shutdown();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}
}