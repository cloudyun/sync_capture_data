1、修改配置，配置目录：/src/main/resources/

	1.1、下载目标hadoop集群的core-site.xml和hdfs-site.xml文件替换配置目录中的

	1.2、修改数据源配置：jdbc.properties(并在数据源新建表,建表语句在/sql目录下)

	1.3、修改任务配置：task.properties
	
	    ha.zookeeper: 高可用zookeeper配置

		orc.data.path：HAWQ库所在路径

		orc.stripe.size：写orc的stripeSize

		orc.buffer.size：写orc的bufferSize

		orc.block.size：写orc的blockSize

		orc.charset：写orc字符串编码
		

        orc.capture_face.table: 人脸数据表
        
        orc.capture_face.cron：人脸定时规则
        
		orc.capture_face.thread.count：人脸写orc文件线程

		orc.capture_face.count.prefile：人脸写orc文件数据限制打个文件的数据量
		
		orc.capture_body.table： 人体数据表
		
		orc.capture_body.cron： 人体定时规则

		orc.capture_body.thread.count：人体写orc文件线程

		orc.capture_body.count.prefile：人体写orc文件数据限制打个文件的数据量
		
		orc.capture_img.table：图片数据表
		
		orc.capture_img.cron：图片定时规则

		orc.capture_img.thread.count：图片写orc文件线程

		orc.capture_img.count.prefile：图片写orc文件数据限制打个文件的数据量

2、使用maven打包

3、运行

	3.1、分别在两个节点上启动，启动命令如下

	java -jar sync_capture_data.jar