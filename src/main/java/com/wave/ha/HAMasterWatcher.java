package com.wave.ha;

/**
 * @Title: HAMasterWatcher.java
 * @Package com.wave.ha
 * @Description: (高可用基类)
 * @author: gaoyun
 * @edit by:
 * @date: 2018年6月19日 下午4:27:32
 * @version V1.0
 */
public abstract class HAMasterWatcher implements HAWatcher {

	// 第三方组件获取连接
	public abstract void getCon();

	// 第三方组件释放连接
	public abstract void releaseCon();

	// master节点
	protected volatile String masterNode;
	
	// 当前节点
	protected String thisNode;

	// 不一定是即时的
	public String getMasterNode() {
		return masterNode;
	}

	public void setMasterNode(String masterNode) {
		this.masterNode = masterNode;
	}

	public String getThisNode() {
		return thisNode;
	}

	public void setThisNode(String thisNode) {
		this.thisNode = thisNode;
	}
}
