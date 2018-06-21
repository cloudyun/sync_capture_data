package com.wave.ha;

import java.util.function.Consumer;

/**  
 * @Title:  MasterWatcher.java   
 * @Package com.wave.ha   
 * @Description:    (高可用接口)   
 * @author: gaoyun     
 * @edit by: 
 * @date:   2018年6月19日 下午4:26:17   
 * @version V1.0 
 */ 
public interface HAWatcher {

    // 开始监听master,发生变化时调用回调函数，能获得最新的 ip:port
    void beginWatch(Consumer<String> nameNodeChanged);

    // 成为master
    boolean becomeMaster();

    // 获得当前即时的master的 ip:port
    String getInstantMaster();

}
