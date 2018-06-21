package com.wave;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class SyncCaptureDataApplication {
	
	private static ApplicationContext ctx = null;

    public static void main(String[] args) {
    	ctx = SpringApplication.run(SyncCaptureDataApplication.class, args);
    }

	public static ApplicationContext getCtx() {
		return ctx;
	}
}
