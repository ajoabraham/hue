package com.hue.model.persist;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HueConfig {
	private static final Properties config;
	private static final Logger logger = LoggerFactory.getLogger(HueConfig.class.getName());

	static {
	  Properties fallback = new Properties();
	  fallback.put("projectsDirectory", "projects");
	  config = new Properties(fallback);
	  try {
	    InputStream stream = new FileInputStream("hue.properties");
	    try {
	      config.load(stream);
	    }
	    finally {
	      stream.close();
	    }
	  }
	  catch (IOException ex) {
	    logger.warn("hue.properties files was not found. Using defaults");
	  }
	}
	
	public String getProjectsDirectory() {
		return config.getProperty("projectsDirectory");
	}
	
	public void setProjectsDirectory(String dir) {
		config.setProperty("projectsDirectory", dir);
	}
}
