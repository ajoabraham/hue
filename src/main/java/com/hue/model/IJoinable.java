package com.hue.model;

import java.util.List;


public interface IJoinable {

	public String getPhysicalName();
	public List<String> getPhysicalNameSegments();
	public String getName();

}
