package com.hue.model;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Project {
	private String name;
	
	@JsonIgnore
	private File file;
	
	private int maxReportExecutionTime = 1800;
	private int maxResultRows = 100000;
	private int maxTempExecutionTime = -1;
	private int maxTempResultRows =-1;	
	private String desc;
	
	@JsonIgnore
	private List<Datasource> datasources = new ArrayList<>();

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Datasource> getDatasources() {
		return datasources;
	}

	public void setDatasources(List<Datasource> datasources) {
		this.datasources = datasources;
	}
	
	/**
	 * 
	 * 
	 * @return the directory location of this project
	 */
	public File getFile() {
		return file;
	}
	
	public void setFile(File file) {
		this.file = file;
	}
	
	/**
	 * Total report execution time in seconds. -1
	 * and 0 indicate no limit.
	 *
	 * @return
	 */
	public int getMaxReportExecutionTime() {
		return maxReportExecutionTime;
	}

	public void setMaxReportExecutionTime(int maxReportExecutionTime) {
		this.maxReportExecutionTime = maxReportExecutionTime;
	}

	/**
	 * Total number of result rows allowed in the final query
	 * of the report. This is the query that fetches data from
	 * the target source. -1 and 0 indicates no limit.
	 *
	 * @return
	 */
	public int getMaxResultRows() {
		return maxResultRows;
	}

	public void setMaxResultRows(int maxResultRows) {
		this.maxResultRows = maxResultRows;
	}

	/**
	 * This is the maximum execution time allowed in
	 * intermediate queries like SetBlocks, EngineBlocks,
	 * and QueryBlocks.  -1 and 0 indicates no limit.
	 *
	 * @return
	 */
	public int getMaxTempExecutionTime() {
		return maxTempExecutionTime;
	}

	public void setMaxTempExecutionTime(int maxTempExecutionTime) {
		this.maxTempExecutionTime = maxTempExecutionTime;
	}

	/**
	 * This is the maximum results allowed in temporary tables
	 * created by SetBlocks, EngineBlocks, and QueryBlocks.
	 * -1 and 0 indicates no limit.
	 *
	 * @return
	 */
	public int getMaxTempResultRows() {
		return maxTempResultRows;
	}

	public void setMaxTempResultRows(int maxTempResultRows) {
		this.maxTempResultRows = maxTempResultRows;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

}
