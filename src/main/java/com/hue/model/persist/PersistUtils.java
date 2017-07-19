package com.hue.model.persist;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

public final class PersistUtils {
	
	private PersistUtils() {
	}
	
	public static boolean projectExists(String projectName) {
		Path pFolder = FileSystems.getDefault().getPath(ProjectServices.getInstance().getConfig().getProjectsDirectory(), projectName);
		Path pFile = FileSystems.getDefault().getPath(ProjectServices.getInstance().getConfig().getProjectsDirectory(), projectName, "project.json");
		return fileExists(pFolder) && fileExists(pFile);
	}
	
	public static boolean isValidProject(String projectName) {
		return projectExists(projectName);
	}
	
	public static boolean isValidDatasource(String projectName, String dsName) {
		Path dsFolder = FileSystems.getDefault().getPath(ProjectServices.getInstance().getConfig().getProjectsDirectory(), projectName, dsName);
		Path dsFile = FileSystems.getDefault().getPath(ProjectServices.getInstance().getConfig().getProjectsDirectory(), projectName, dsName, "datasource.json");
		return fileExists(dsFolder) && fileExists(dsFile);					
	}
	
	private static boolean fileExists(Path pr) {
		if(Files.exists(pr)) {
			return true;
		}else {
			return false;
		}
	}
}
