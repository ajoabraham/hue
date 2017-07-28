package com.hue.model.persist;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.lang.WordUtils;

import com.hue.utils.CommonUtils;

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
	
	public static String deriveExpressibleName(String colName, String tableName, boolean dePluralize) {
		tableName = CommonUtils.removeQuotes(tableName).trim();
		
		if(dePluralize) {
			String tn = tableName.toLowerCase();
			if(tn.endsWith("ves")) {
				tableName = tn.replaceAll("ves$", "f");
			}
			else if(tn.endsWith("ies")) {
				tableName = tn.replaceAll("ies$", "y");
			}
			else if(tn.endsWith("s")) {
				tableName = tn.replaceAll("s$", "");
			}
		}
		colName = CommonUtils.removeQuotes(colName).trim();
		
		if(colName.toLowerCase().equalsIgnoreCase("id")) {
			colName = tableName+ "_"+colName;
		}
		
		if(colName.contains("_")){
			return WordUtils.capitalizeFully(colName.replaceAll("_", " "));
		}else{
			colName = colName.replaceAll(
				      String.format("%s|%s|%s",
				         "(?<=[A-Z])(?=[A-Z][a-z])",
				         "(?<=[^A-Z])(?=[A-Z])",
				         "(?<=[A-Za-z])(?=[^A-Za-z])"
				      ),
				      " "
				   );
			if(!colName.contains(" ")){
				colName = WordUtils.capitalizeFully(colName);
			}
			
			return colName;
		}		   
	}
	
}
