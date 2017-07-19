package com.hue.model;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.Lists;
import com.hue.common.DBType;
import com.hue.utils.SecurityUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Datasource extends HueBase {
	private static final long serialVersionUID = 1L;

	private String databaseName;
	private DBType databaseType;
	private String server;
	private String password;
	private Integer port;
	private String postExecCommands;	
	private String preExecCommands;	
	private String userName;
	
	private boolean useSshTunnel;	
	private String sshUserName;	
	private String sshPassword;	
	private String sshHostAddress;	
	private int sshPort;	
	private boolean useKeyFile;	
	private String sshKeyFile;	
	private String sshPassphrase;
	
	private String tempTablePrefix;
	private String tempTableSchema;
	private String jdbcUrl;
	
	private Map<String,String> connProps;
	
	private String accessKeyId;	
	private String accessKey;	
	private String region;	
	private String bucketName;	
	private String objectKey;
	
	@JsonIgnore
	private int sshLocalPort = -1;

	@JsonIgnore
	private List<Table> tables = Lists.newArrayList();

	public Datasource() {
	}

	public List<Table> getTables() {
		return tables;
	}

	public void setTables(List<Table> tables) {
		this.tables = tables;
	}

	public String getDatabaseName() {
		return this.databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public DBType getDatabaseType() {
		return this.databaseType;
	}

	public void setDatabaseType(DBType databaseType) {
		this.databaseType = databaseType;
	}

	public String getServer() {
		return this.server;
	}

	public void setServer(String hostAddress) {
		this.server = hostAddress;
	}

	public String getPassword() {
//		if (!CommonUtils.isBlank(password)) {
//			try {
//				return SecurityUtils.decryptPassword(password);
//			}
//			catch (Exception e) {
//				throw new RuntimeException(e);
//			}
//		}

		return this.password;
	}

	public void setPassword(String password) {
//		if (!CommonUtils.isBlank(password)) {
//			try {
//				password = SecurityUtils.encryptPassword(password);
//			}
//			catch (Exception e) {
//				throw new RuntimeException(e);
//			}
//		}

		this.password = password;
	}

	public Integer getPort() {
		if(port == null) {
			return getDatabaseType().getDefaultPort();
		}
		return this.port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public String getPostExecCommands() {
		return this.postExecCommands;
	}

	public void setPostExecCommands(String postExecCommands) {
		this.postExecCommands = postExecCommands;
	}

	public String getPreExecCommands() {
		return this.preExecCommands;
	}

	public void setPreExecCommands(String preExecCommands) {
		this.preExecCommands = preExecCommands;
	}

	public String getUserName() {
		return this.userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public boolean isUseSshTunnel() {
		return useSshTunnel;
	}

	public void setUseSshTunnel(boolean useSshTunnel) {
		this.useSshTunnel = useSshTunnel;
	}

	public String getSshUserName() {
		return sshUserName;
	}

	public void setSshUserName(String sshUserName) {
		this.sshUserName = sshUserName;
	}

	public String getSshPassword() {
//		if (!CommonUtils.isBlank(sshPassword)) {
//			try {
//				return SecurityUtils.decryptPassword(sshPassword);
//			}
//			catch (Exception e) {
//				throw new RuntimeException(e);
//			}
//		}
		return sshPassword;
	}

	public void setSshPassword(String sshPassword) {
//		if (!CommonUtils.isBlank(sshPassword)) {
//			try {
//				sshPassword = SecurityUtils.encryptPassword(sshPassword);
//			}
//			catch (Exception e) {
//				throw new RuntimeException(e);
//			}
//		}

		this.sshPassword = sshPassword;
	}

	public String getSshHostAddress() {
		return sshHostAddress;
	}

	public void setSshHostAddress(String sshHostAddress) {
		this.sshHostAddress = sshHostAddress;
	}

	public int getSshPort() {
		return sshPort;
	}

	public void setSshPort(int sshPort) {
		this.sshPort = sshPort;
	}

	public boolean isUseKeyFile() {
		return useKeyFile;
	}

	public void setUseKeyFile(boolean useKeyFile) {
		this.useKeyFile = useKeyFile;
	}

	public String getSshKeyFile() {
		return sshKeyFile;
	}

	public void setSshKeyFile(String sshKeyFile) {
		this.sshKeyFile = sshKeyFile;
	}

	public String getSshPassphrase() {
		// TODO: Figure out pwd encryption in files
//		if (!CommonUtils.isBlank(sshPassphrase)) {
//			try {
//				return SecurityUtils.decryptPassword(sshPassphrase);
//			}
//			catch (Exception e) {
//				throw new RuntimeException(e);
//			}
//		}
		return sshPassphrase;
	}

	public void setSshPassphrase(String sshPassphrase) {
//		if (!CommonUtils.isBlank(sshPassphrase)) {
//			try {
//				sshPassphrase = SecurityUtils.encryptPassword(sshPassphrase);
//			}
//			catch (Exception e) {
//				throw new RuntimeException(e);
//			}
//		}
		this.sshPassphrase = sshPassphrase;
	}

	public String getTempTablePrefix() {
		return tempTablePrefix;
	}

	public void setTempTablePrefix(String tempTablePrefix) {
		this.tempTablePrefix = tempTablePrefix;
	}

	public String getJdbcUrl() {
		return jdbcUrl;
	}

	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	public Map<String, String> getConnProps() {
		return connProps;
	}

	public void setConnProps(Map<String,String> connProps) {
		this.connProps = connProps;
	}

	public final int getSshLocalPort() {
		if (sshLocalPort == -1) {
			sshLocalPort = SecurityUtils.getRandomUnusedLocalPort();
			
			if (sshLocalPort == -1) {
				sshLocalPort = getPort();
			}
		}
		
		return sshLocalPort;
	}
	
	public final void setSshLocalPort(int sshLocalPort) {
		this.sshLocalPort = sshLocalPort;
	}

	public String getAccessKeyId() {
		return accessKeyId;
	}

	public void setAccessKeyId(String accessKeyId) {
		this.accessKeyId = accessKeyId;
	}

	public String getAccessKey() {
		return accessKey;
	}

	public void setAccessKey(String accessKey) {
		this.accessKey = accessKey;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public String getBucketName() {
		return bucketName;
	}

	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}

	public String getObjectKey() {
		return objectKey;
	}

	public void setObjectKey(String objectKey) {
		this.objectKey = objectKey;
	}

	public String getTempTableSchema() {
		return tempTableSchema;
	}

	public void setTempTableSchema(String tempTableSchema) {
		this.tempTableSchema = tempTableSchema;
	}
}
