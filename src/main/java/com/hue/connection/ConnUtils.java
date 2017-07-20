package com.hue.connection;


import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hue.common.DBType;
import com.hue.model.Datasource;
import com.hue.utils.CommonUtils;

import schemacrawler.schemacrawler.DatabaseConnectionOptions;
import schemacrawler.schemacrawler.SchemaCrawlerException;
import schemacrawler.schemacrawler.UserCredentials;

/**
 * 
 * @author Tai Hu
 *
 * Centralized place to acquire database connections
 */
public final class ConnUtils {
	private static final Logger logger = LoggerFactory.getLogger(ConnUtils.class.getName());
	
	private ConnUtils() {		
	}
	
	// TH 06/10/2015, this method is only used under the multithread situation. When multiple thread connect to
	// a database, SSH Tunnel should be started before thread started and close when threads all finished.
	// This connection will not start a SSH tunnel, but still format connection URL to use localhost and local
	// ssh port.
	public static Connection getNonSshConnection(Datasource datasource) throws ClassNotFoundException, SshException, SQLException, IOException {
		return getConnection(datasource, false);
	}

			
	public static Connection getConnection(Datasource datasource) throws ClassNotFoundException, SshException, SQLException, IOException {
		return getConnection(datasource, true);
	}
	
	private static Connection getConnection(Datasource datasource, boolean enableSsh) throws SshException, SQLException, IOException, ClassNotFoundException {
		try {
    			DBType dbType = datasource.getDatabaseType();
    
    			String dbUrl = getDbConnectionUrl(datasource, false);
    
    			Connection connection = null;		
    			Class.forName(dbType.getDriver());
    			
    			Connection dbConnection = null;
    			SshService sshService = null;
    			
    			try {
            			if (enableSsh && datasource.isUseSshTunnel()) {
            				sshService = SshUtils.createSshService(datasource);
            				sshService.connect();
            			}
            			
            			Properties props = new Properties();
            			if (!CommonUtils.isBlank(datasource.getUserName()))
            				props.put(datasource.getDatabaseType().getUserNameParamName(), datasource.getUserName());
            			if (!CommonUtils.isBlank(datasource.getPassword()))
            				props.put(datasource.getDatabaseType().getPasswordParamName(), datasource.getPassword());
            			
            			if (datasource.getConnProps() != null && !datasource.getConnProps().isEmpty()) {            				
            				props.putAll(datasource.getConnProps());
            			}
//            			else {
//            				dbConnection = DriverManager.getConnection(dbUrl);
//            			}
            			
        				dbConnection = DriverManager.getConnection(dbUrl, props);
            			
            			if (enableSsh && datasource.isUseSshTunnel()) {				
            				connection = new SshConnection(sshService, dbConnection);
            			}
            			else {
                			connection = dbConnection;
            			}
    			}
    			catch (SshException e) {
    				if (enableSsh && datasource.isUseSshTunnel() && sshService != null) {
    					sshService.disconnect();
    				}
    				
    				throw new SshException(e);
    			}
	    		
	    		return connection;
		}
		catch (SQLException | ClassNotFoundException e) {
			if (CommonUtils.containsPassword(e, datasource.getUserName(), datasource.getPassword())) {
				String sanitizedMessage = CommonUtils.sanitizeMessage(e, datasource.getUserName(), datasource.getPassword());
				logger.info(sanitizedMessage);
				throw new SQLException("Failed to connect to database. Please check log file for details.");
			}
			else {
				throw e;
			}
		}
	}
	
	public static Connection getSchemaCrawlerConnection(Datasource datasource) throws SchemaCrawlerException, SQLException, SshException {
		try {
    		String dbUrl = getDbConnectionUrl(datasource, true);
    		
    		// Create a database connection
    		DataSource databaseConnectionOptions = null;
			Map<String, String> props = new HashMap<>();
			props.put("url", dbUrl);
			if (datasource.getDatabaseType() != DBType.DERBY_LOCAL && !CommonUtils.isBlank(datasource.getUserName()))
				props.put(datasource.getDatabaseType().getUserNameParamName(), datasource.getUserName());
			if (!CommonUtils.isBlank(datasource.getPassword()))
				props.put(datasource.getDatabaseType().getPasswordParamName(), datasource.getPassword());
			
    		if (datasource.getConnProps() != null && !datasource.getConnProps().isEmpty()) {
    			props.putAll(datasource.getConnProps());
    		}
//    		else {
//    			databaseConnectionOptions = new DatabaseConnectionOptions(dbUrl);	
//    		}
    		
			databaseConnectionOptions = new DatabaseConnectionOptions(getUserCredentials(datasource), props);
    		
    		if (datasource.isUseSshTunnel()) {
    			SshService sshService = SshUtils.createSshService(datasource);
    			try {
    				sshService.connect();
    				// If connect to database failed, need to kill ssh tunnel as well.
    				return new SshConnection(sshService, databaseConnectionOptions.getConnection());
    			}
    			catch (Exception e) {
    				if (sshService != null) {
    					sshService.disconnect();
    				}
    				
    				throw new SshException(e);
    			}
    		}
    		
    		if (datasource.getDatabaseType() == DBType.VERTICA) {
    			// FIXME: vertica jdbc property doesn't have "password"
    			return databaseConnectionOptions.getConnection(datasource.getUserName(), datasource.getPassword());
    		} else {
    			return databaseConnectionOptions.getConnection();
    		}
		}
		catch (SchemaCrawlerException | SQLException | SshException e) {			
			
			if (CommonUtils.containsPassword(e, datasource.getUserName(), datasource.getPassword())) {
				String sanitizedMessage = CommonUtils.sanitizeMessage(e, datasource.getUserName(), datasource.getPassword());
				logger.info(sanitizedMessage);				
				
				throw new SQLException(buildClearDBErrorMessage(e));
			}
			else {
				throw new SQLException(buildClearDBErrorMessage(e));
			}
		}
	}
	
	private static UserCredentials getUserCredentials(Datasource datasource) {
		return new UserCredentials() {
			
			@Override
			public boolean hasUser() {
				return !CommonUtils.isBlank(datasource.getUserName());
			}
			
			@Override
			public boolean hasPassword() {
				return !CommonUtils.isBlank(datasource.getPassword());
			}
			
			@Override
			public String getUser() {
				return datasource.getUserName();
			}
			
			@Override
			public String getPassword() {
				return datasource.getPassword();
			}
			
			@Override
			public void clearPassword() {
				// NOOP				
			}
		};
	}

	public static boolean isVeroDBErrorMessage(String msg){
		return msg.contains(" Possible Reasons:");
	}
	
	public static String buildClearDBErrorMessage(Exception e){
		String msg = "Failed to connect to Database. Possible Reasons:";
		if(e.getCause() != null){
			if(e.getCause().getCause() != null){
				msg += "\n\t" + e.getCause().getCause().getClass().getSimpleName()+": " +e.getCause().getCause().getMessage();
				if(e.getCause().getCause().getCause() != null){
					msg += "\n\t" + e.getCause().getCause().getCause().getClass().getSimpleName()+": " +e.getCause().getCause().getCause().getMessage();
				}
			}
			msg += "\n\t" + e.getCause().getLocalizedMessage();					
		}
		msg += "\n\t" + e.getLocalizedMessage();
		return msg;
	}
	
	private static String getDbConnectionUrl(Datasource datasource, boolean isSchemaCrawler) {
		DBType dbType = datasource.getDatabaseType();
		String dbUrl = null;

		String hostName = datasource.getServer();
		int port = datasource.getPort();
		
		if (dbType != DBType.DERBY_LOCAL && datasource.isUseSshTunnel()) {
			hostName = "localhost";
			port = datasource.getSshLocalPort();
		}
		
		if (!CommonUtils.isBlank(datasource.getJdbcUrl())) {
			dbUrl = datasource.getJdbcUrl();
		}
		else {
			dbUrl = String.format(dbType.getDBUrl(), hostName, port, datasource.getDatabaseName());
		}

		return dbUrl;
	}
}
