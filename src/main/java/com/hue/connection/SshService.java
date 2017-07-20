package com.hue.connection;

import com.hue.utils.CommonUtils;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.util.Properties;

public final class SshService {
	private String hostName = null;
	private int port = 22;
	private String sshHostName = null;
	private int sshPort = 22;
	private int localPort = 0;
	private String sshUserName = null;
	private String sshPassword = null;
	private String sshKeyFile = null;
	private String sshPassphrase = null;
	
	private Session session = null;
	
	private SshService(SshServiceBuilder builder) {
		hostName = builder.hostName;
		port = builder.port;
		sshHostName = builder.sshHostName;
		sshPort = builder.sshPort;
		localPort = builder.localPort;
		sshUserName = builder.sshUserName;
		sshPassword = builder.sshPassword;
		sshKeyFile = builder.sshKeyFile;
		sshPassphrase = builder.sshPassphrase;
	}
	
	public void connect() throws SshException {
		try {
    		final JSch jsch = new JSch();
    		if (sshKeyFile != null) {
    			if (sshPassphrase != null && !sshPassphrase.trim().equals("")) {
    				jsch.addIdentity(sshKeyFile);
    			}
    			else {
    				jsch.addIdentity(sshKeyFile, sshPassphrase);
    			}
    		}
    		
    		Properties config = new Properties();
    		config.put("StrictHostKeyChecking", "no");
    		
    		session = jsch.getSession(sshUserName, sshHostName, sshPort);
    		session.setConfig(config);
    		
    		if (sshKeyFile == null) {
    			session.setPassword(sshPassword);
    		}
    		
    		session.connect();
    		session.setPortForwardingL(localPort, hostName, port);
		}
		catch (Exception e) {
			if (CommonUtils.doesPortForwardingAlreadyExist(e))
				session = null;
			else
				throw new SshException(e);
		}
	}

	public boolean isConnected() {
		return session != null
				&& session.isConnected();
	}
	
	public void disconnect() throws SshException {
		try {
			if (session != null) {
				session.disconnect();
			}
		}
		catch (Exception e) {
			throw new SshException(e);
		}
	}

	public static class SshServiceBuilder {
		private String hostName = null;
		private int port = 22;
		private String sshHostName = null;
		private int sshPort = 22;
		private int localPort = 0;
		private String sshUserName = null;
		private String sshPassword = null;
		private String sshKeyFile = null;
		private String sshPassphrase = null;
		
		private SshServiceBuilder() {			
		}
		
		public static SshServiceBuilder create() {
			return new SshServiceBuilder();
		}
		
		public SshServiceBuilder hostName(String hostName) {
			this.hostName = hostName;
			return this;
		}
		
		public SshServiceBuilder port(int port) {
			this.port = port;
			return this;
		}
		
		public SshServiceBuilder sshHostName(String sshHostName) {
			this.sshHostName = sshHostName;
			return this;
		}
		
		public SshServiceBuilder sshPort(int sshPort) {
			this.sshPort = sshPort;
			return this;
		}
		
		public SshServiceBuilder localPort(int localPort) {
			this.localPort = localPort;
			return this;
		}
		
		public SshServiceBuilder sshUserName(String sshUserName) {
			this.sshUserName = sshUserName;
			return this;
		}
		
		public SshServiceBuilder sshPassword(String sshPassword) {
			this.sshPassword = sshPassword;
			return this;
		}
		
		public SshServiceBuilder sshKeyFile(String sshKeyFile) {
			this.sshKeyFile = sshKeyFile;
			return this;
		}
		
		public SshServiceBuilder sshPassphrase(String sshPassphrase) {
			this.sshPassphrase = sshPassphrase;
			return this;
		}
		
		public SshService build() {
			return new SshService(this);
		}
	}
}
