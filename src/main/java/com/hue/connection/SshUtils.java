package com.hue.connection;

import com.hue.connection.SshService.SshServiceBuilder;
import com.hue.model.Datasource;

public final class SshUtils {

	private SshUtils() {
	}

	public static SshService createSshService(Datasource datasource) {
		SshServiceBuilder sshServiceBuilder = SshServiceBuilder.create()
				.hostName(datasource.getServer()).port(datasource.getPort())
				.sshHostName(datasource.getSshHostAddress()).sshPort(datasource.getSshPort())
				.localPort(datasource.getSshLocalPort()).sshUserName(datasource.getSshUserName());
		
		if (datasource.isUseKeyFile()) {
			sshServiceBuilder.sshKeyFile(datasource.getSshKeyFile());
			sshServiceBuilder.sshPassphrase(datasource.getSshPassphrase());
		}
		else {
			sshServiceBuilder.sshPassword(datasource.getSshPassword());
		}
		
		
		return sshServiceBuilder.build();
	}
}
