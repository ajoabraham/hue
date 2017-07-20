package com.hue.connection;

public class SshException extends Exception {
	private static final long serialVersionUID = 1L;

	public SshException() {
	}

	public SshException(String message) {
		super(message);
	}

	public SshException(Throwable cause) {
		super(cause);
	}

	public SshException(String message, Throwable cause) {
		super(message, cause);
	}

	public SshException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
