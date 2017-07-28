package com.hue.graph;

public class GraphException extends Exception {

	private static final long serialVersionUID = 1L;

	public GraphException() {
	}

	public GraphException(String message) {
		super(message);
	}

	public GraphException(Throwable cause) {
		super(cause);
	}

	public GraphException(String message, Throwable cause) {
		super(message, cause);
	}

	public GraphException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
