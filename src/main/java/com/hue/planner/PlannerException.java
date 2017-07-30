package com.hue.planner;

public class PlannerException extends Exception {

	private static final long serialVersionUID = 1L;

	public PlannerException() {
	}

	public PlannerException(String message) {
		super(message);
	}

	public PlannerException(Throwable cause) {
		super(cause);
	}

	public PlannerException(String message, Throwable cause) {
		super(message, cause);
	}

	public PlannerException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
