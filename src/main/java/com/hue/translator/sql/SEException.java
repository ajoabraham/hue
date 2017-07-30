package com.hue.translator.sql;

import com.vero.common.sql.SqlException;

public class SEException extends SqlException {
    private static final long serialVersionUID = 1L;

    public SEException(Type type, String message) {
        super(type, message);
    }

    public SEException(Throwable cause) {
        super(cause);
    }

    public SEException(Type type, String message, Throwable cause) {
        super(type, message, cause);
    }

    public SEException(
	Type type,
        String message,
        Throwable cause,
        boolean enableSuppression,
        boolean writableStackTrace) {
        super(type, message, cause, enableSuppression, writableStackTrace);
    }
}