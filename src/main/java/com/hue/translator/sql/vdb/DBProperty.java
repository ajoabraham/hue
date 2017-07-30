package com.hue.translator.sql.vdb;

interface DBProperty {
    // jdbc properties
    abstract Boolean supportsFullOuterJoins();
    //abstract int getDriverMinorVersion();
    //abstract Boolean supportsUnion();
    //abstract Boolean supportsGroupBy();
    //abstract int getDatabaseMajorVersion();
    //abstract int getDatabaseMinorVersion();
    abstract Boolean supportsSubqueriesInExist();
    abstract int getMaxColumnNameLength();

    // vero properties
    abstract Boolean supportsWindowFunctions();
    abstract Boolean supportsCreateTemporaryTable();
}
