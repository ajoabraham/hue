package com.hue.translator.sql.vdb;

import java.util.Map;

import com.vero.common.constant.DBType;
import com.vero.model.Datasource;

public class VirtualDB implements DBProperty {
    protected class DerbyDBProperty implements DBProperty {
        public Boolean supportsFullOuterJoins() { return false; }
        public Boolean supportsSubqueriesInExist() { return true; }
        public Boolean supportsWindowFunctions() { return false; }
        public Boolean supportsCreateTemporaryTable() { return false; }
        public int getMaxColumnNameLength() { return 128; }
    }

    protected class HiveDBProperty implements DBProperty {
        public Boolean supportsFullOuterJoins() { return true; }
        public Boolean supportsSubqueriesInExist() { return false; }
        public Boolean supportsWindowFunctions() { return true; }
        public Boolean supportsCreateTemporaryTable() { return true; } // Hive starts to support temp table after 0.13
        public int getMaxColumnNameLength() { return 128; }
    }

    protected class MssqlDBProperty implements DBProperty {
        public Boolean supportsFullOuterJoins() { return true; }
        public Boolean supportsSubqueriesInExist() { return true; }
        public Boolean supportsWindowFunctions() { return true; }
        public Boolean supportsCreateTemporaryTable() { return true; }
        public int getMaxColumnNameLength() { return 128; }
    }

    protected class MysqlDBProperty implements DBProperty {
        public Boolean supportsFullOuterJoins() { return false; }
        public Boolean supportsSubqueriesInExist() { return true; }
        public Boolean supportsWindowFunctions() { return false; }
        public Boolean supportsCreateTemporaryTable() { return true; }
        public int getMaxColumnNameLength() { return 64; }
    }

    protected class OracleDBProperty implements DBProperty {
        public Boolean supportsFullOuterJoins() { return true; }
        public Boolean supportsSubqueriesInExist() { return true; }
        public Boolean supportsWindowFunctions() { return true; }
        public Boolean supportsCreateTemporaryTable() { return true; }
        public int getMaxColumnNameLength() { return 30; }
    }

    protected class PostgresqlDBProperty implements DBProperty {
        public Boolean supportsFullOuterJoins() { return true; }
        public Boolean supportsSubqueriesInExist() { return true; }
        public Boolean supportsWindowFunctions() { return true; }
        public Boolean supportsCreateTemporaryTable() { return true; }
        public int getMaxColumnNameLength() { return 31; }
    }

    protected class RedshiftDBProperty implements DBProperty {
        public Boolean supportsFullOuterJoins() { return true; }
        public Boolean supportsSubqueriesInExist() { return true; }
        public Boolean supportsWindowFunctions() { return true; }
        public Boolean supportsCreateTemporaryTable() { return true; }
        public int getMaxColumnNameLength() { return 127; }
    }

    protected class TeradataDBProperty implements DBProperty {
        public Boolean supportsFullOuterJoins() { return true; }
        public Boolean supportsSubqueriesInExist() { return true; }
        public Boolean supportsWindowFunctions() { return true; }
        public Boolean supportsCreateTemporaryTable() { return true; }
        public int getMaxColumnNameLength() { return 30; }
    }

    protected class AccessDBProperty implements DBProperty {
        public Boolean supportsFullOuterJoins() { return false; }
        public Boolean supportsSubqueriesInExist() { return true; }
        public Boolean supportsWindowFunctions() { return false; }
        public Boolean supportsCreateTemporaryTable() { return false; }
        public int getMaxColumnNameLength() { return 30; }
    }
    
    protected class DrillDBProperty implements DBProperty {
        public Boolean supportsFullOuterJoins() { return true; }
        public Boolean supportsSubqueriesInExist() { return true; }
        public Boolean supportsWindowFunctions() { return true; }
        public Boolean supportsCreateTemporaryTable() { return false; }
        public int getMaxColumnNameLength() { return 30; }
    }
    
    protected class NetezzaDBProperty implements DBProperty {
        public Boolean supportsFullOuterJoins() { return true; }
        public Boolean supportsSubqueriesInExist() { return false; }
        public Boolean supportsWindowFunctions() { return true; }
        public Boolean supportsCreateTemporaryTable() { return true; }
        public int getMaxColumnNameLength() { return 128; }
    }
    
    protected class VerticaDBProperty implements DBProperty {
        public Boolean supportsFullOuterJoins() { return true; }
        public Boolean supportsSubqueriesInExist() { return true; }
        public Boolean supportsWindowFunctions() { return true; }
        public Boolean supportsCreateTemporaryTable() { return true; }
        public int getMaxColumnNameLength() { return 128; }
    }
    
    protected class PrestoDBProperty implements DBProperty {
        public Boolean supportsFullOuterJoins() { return true; }
        public Boolean supportsSubqueriesInExist() { return true; }
        public Boolean supportsWindowFunctions() { return true; }
        public Boolean supportsCreateTemporaryTable() { return true; }
        public int getMaxColumnNameLength() { return 128; }
    }

    private DBProperty _physicalDBProperty = null;
    private Map<String, Object> _dbProps = null;
    private DBType _dbType = DBType.POSTGRESQL;
    private Boolean _appendSchema = true;

    // for regression tests
    public VirtualDB(DBType dbType, Boolean appendSchema) {
        _dbType = dbType;
        setPhysicalDBProperty(dbType);
        _appendSchema = appendSchema;
    }

    public VirtualDB(DBType dbType) {
        _dbType = dbType;
        setPhysicalDBProperty(dbType);
    }

    public VirtualDB(Datasource datasource) {
        if (datasource != null) {
            _dbType = datasource.getDatabaseType();
            setPhysicalDBProperty(_dbType);

            try {
                _dbProps = datasource.getDbProps();
            } catch (Exception e) {
                System.out.println("VirtualDB: Exception: " + e);
                _dbProps = null;
            }
        } else {
            // in regression tests, datasource is null
            setDefault();
        }
    }

    public VirtualDB() { setDefault(); }

    private void setDefault() {
        _dbType = DBType.POSTGRESQL;
        setPhysicalDBProperty(_dbType);
        _dbProps = null;
        _appendSchema = true;
    }

    private void setPhysicalDBProperty(DBType dbType) {
        switch (dbType) {
            case DERBY_LOCAL:
            case DERBY_REMOTE:
                _physicalDBProperty = new DerbyDBProperty();
                break;
            case HIVE:
                _physicalDBProperty = new HiveDBProperty();
                break;
            case MSSQL:
            case AZURE:
                _physicalDBProperty = new MssqlDBProperty();
                break;
            case MYSQL:
                _physicalDBProperty = new MysqlDBProperty();
                break;
            case ORACLE:
                _physicalDBProperty = new OracleDBProperty();
                break;
            case POSTGRESQL:
                _physicalDBProperty = new PostgresqlDBProperty();
                break;
            case REDSHIFT:
                _physicalDBProperty = new RedshiftDBProperty();
                break;
            case TERADATA:
                _physicalDBProperty = new TeradataDBProperty();
                break;
            case ACCESS:
                _physicalDBProperty = new AccessDBProperty();
                break;
            case DRILL:
                _physicalDBProperty = new DrillDBProperty();
                break;
            case NETEZZA:
                _physicalDBProperty = new NetezzaDBProperty();
                break;
            case VERTICA:
                _physicalDBProperty = new VerticaDBProperty();
                break;
            case PRESTO:
                _physicalDBProperty = new PrestoDBProperty();
                break;
            default:
                System.out.println("DB: " + dbType + " not supported in VirtualDB...");
                assert(true);
        }
    }

    public DBType getDbType() {
        return _dbType;
    }

    public Boolean getAppendSchema() {
        return _appendSchema;
    }

    @Override
    public Boolean supportsFullOuterJoins() {
        if (_dbProps != null) {
            Object value = _dbProps.get("supportsFullOuterJoins");
            if (value != null) {
                return (Boolean) value;
            }
        }

        return _physicalDBProperty.supportsFullOuterJoins();
    }

    @Override
    public Boolean supportsSubqueriesInExist() {
        if (_dbProps != null) {
            Object value = _dbProps.get("supportsSubqueriesInExist");
            if (value != null) {
                return (Boolean) value;
            }
        }

        return _physicalDBProperty.supportsSubqueriesInExist();
    }

    @Override
    public int getMaxColumnNameLength() {
        if (_dbProps != null) {
            Object value = _dbProps.get("getMaxColumnNameLength");
            if (value != null) {
                return (Integer) value;
            }
        }

        return _physicalDBProperty.getMaxColumnNameLength();
    }
    
    @Override
    public Boolean supportsWindowFunctions() {
        if (_dbProps != null) {
            Object value = _dbProps.get("supportsWindowFunctions");
            if (value != null) {
                return (Boolean) value;
            }
        }

        return _physicalDBProperty.supportsWindowFunctions();
    }
    
    @Override
    public Boolean supportsCreateTemporaryTable() {
        if (_dbProps != null) {
            Object value = _dbProps.get("supportsCreateTemporaryTable");
            if (value != null) {
                return (Boolean) value;
            }
        }

        return _physicalDBProperty.supportsCreateTemporaryTable();
    }
}
