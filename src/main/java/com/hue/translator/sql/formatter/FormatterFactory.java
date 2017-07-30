package com.hue.translator.sql.formatter;

import com.vero.common.constant.DBType;

public class FormatterFactory {
    // ExpressionFormatter
    private static VeroGenExpFormatter genExpFormatter = new VeroGenExpFormatter();
    private static VeroGenExpFormatter postgresqlExpFormatter = new VeroPostgresqlExpFormatter();
    private static VeroGenExpFormatter teradataExpFormatter = new VeroTeradataExpFormatter();
    private static VeroGenExpFormatter oracleExpFormatter = new VeroOracleExpFormatter();
    private static VeroGenExpFormatter mssqlExpFormatter = new VeroMssqlExpFormatter();
    private static VeroGenExpFormatter mysqlExpFormatter = new VeroMysqlExpFormatter();
    private static VeroGenExpFormatter derbyExpFormatter = new VeroDerbyExpFormatter();
    private static VeroGenExpFormatter redshiftExpFormatter = new VeroRedshiftExpFormatter();
    private static VeroGenExpFormatter hiveExpFormatter = new VeroHiveExpFormatter();
    private static VeroGenExpFormatter accessExpFormatter = new VeroAccessExpFormatter();
    private static VeroGenExpFormatter drillExpFormatter = new VeroDrillExpFormatter();
    private static VeroGenExpFormatter netezzaExpFormatter = new VeroNetezzaExpFormatter();
    private static VeroGenExpFormatter salesforceExpFormatter = new VeroSalesforceExpFormatter();
    private static VeroGenExpFormatter googleAnalyticsExpFormatter = new VeroGoogleAnalyticsExpFormatter();
    private static VeroGenExpFormatter verticaExpFormatter = new VeroVerticaExpFormatter();
    private static VeroGenExpFormatter prestoExpFormatter = new VeroPrestoExpFormatter();

    // SqlFormatter
    private static VeroGenSqlFormatter genSqlFormatter = new VeroGenSqlFormatter(genExpFormatter);
    private static VeroGenSqlFormatter postgresqlSqlFormatter = new VeroPostgresqlSqlFormatter(postgresqlExpFormatter);
    private static VeroGenSqlFormatter teradataSqlFormatter = new VeroTeradataSqlFormatter(teradataExpFormatter);
    private static VeroGenSqlFormatter oracleSqlFormatter = new VeroOracleSqlFormatter(oracleExpFormatter);
    private static VeroGenSqlFormatter mssqlSqlFormatter = new VeroMssqlSqlFormatter(mssqlExpFormatter);
    private static VeroGenSqlFormatter mysqlSqlFormatter = new VeroMysqlSqlFormatter(mysqlExpFormatter);
    private static VeroGenSqlFormatter derbySqlFormatter = new VeroDerbySqlFormatter(derbyExpFormatter);
    private static VeroGenSqlFormatter redshiftSqlFormatter = new VeroRedshiftSqlFormatter(redshiftExpFormatter);
    private static VeroGenSqlFormatter hiveSqlFormatter = new VeroHiveSqlFormatter(hiveExpFormatter);
    private static VeroGenSqlFormatter accessSqlFormatter = new VeroAccessSqlFormatter(accessExpFormatter);
    private static VeroGenSqlFormatter drillSqlFormatter = new VeroDrillSqlFormatter(drillExpFormatter);
    private static VeroGenSqlFormatter netezzaSqlFormatter = new VeroNetezzaSqlFormatter(netezzaExpFormatter);
    private static VeroGenSqlFormatter salesforceSqlFormatter = new VeroSalesforceSqlFormatter(salesforceExpFormatter);
    private static VeroGenSqlFormatter googleAnalyticsSqlFormatter = new VeroGoogleAnalyticsSqlFormatter(googleAnalyticsExpFormatter);
    private static VeroGenSqlFormatter verticaSqlFormatter = new VeroVerticaSqlFormatter(verticaExpFormatter);
    private static VeroGenSqlFormatter prestoSqlFormatter = new VeroPrestoSqlFormatter(prestoExpFormatter);
    
    static {
        // Set SqlFormatter to ExpressionFormatter
        // 1. Generic : Generic ExpFormatter + Generic SqlFormatter
        genExpFormatter.setSqlFormatter(genSqlFormatter);

        // 2. Postgresql : Postgresql ExpFormatter + Postgresql SqlFormatter
        postgresqlExpFormatter.setSqlFormatter(postgresqlSqlFormatter);

        // 3. Teradata : Teradata ExpFormatter + Teradata SqlFormatter
        teradataExpFormatter.setSqlFormatter(teradataSqlFormatter);

        // 4. Oracle : Oracle ExpFormatter + Oracle SqlFormatter
        oracleExpFormatter.setSqlFormatter(oracleSqlFormatter);

        // 5. Mssql : Mssql ExpFormatter + Mssql SqlFormatter
        mssqlExpFormatter.setSqlFormatter(mssqlSqlFormatter);

        // 6. Mysql : Mysql ExpFormatter + Mysql SqlFormatter
        mysqlExpFormatter.setSqlFormatter(mysqlSqlFormatter);

        // 7. Derby : Derby ExpFormatter + Derby SqlFormatter
        derbyExpFormatter.setSqlFormatter(derbySqlFormatter);

        // 8. Redshift : Redshift ExpFormatter + Redshift SqlFormatter
        redshiftExpFormatter.setSqlFormatter(redshiftSqlFormatter);

        // 9. Hive : Hive ExpFormatter + Hive(Mysql) SqlFormatter
        hiveExpFormatter.setSqlFormatter(hiveSqlFormatter);

        // 10. Access : Access ExpFormatter + Access SqlFromatter
        accessExpFormatter.setSqlFormatter(accessSqlFormatter);
        
        // 11. Drill : Drill ExpFormatter + Drill SqlFromatter
        drillExpFormatter.setSqlFormatter(drillSqlFormatter);
        
        // 12. Netezza : Netezza ExpFormatter + Netezza SqlFromatter
        netezzaExpFormatter.setSqlFormatter(netezzaSqlFormatter);
        
        // 13. Salesforce
        salesforceExpFormatter.setSqlFormatter(salesforceSqlFormatter);
        
        // 14. Google Analytics
        googleAnalyticsExpFormatter.setSqlFormatter(googleAnalyticsSqlFormatter);
        
        // 15. Vertica
        verticaExpFormatter.setSqlFormatter(verticaSqlFormatter);
        
        // 16. Presto
        prestoExpFormatter.setSqlFormatter(prestoSqlFormatter);
    }

    public static VeroGenExpFormatter getExpressionFormatter(String dbName) {
        switch (dbName) {
            case "POSTGRESQL":
                return postgresqlExpFormatter;
            case "TERADATA":
                return teradataExpFormatter;
            case "ORACLE":
                return oracleExpFormatter;
            case "MSSQL":
                return mssqlExpFormatter;
            case "MYSQL":
                return mysqlExpFormatter;
            case "HIVE":
                return hiveExpFormatter;
            case "DERBY":
            case "DERBY_LOCAL":
                return derbyExpFormatter;
            case "REDSHIFT":
                return redshiftExpFormatter;
            case "ACCESS":
                return accessExpFormatter;
            case "DRILL":
                return drillExpFormatter;
            case "NETEZZA":
                return netezzaExpFormatter;
            case "SALESFORCE":
                return salesforceExpFormatter;
            case "GOOGLEANALYTICS":
                return googleAnalyticsExpFormatter;
            case "VERTICA":
                return verticaExpFormatter;
            case "PRESTO":
                return prestoExpFormatter;
            case "GENERIC":
            default:
                //return genExpFormatter;
                return postgresqlExpFormatter;
        }
    }

    public static VeroGenSqlFormatter getSqlFormatter(String dbName) {
        switch (dbName) {
            case "POSTGRESQL":
                return postgresqlSqlFormatter;
            case "TERADATA":
                return teradataSqlFormatter;
            case "ORACLE":
                return oracleSqlFormatter;
            case "MSSQL":
                return mssqlSqlFormatter;
            case "MYSQL":
                return mysqlSqlFormatter;
            case "HIVE":
                return hiveSqlFormatter;
            case "DERBY":
            case "DERBY_LOCAL":
                return derbySqlFormatter;
            case "REDSHIFT":
                return redshiftSqlFormatter;
            case "ACCESS":
                return accessSqlFormatter;
            case "DRILL":
                return drillSqlFormatter;
            case "NETEZZA":
                return netezzaSqlFormatter;
            case "SALESFORCE":
                return salesforceSqlFormatter;
            case "GOOGLEANALYTICS":
                return googleAnalyticsSqlFormatter;
            case "VERTICA":
                return verticaSqlFormatter;
            case "PRESTO":
                return prestoSqlFormatter;
            case "GENERIC":
            default:
                //return genSqlFormatter;
                return postgresqlSqlFormatter;
        }
    }

    public static VeroGenExpFormatter getExpressionFormatter(DBType dbType) {
        switch (dbType) {
            case POSTGRESQL:
                return postgresqlExpFormatter;
            case TERADATA:
                return teradataExpFormatter;
            case ORACLE:
                return oracleExpFormatter;
            case MSSQL:
            case AZURE:
                return mssqlExpFormatter;
            case MYSQL:
                return mysqlExpFormatter;
            case HIVE:
                return hiveExpFormatter;
            case DERBY_LOCAL:
                return derbyExpFormatter;
            case REDSHIFT:
                return redshiftExpFormatter;
            case ACCESS:
                return accessExpFormatter;
            case DRILL:
                return drillExpFormatter;
            case NETEZZA:
                return netezzaExpFormatter;
            case SALESFORCE:
                return salesforceExpFormatter;
            case GOOGLEANALYTICS:
                return googleAnalyticsExpFormatter;
            case VERTICA:
                return verticaExpFormatter;
            case PRESTO:
                return prestoExpFormatter;
            default:
                //return genExpFormatter;
                return postgresqlExpFormatter;
        }
    }

    public static VeroGenSqlFormatter getSqlFormatter(DBType dbType) {
        switch (dbType) {
            case POSTGRESQL:
                return postgresqlSqlFormatter;
            case TERADATA:
                return teradataSqlFormatter;
            case ORACLE:
                return oracleSqlFormatter;
            case MSSQL:
            case AZURE:
                return mssqlSqlFormatter;
            case MYSQL:
                return mysqlSqlFormatter;
            case HIVE:
                return hiveSqlFormatter;
            case DERBY_LOCAL:
                return derbySqlFormatter;
            case REDSHIFT:
                return redshiftSqlFormatter;
            case ACCESS:
                return accessSqlFormatter;
            case DRILL:
                return drillSqlFormatter;
            case NETEZZA:
                return netezzaSqlFormatter;
            case SALESFORCE:
                return salesforceSqlFormatter;
            case GOOGLEANALYTICS:
                return googleAnalyticsSqlFormatter;
            case VERTICA:
                return verticaSqlFormatter;
            case PRESTO:
                return prestoSqlFormatter;
            default:
                //return genSqlFormatter;
                return postgresqlSqlFormatter;
        }
    }

    public static VeroGenExpFormatter getExpressionFormatter() {
        return genExpFormatter;
    }

    public static VeroGenSqlFormatter getSqlFormatter() {
        return genSqlFormatter;
    }
}
