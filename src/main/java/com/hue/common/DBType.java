/**
 * Vero Analytics
 */
package com.hue.common;


/**
 * @author Tai Hu
 * 
 */
public enum DBType {
	POSTGRESQL {
		@Override
		public String getVendorName() {
			return "PostgreSQL";
		}

		@Override
		public String getDriver() {
			return "org.postgresql.Driver";
		}

		@Override
		public String getDBUrl() {
			return "jdbc:postgresql://%s:%d/%s";
		}

		@Override
		public int getDefaultPort() {
			return 5432;
		}

		@Override
		public String getUserNameParamName() {
			return "user";
		}

		@Override
		public String getPasswordParamName() {
			return "password";
		}
	},
	REDSHIFT {
		@Override
		public String getVendorName() {
			return "Amazon Redshift";
		}

		@Override
		public String getDriver() {
			return "com.amazon.redshift.jdbc41.Driver";
		}

		@Override
		public String getDBUrl() {
			return "jdbc:redshift://%s:%d/%s";
		}

		@Override
		public int getDefaultPort() {
			return 5439;
		}

		@Override
		public String getUserNameParamName() {
			return "UID";
		}

		@Override
		public String getPasswordParamName() {
			return "PWD";
		}
	},
	MSSQL {
		@Override
		public String getVendorName() {
			return "Microsoft SQL Server";
		}

		@Override
		public String getDriver() {
			return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		}

		@Override
		public String getDBUrl() {
			return "jdbc:sqlserver://%s:%d;databaseName=%s";
		}

		@Override
		public int getDefaultPort() {
			return 1433;
		}

		@Override
		public String getUserNameParamName() {
			return "user";
		}

		@Override
		public String getPasswordParamName() {
			return "password";
		}
	},
	AZURE {
		@Override
		public String getVendorName() {
			return "Microsoft Azure Cloud";
		}

		@Override
		public String getDriver() {
			return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		}

		@Override
		public String getDBUrl() {
			return "jdbc:sqlserver://%s:%d;databaseName=%s";
		}

		@Override
		public int getDefaultPort() {
			return 1433;
		}

		@Override
		public String getUserNameParamName() {
			return "user";
		}

		@Override
		public String getPasswordParamName() {
			return "password";
		}
	},
	TERADATA {
		@Override
		public String getVendorName() {
			return "Teradata";
		}

		@Override
		public String getDriver() {
			return "com.teradata.jdbc.TeraDriver";
		}

		@Override
		public String getDBUrl() {
			return "jdbc:teradata://%s/DBS_PORT=%d,DATABASE=%s";
		}

		@Override
		public int getDefaultPort() {
			return 1025;
		}

		@Override
		public String getUserNameParamName() {
			return "user";
		}

		@Override
		public String getPasswordParamName() {
			return "password";
		}
	},
	MYSQL {
		@Override
		public String getVendorName() {
			return "MySQL";
		}

		@Override
		public String getDriver() {
			return "com.mysql.jdbc.Driver";
		}

		@Override
		public String getDBUrl() {
			return "jdbc:mysql://%s:%d/%s?rewriteBatchedStatements=true";
		}

		@Override
		public int getDefaultPort() {
			return 3306;
		}

		@Override
		public String getUserNameParamName() {
			return "user";
		}

		@Override
		public String getPasswordParamName() {
			return "password";
		}
	},
	DERBY_LOCAL {
		@Override
		public String getVendorName() {
			return "Derby";
		}

		@Override
		public String getDriver() {
			return "org.apache.derby.jdbc.EmbeddedDriver";
		}

		@Override
		public String getDBUrl() {
			return "jdbc:derby:%s;create=true";
		}

		@Override
		public int getDefaultPort() {
			return 1527;
		}

		@Override
		public String getUserNameParamName() {
			return null;
		}

		@Override
		public String getPasswordParamName() {
			return null;
		}
	},
	
	DERBY_REMOTE {
		@Override
		public String getVendorName() {
			return "Derby";
		}

		@Override
		public String getDriver() {
			return "org.apache.derby.jdbc.ClientDriver";
		}

		@Override
		public String getDBUrl() {
			return "jdbc:derby://%s:%d/%s";
		}

		@Override
		public int getDefaultPort() {
			return 1527;
		}

		@Override
		public String getUserNameParamName() {
			return "user";
		}

		@Override
		public String getPasswordParamName() {
			return "password";
		}
	},
	ACCESS {
		@Override
		public String getVendorName() {
			return "Access";
		}

		@Override
		public String getDriver() {
			return "net.ucanaccess.jdbc.UcanaccessDriver";
		}

		@Override
		public String getDBUrl() {
			return null;
		}

		@Override
		public int getDefaultPort() {
			return 0;
		}

		@Override
		public String getUserNameParamName() {
			return null;
		}

		@Override
		public String getPasswordParamName() {
			return null;
		}
	},
	
    HIVE {
        @Override
        public String getVendorName() {
            return "Hive";
        }

        @Override
        public String getDriver() {
            return "org.apache.hive.jdbc.HiveDriver";
        }

        @Override
        public String getDBUrl() {
            return "jdbc:hive2://%s:%d/%s";
        }

        @Override
        public int getDefaultPort() {
            return 10000;
        }

		@Override
		public String getUserNameParamName() {
			return "user";
		}

		@Override
		public String getPasswordParamName() {
			return "password";
		}
    },
    
	NETEZZA {
		@Override
		public String getVendorName() {
			return "Netezza";
		}

		@Override
		public String getDriver() {
			return "org.netezza.Driver";
		}

		@Override
		public String getDBUrl() {
			return "jdbc:netezza://%s:%d:%s";
		}

		@Override
		public int getDefaultPort() {
			return 5480;
		}

		@Override
		public String getUserNameParamName() {
			return "user";
		}

		@Override
		public String getPasswordParamName() {
			return "password";
		}
	},
    VERTICA {
        @Override
        public String getVendorName() {
            return "Vertica";
        }

        @Override
        public String getDriver() {
            return "com.vertica.jdbc.Driver";
        }

        @Override
        public String getDBUrl() {
            return "jdbc:vertica://%s:%d/%s";
        }

        @Override
        public int getDefaultPort() {
            return 5433;
        }


        @Override
        public String getUserNameParamName() {
            return "user";
        }

        @Override
        public String getPasswordParamName() {
            return "password";
        }
    },

	ORACLE {
		@Override
		public String getVendorName() {
			return "Oracle";
		}

		@Override
		public String getDriver() {
			return "oracle.jdbc.driver.OracleDriver";
		}

		@Override
		public String getDBUrl() {
			return "jdbc:oracle:thin:@%s:%d:%s";
		}

		@Override
		public int getDefaultPort() {
			return 1521;
		}

		@Override
		public String getUserNameParamName() {
			return "user";
		}

		@Override
		public String getPasswordParamName() {
			return "password";
		}	
	},
    PRESTO {
        @Override
        public String getVendorName() {
            return "Presto";
        }

        @Override
        public String getDriver() {
            return null;
        }

        @Override
        public String getDBUrl() {
            //return "jdbc:oracle:thin:@%s:%d:%s";
            return null;
        }

        @Override
        public int getDefaultPort() {
            //return 1521;
            return 0;
        }

        @Override
        public String getUserNameParamName() {
            return "user";
        }

        @Override
        public String getPasswordParamName() {
            return "password";
        }
    },
	UNKNOWN {

		@Override
		public String getVendorName() {
			return null;
		}

		@Override
		public String getDriver() {
			return null;
		}

		@Override
		public String getDBUrl() {
			return null;
		}

		@Override
		public int getDefaultPort() {
			return 0;
		}

		@Override
		public String getUserNameParamName() {
			return null;
		}

		@Override
		public String getPasswordParamName() {
			return null;
		}
	};

	public abstract String getVendorName();

	public abstract String getDriver();

	public abstract String getDBUrl();

	public abstract int getDefaultPort();
		
	public abstract String getUserNameParamName();
	public abstract String getPasswordParamName();
}
