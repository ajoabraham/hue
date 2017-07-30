package com.hue.translator.sql;

import java.util.ArrayList;
import java.util.List;

public class HiveConfig {
	private static class Configuration {
		String name;
		String value;
		String version;
		String description;

		public Configuration(String name, String value, String version, String description) {
			super();
			this.name = name;
			this.value = value;
			this.version = version;
			this.description = description;
		}

		public String getName() { return name; }
		public String getValue() { return value; }
		public String getVersion() { return version; }
		public String getDescription() { return description; }
	}

	private static final Configuration[] configurations = {
		new Configuration("hive.conf.validation", "false", "0.10.0", "Enables type checking for registered Hive configurations."),
		new Configuration("hive.optimize.index.filter", "true", "0.8.0", "Whether to enable automatic use of indexes."),
		new Configuration("hive.optimize.ppd", "true", "0.4.0", "Whether to enable predicate pushdown (PPD)."),
		new Configuration("hive.mapred.mode", "nonstrict", "0.3.0", "The mode in which the Hive operations are being performed. In strict mode, some risky queries are not allowed to run. For example, full table scans are prevented (see HIVE-10454) and ORDER BY requires a LIMIT clause."),
		new Configuration("hive.exec.parallel", "true", "0.5.0", "Whether to execute jobs in parallel.  Applies to MapReduce jobs that can run in parallel, for example jobs processing different source tables before a join.  As of Hive 0.14, also applies to move tasks that can run in parallel, for example moving files to insert targets during multi-insert."),
		new Configuration("hive.auto.convert.join", "true", "0.7.0", "Whether Hive enables the optimization about converting common join into mapjoin based on the input file size."),
		new Configuration("hive.auto.convert.join.noconditionaltask", "true", "0.10.0", "Whether Hive enables the optimization about converting common join into mapjoin based on the input file size. If this parameter is on, and the sum of size for n-1 of the tables/partitions for an n-way join is smaller than the size specified by hive.auto.convert.join.noconditionaltask.size, the join is directly converted to a mapjoin (there is no conditional task)."),
		new Configuration("hive.optimize.correlation", "true", "0.12.0", "Exploit intra-query correlations."),
		new Configuration("hive.exec.mode.local.auto", "true", "0.7.0", "Let Hive determine whether to run in local mode automatically."),
		new Configuration("hive.exec.drop.ignorenonexistent", "true", "0.7.0", "Do not report an error if DROP TABLE/VIEW/PARTITION/INDEX/TEMPORARY FUNCTION specifies a non-existent table/view. Also applies to permanent functions as of Hive 0.13.0."),
		new Configuration("hive.support.quoted.identifiers", "column", "0.13.0", "Whether to use quoted identifiers.  Value can be none or column."),
		new Configuration("hive.exec.check.crossproducts", "false", "0.13.0", "Check if a query plan contains a cross product. If there is one, output a warning to the session's console."),
		new Configuration("hive.optimize.sampling.orderby", "true", "0.12.0", "Uses sampling on order-by clause for parallel execution."),
        new Configuration("hive.mapred.reduce.tasks.speculative.execution", "true", "0.5.0", "Whether speculative execution for reducers should be turned on."),

		// vectorized execution relies on ORC file format
		new Configuration("hive.default.fileformat", "ORC", "0.2.0", "Default file format for CREATE TABLE statement. Options are TextFile, SequenceFile, RCfile, and ORC."),
		new Configuration("hive.exec.orc.skip.corrupt.data", "true", "0.13.0", "If ORC reader encounters corrupt data, this value will be used to determine whether to skip the corrupt data or throw an exception. The default behavior is to throw an exception."),
		new Configuration("hive.vectorized.execution.enabled", "true", "0.13.0", "This flag should be set to true to enable vectorized mode of query execution. The default value is false.")
	};

	public static List<String> getConfigurations() {
	    List<String> configs = new ArrayList<String>();

	    for (Configuration config : configurations) {
	        configs.add(new String("set ".concat(config.getName()).concat("=").concat(config.getValue())));
	    }

	    return configs;
	}
}
