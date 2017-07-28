package com.hue.services;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hue.common.CardinalityType;
import com.hue.common.ColumnKeyType;
import com.hue.common.DBType;
import com.hue.common.JoinType;
import com.hue.common.TableType;
import com.hue.connection.ConnUtils;
import com.hue.connection.SshException;
import com.hue.model.Column;
import com.hue.model.Datasource;
import com.hue.model.Join;
import com.hue.utils.CommonUtils;

import schemacrawler.schema.Catalog;
import schemacrawler.schema.ForeignKey;
import schemacrawler.schema.Schema;
import schemacrawler.schema.Table;
import schemacrawler.schemacrawler.RegularExpressionExclusionRule;
import schemacrawler.schemacrawler.RegularExpressionInclusionRule;
import schemacrawler.schemacrawler.SchemaCrawlerException;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaInfoLevel;
import schemacrawler.schemacrawler.SchemaInfoLevelBuilder;
import schemacrawler.utility.SchemaCrawlerUtility;

public final class SchemaImportService {
	private static final Logger logger = LoggerFactory.getLogger(SchemaImportService.class.getName());

	private static final String TEST_QUERY = "SELECT 1+1";
	private static final String TEST_QUERY_ORACLE = "SELECT 'X' FROM dual";
	private static final String TEST_QUERY_DERBY = "SELECT 1+1 FROM SYSIBM.SYSDUMMY1";
	private static final String ROW_COUNT_QUERY = "SELECT count(*) FROM \"%s\"";
	private static final String ROW_COUNT_QUERY_WITH_SCHEMA = "SELECT count(*) FROM \"%s\".\"%s\"";
	private static final String MYSQL_ROW_COUNT_QUERY = "SELECT count(*) FROM `%s`.`%s`";
	private static final List<String> EXCLUDED_METHODS = Arrays.asList("getURL", "toString", "hashCode", "equals");

	private static final Map<String, schemacrawler.schema.Table> schemaTables = new HashMap<String, schemacrawler.schema.Table>();

	private SchemaImportService() {
	}

	// Import schema names only. No need for anything else.
	public static List<String> importSchemas(Datasource datasource) throws ServiceException {
		List<String> schemaNames = new ArrayList<>();
		try (Connection connection = ConnUtils.getSchemaCrawlerConnection(datasource)) {
			final SchemaCrawlerOptions options = getSchemaCrawlerOptions(datasource);
			options.getSchemaInfoLevel().setRetrieveDatabaseInfo(false);
			options.getSchemaInfoLevel().setRetrieveTables(false);
			Catalog database = SchemaCrawlerUtility.getCatalog(connection, options);
			// MySQL only has catalog name, no schema name
			if (datasource.getDatabaseType() == DBType.MYSQL) {
				schemaNames.addAll(
						database.getSchemas().stream().map(Schema::getCatalogName).collect(Collectors.toList()));
			}
			else {
				schemaNames.addAll(database.getSchemas().stream().map(Schema::getName).collect(Collectors.toList()));
			}
		}
		catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new ServiceException(e);
		}

		return schemaNames;
	}

	public static Integer getRowCount(Datasource datasource, String schemaName, String tableName)
			throws ServiceException {

		DBType dbType = datasource.getDatabaseType();

		try (final Connection connection = ConnUtils.getNonSshConnection(datasource)) {
			String query = null;
			tableName = CommonUtils.removeQuotes(tableName);
			
			if (dbType == DBType.MYSQL || dbType == DBType.HIVE) {
				query = String.format(MYSQL_ROW_COUNT_QUERY, schemaName, tableName);
			}
			else if (dbType == DBType.POSTGRESQL || dbType == DBType.REDSHIFT 
					|| dbType == DBType.MSSQL || dbType == DBType.DERBY_LOCAL
					|| datasource.getDatabaseType() == DBType.AZURE
					|| datasource.getDatabaseType() == DBType.VERTICA) {
				query = String.format(ROW_COUNT_QUERY_WITH_SCHEMA, schemaName, tableName);
			}
			else {
				query = String.format(ROW_COUNT_QUERY, tableName);
			}

			logger.debug("Count Query = " + query);

			ResultSet rs = connection.createStatement().executeQuery(query);

			rs.next();
			return rs.getInt(1);
		}
		catch (Exception e) {
			throw new ServiceException(e);
		}
	}

	public static void importColumnsForTable(Datasource datasource, com.hue.model.Table table) throws ServiceException {
		// TH 06/07/2015, this method will be called in multithread environment.
		// So have to create
		// SSH connection outside of this method.
		// SshService sshService = null;
		
		// AA 8/19/2015 - quoting table name to prevent query errors.
		String q = "";
		try (Connection connection = ConnUtils.getNonSshConnection(datasource)) {
			q = connection.getMetaData().getIdentifierQuoteString();
			
			// Create the options
			final SchemaCrawlerOptions options = getSchemaCrawlerOptions(datasource);
			// update options to only specific schema and table
			String schemaName = table.getSchemaName();
			if (datasource.getDatabaseType() == DBType.MSSQL 
					|| datasource.getDatabaseType() == DBType.AZURE
					|| datasource.getDatabaseType() == DBType.REDSHIFT
					|| datasource.getDatabaseType() == DBType.VERTICA) {
				schemaName = datasource.getDatabaseName() + "." + schemaName;
			}

			options.getSchemaInfoLevel().setRetrieveTableColumns(true);
			options.getSchemaInfoLevel().setRetrieveForeignKeyDefinitions(true);
			options.getSchemaInfoLevel().setRetrievePrimaryKeyDefinitions(true);
			options.getSchemaInfoLevel().setRetrieveForeignKeyDefinitions(true);
			options.setSchemaInclusionRule(createSchemaInclusionRule(datasource, table.getSchemaName()));
			options.setTableNamePattern(table.getPhysicalName());

			try {
				// Get the schema definition
				Catalog database = SchemaCrawlerUtility.getCatalog(connection, options);
				com.annimon.stream.Optional<? extends Schema> schemaO = database.lookupSchema(schemaName);
				
				if(!schemaO.isPresent()) throw new ServiceException("Schema with name " + schemaName + " not found.");

				com.annimon.stream.Optional<? extends Table> schemaTable = database.lookupTable(schemaO.get(), 
						quoteIdent(table.getPhysicalName(),q));
				
				if(!schemaTable.isPresent()) throw new ServiceException("Table with name " + table.getPhysicalName() + " not found in schema " + schemaName);
				
				// Saved schema table for join detection later.
				schemaTables.put(table.getName(), schemaTable.get());

				for (schemacrawler.schema.Column schemaColumn : schemaTable.get().getColumns()) {
					Column column = new Column();
					column.setName(stripQuoteIdent(CommonUtils.removeBackTicks(schemaColumn.getName()),q));					
					column.setDataType(CommonUtils.mapToDataType(
							schemaColumn.getColumnDataType().getTypeMappedClass().getName(), 
							schemaColumn.getColumnDataType().getJavaSqlType().getJavaSqlTypeName(), 
							schemaColumn.getColumnDataType().getDatabaseSpecificTypeName()));
					column.setKeyType(getKeyType(schemaColumn, schemaTable.get()));
					
					table.getColumns().add(column);
				}
			}
			catch (Exception e) {
				logger.error(e.getMessage(), e);
				// TH 04/22/2015, In Teradata, if user doesn't have permission
				// on DBC.UDTInfo, getColumns through JDBC will
				// fail. Implemented this fall back method try to retrieve
				// column from ResultSet
				DatabaseMetaData databaseMetaData = connection.getMetaData();
				Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery(
						"select * from " + quoteIdent(table.getPhysicalName(),q) + " where 1 = 2");

				ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

				List<String> primaryKeys = new ArrayList<>();
				List<String> foreignKeys = new ArrayList<>();

				ResultSet primaryKeyResultSet = databaseMetaData.getPrimaryKeys(null, schemaName,
						table.getPhysicalName());

				while (primaryKeyResultSet.next()) {
					String columnName = primaryKeyResultSet.getString("COLUMN_NAME");
					primaryKeys.add(columnName);
				}

				ResultSet foreignKeyResultSet = connection.getMetaData().getImportedKeys(null, schemaName,
						table.getPhysicalName());

				while (foreignKeyResultSet.next()) {
					String columnName = foreignKeyResultSet.getString("FKCOLUMN_NAME");
					foreignKeys.add(columnName);
				}

				for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
					Column column = new Column();
					column.setName(resultSetMetaData.getColumnName(i));
					column.setDataType(CommonUtils.mapToDataType(
							resultSetMetaData.getColumnClassName(i), 
							CommonUtils.getJavaSqlTypeName(resultSetMetaData.getColumnType(i)), 
							resultSetMetaData.getColumnTypeName(i)));
					
					if (primaryKeys.contains(column.getName()))
						column.setKeyType(ColumnKeyType.PRIMARY_KEY);
					else if (foreignKeys.contains(column.getName()))
						column.setKeyType(ColumnKeyType.FOREIGN_KEY);
					else
						column.setKeyType(ColumnKeyType.NO_KEY_TYPE);

//					column.setNullable(resultSetMetaData.isNullable(i) != ResultSetMetaData.columnNoNulls);

					table.getColumns().add(column);
				}
			}

			table.setTableType(getTableType(table));
		}
		catch (Exception e) {
			throw new ServiceException("Failed to import columns for table - " + table.getName(), e);
		}
	}
	
	public static TableType getTableType(com.hue.model.Table table) {
		List<Column> primaryKeyColumns = table.getPrimaryKeys();
		List<Column> foreignKeyColumns = table.getForeignKeys();
		List<Column> nonkeyColumns = table.getKeys(ColumnKeyType.NO_KEY_TYPE);

		if (primaryKeyColumns.size() == 1 && foreignKeyColumns.isEmpty()) {
			return TableType.DIMENSION;
		}
		else if (foreignKeyColumns.size() == table.getColumns().size()
				|| (foreignKeyColumns.size() > 0 && nonkeyColumns.isEmpty())) {
			return TableType.BRIDGE;
		}
		else if (foreignKeyColumns.size() > 0 && nonkeyColumns.size() > 0) {
			return TableType.FACT;
		}else if (foreignKeyColumns.isEmpty() && primaryKeyColumns.isEmpty()) {
			if(table.getDatasource() != null && table.getDatasource().getDatabaseType() == DBType.REDSHIFT) {
				return TableType.DIMENSION;
			}
			return TableType.FACT;
		}

		return TableType.DIMENSION;
	}
	
	public static ColumnKeyType getKeyType(schemacrawler.schema.Column column, Table table) {
		if (table.getPrimaryKey() != null
				&& table.getPrimaryKey().getColumns().stream().anyMatch(c -> column.getName().equals(c.getName()))) {
			return ColumnKeyType.PRIMARY_KEY;
		}
		else {
			for (ForeignKey foreignKey : table.getImportedForeignKeys()) {
				if (foreignKey.getColumnReferences().stream().anyMatch(
						c -> c.getForeignKeyColumn().getName().equals(column.getName()))) {
					return ColumnKeyType.FOREIGN_KEY;
				}
			}
		}

		return ColumnKeyType.NO_KEY_TYPE;
	}
	
	public static String quoteIdent(String physicalName, String q) {
		if(physicalName.contains(" ")){
			return q + physicalName + q;
		}else{
			return physicalName;
		}
	}

	// Only import tables based on options. No column and key information
	// loaded.
	// Be aware that how much information to retrieve by this method is
	// determined by options. By default,
	// options excluded column information.
	public static List<com.hue.model.Table> importTables(Datasource datasource, Connection connection, SchemaCrawlerOptions options)
			throws ServiceException {

		List<com.hue.model.Table> availableTables = new ArrayList<com.hue.model.Table>();
		schemaTables.clear();

		// Get the schema definition
		Catalog database;
		String q = "";
		try {
			database = SchemaCrawlerUtility.getCatalog(connection, options);
			q= connection.getMetaData().getIdentifierQuoteString();
		}
		catch (SchemaCrawlerException | SQLException e) {
			throw new ServiceException("Cannot crawl database catalog", e);
		}
		
		String databaseVendor = database.getDatabaseInfo().getProductName();
		if (databaseVendor == null) {
			databaseVendor = datasource.getDatabaseType().getVendorName();
		}
//		datasource.setDatabaseVendor(databaseVendor);
		String databaseVersion = database.getDatabaseInfo().getProductVersion();
		if (databaseVersion == null) {
			databaseVersion = "?";
		}
//		datasource.setDatabaseVersion(databaseVersion);
		
		
		for (Schema schema : database.getSchemas()) {
			logger.debug("Import schema - " + schema.getName());

			for (schemacrawler.schema.Table schemaTable : database.getTables(schema)) {
				com.hue.model.Table table = new com.hue.model.Table();
				table.setDatasource(datasource);
				table.setName(stripQuoteIdent(schemaTable.getName(),q));
				table.setPhysicalName(stripQuoteIdent(schemaTable.getName(),q));
				Schema tableSchema = schemaTable.getSchema();
				String schemaName = tableSchema.getName();
				if (schemaName == null) {
					schemaName = tableSchema.getFullName();
				}
				table.setSchemaName(schemaName);
				// System.err.println("Table name = " + schemaTable.getName());
				// if (schemaTable.getPrimaryKey() != null) {
				// schemaTable.getPrimaryKey().getColumns().forEach(c ->
				// System.err.println("Primary Key Column: " + c.getName()));
				// }
				//
				// if (schemaTable.getImportedForeignKeys().size() > 0) {
				// schemaTable.getImportedForeignKeys().forEach(fk ->
				// {System.err.println("Imported Foreign Key Name : " +
				// fk.getName());
				// fk.getColumnReferences().forEach(cr ->
				// System.err.println("Imported Foreign Key Column Reference : "
				// +
				// cr.getForeignKeyColumn().getParent().getName() + "." +
				// cr.getForeignKeyColumn().getName() + " "
				// + cr.getPrimaryKeyColumn().getParent().getName() + "." +
				// cr.getPrimaryKeyColumn().getName()));
				// });
				// }
				//
				// if (schemaTable.getExportedForeignKeys().size() > 0) {
				// schemaTable.getExportedForeignKeys().forEach(fk ->
				// {System.err.println("Exported Foreign Key Name : " +
				// fk.getName());
				// fk.getColumnReferences().forEach(cr ->
				// System.err.println("Exported Foreign Key Column References :
				// "
				// + cr.getForeignKeyColumn().getParent().getName() + "." +
				// cr.getForeignKeyColumn().getName()
				// + " " + cr.getPrimaryKeyColumn().getParent().getName() + "."
				// + cr.getPrimaryKeyColumn().getName()));
				// });
				// }
				//
				// if (schemaTable.getPrivileges().size() > 0) {
				// schemaTable.getPrivileges().forEach(p ->
				// System.err.println("Table Privileges" + p.getFullName()));
				// }
				// TH 04/02/2015, no longer load column information here.
				// for (schemacrawler.schema.Column schemaColumn :
				// schemaTable.getColumns()) {
				// Column column = new Column();
				// column.setName(schemaColumn.getName());
				// column.setDataType(schemaColumn.getColumnDataType().getDatabaseSpecificTypeName());
				// column.setKeyType(UIUtils.getKeyType(schemaColumn,
				// schemaTable));
				// column.setNullable(schemaColumn.isNullable());
				//
				// table.addColumn(column);
				// }

				// table.setTableType(UIUtils.getTableType(table));

				// TH 09/22/2014, do not load row count when import tables
				// ResultSet rs =
				// connection.createStatement().executeQuery(String.format(ROW_COUNT_QUERY,
				// schemaTable.getName()));
				// rs.next();
				// table.setRowCount(rs.getInt(1));

				availableTables.add(table);
				// schemaTables.put(table.getName(), schemaTable);
			}
		}

		return availableTables;
	}
	
	private static String stripQuoteIdent(String name, String q) {
		String pattern = "^\\" +  q + "|\\.\\" + q + "|\\" + q +"$";
		return name.replaceAll(pattern, "");
	}

	public static String getDefaultSchema(Datasource datasource, List<String> schemas) throws ServiceException {
		if (schemas == null || schemas.isEmpty()) throw new ServiceException("No schema found in target database");
		
		String schemaName = null;
		
		if (datasource.getDatabaseType() == DBType.ORACLE) {
			schemaName = datasource.getUserName();
		}
		else if (datasource.getDatabaseType() == DBType.MSSQL
				|| datasource.getDatabaseType() == DBType.AZURE) {
			schemaName = "dbo";
		}
		else if (datasource.getDatabaseType() == DBType.POSTGRESQL || datasource.getDatabaseType() == DBType.REDSHIFT || datasource.getDatabaseType() == DBType.VERTICA) {
			schemaName = "public";
		}
		else {
			schemaName = datasource.getDatabaseName();
		}
		
		// go through given schema list to find out exact name in database. If it doesn't exist, pick the first
		// one as default schema name
		final String name = schemaName;
		Optional<String> result = schemas.stream().filter(n -> n.equalsIgnoreCase(name)).findAny();
		
		if (result.isPresent()) {
			schemaName = result.get();
		}
		else {
			schemaName = schemas.get(0);
		}
		
		return schemaName;
	}
	
	public static List<com.hue.model.Table> importTablesForSchemas(Datasource datasource, List<String> schemaNames) throws ServiceException {
		try (Connection connection = ConnUtils.getConnection(datasource)) {
			// Create the options
			final SchemaCrawlerOptions options = getSchemaCrawlerOptions(datasource);
			options.setSchemaInclusionRule(createSchemaInclusionRule(datasource, schemaNames));

			return importTables(datasource, connection, options);
		}
		catch (SQLException e) {
			throw new ServiceException("Database exception", e);
		}
		catch (SshException e) {
			throw new ServiceException("SSH connection failed.", e);
		}
		catch (ClassNotFoundException e) {
			throw new ServiceException(e);
		}
		catch (IOException e) {
			throw new ServiceException(e);
		}
	}
	
	public static List<com.hue.model.Table> importTablesForSchema(Datasource datasource, String schemaName) throws ServiceException {
		return importTablesForSchemas(datasource, Arrays.asList(schemaName));
	}

	public static SchemaCrawlerOptions getSchemaCrawlerOptions(Datasource datasource) {
		final SchemaCrawlerOptions options = new SchemaCrawlerOptions();
		// Set what details are required in the schema - this affects the
		// time taken to crawl the schema
		SchemaInfoLevel schemaInfoLevel = SchemaInfoLevelBuilder.minimum();
		schemaInfoLevel.setRetrieveAdditionalJdbcDriverInfo(false);
		schemaInfoLevel.setRetrieveRoutines(false);
		schemaInfoLevel.setRetrieveColumnDataTypes(false);
		schemaInfoLevel.setRetrieveTableColumns(false);
		schemaInfoLevel.setRetrieveForeignKeys(true);

		options.setSchemaInfoLevel(schemaInfoLevel);

		DBType dbType = datasource.getDatabaseType();

		if (dbType == DBType.REDSHIFT) {
			options.getSchemaInfoLevel().setRetrieveForeignKeys(false);
		}

		switch (dbType) {
		case POSTGRESQL:
		case REDSHIFT:
			// options.setSchemaInclusionRule(new
			// RegularExpressionExclusionRule(Pattern.compile(
			// "INFORMATION_SCHEMA|PG_CATALOG|PG_INTERNAL",
			// Pattern.CASE_INSENSITIVE)));
			break;
		case MYSQL:
			// options.setSchemaInclusionRule(new
			// RegularExpressionExclusionRule(Pattern.compile("INFORMATION_SCHEMA",
			// Pattern.CASE_INSENSITIVE)));
			break;
		case MSSQL:
		case AZURE:
			options.setSchemaInclusionRule(new RegularExpressionInclusionRule(Pattern.compile(
					String.format("%s\\..*", datasource.getDatabaseName()), Pattern.CASE_INSENSITIVE)));
			break;
		case TERADATA:
			// options.setSchemaInclusionRule(new
			// RegularExpressionExclusionRule(Pattern.compile(
			// "\"All\"|Crashdumps|\"DBC\"|EXTUSER|SQLJ|SYSLIB|SYSSPATIAL|SYSUDTLIB|SysAdmin|Sys_Calendar|SystemFe|TDPUSER|TDStats|TD_SYSFNLIB|dbcmngr|tdwm",
			// Pattern.CASE_INSENSITIVE)));
			// options.setSchemaInclusionRule(new
			// RegularExpressionInclusionRule(Pattern.compile(
			// datasource.getDatabaseName(), Pattern.CASE_INSENSITIVE)));
			break;
		case DERBY_LOCAL:
		case DERBY_REMOTE:
			options.setSchemaInclusionRule(
					new RegularExpressionExclusionRule(Pattern.compile("SYS", Pattern.CASE_INSENSITIVE)));
			break;
		case ORACLE:
			// options.setSchemaInclusionRule(new
			// RegularExpressionExclusionRule(Pattern.compile(
			// "ANONYMOUS|APEX_PUBLIC_USER|CTXSYS|DBSNMP|DIP|EXFSYS|FLOWS_%|FLOWS_FILES|LBACSYS|MDDATA|MDSYS|MGMT_VIEW|OLAPSYS|ORACLE_OCM|ORDDATA|ORDPLUGINS|ORDSYS|OUTLN|OWBSYS|SI_INFORMTN_SCHEMA|SPATIAL_CSW_ADMIN_USR|SPATIAL_WFS_ADMIN_USR|SYS|SYSMAN|SYSTEM|WKPROXY|WKSYS|WK_TEST|WMSYS|XDB|XS$NULL|APPQOSSYS|RDSADMIN|\"SYSTEM\"|DBSNMP|CTXSYS|DIP|SYS|OUTLN",
			// Pattern.CASE_INSENSITIVE)));
			break;
		case HIVE:
			schemaInfoLevel.setRetrieveForeignKeys(false);
			schemaInfoLevel.setRetrieveColumnDataTypes(false);
			schemaInfoLevel.setRetrieveDatabaseInfo(false);
//			schemaInfoLevel.setRetrieveAdditionalSchemaCrawlerInfo(false);
			schemaInfoLevel.setRetrieveAdditionalJdbcDriverInfo(false);
			break;
		case VERTICA:
		case ACCESS:
		case UNKNOWN:
		default:
			break;
		}
		return options;
	}

	public static boolean testConnection(Datasource datasource) throws Exception {
		try (Connection connection = ConnUtils.getSchemaCrawlerConnection(datasource)) {
			DBType dbType = datasource.getDatabaseType();

			String query = null;
			switch (dbType) {
			default:
			case POSTGRESQL:
			case MSSQL:
			case AZURE:
			case TERADATA:
			case MYSQL:
			case ACCESS:
			case HIVE:
			case REDSHIFT:
			case VERTICA:
			case UNKNOWN:
				query = TEST_QUERY;
				break;
			case DERBY_LOCAL:
			case DERBY_REMOTE:
				query = TEST_QUERY_DERBY;
				break;
			case ORACLE:
				query = TEST_QUERY_ORACLE;
				break;
			}
			connection.createStatement().execute(query);

			return true;
		}
		catch (Exception e) {
			throw e;
		}
	}

	// This method have to be called after importSchema() method
	public static List<Join> getJoinDefs(List<com.hue.model.Table> tables) {
		List<Join> joinDefs = new ArrayList<Join>();

		tables.forEach(t -> {
			schemacrawler.schema.Table rightSchemaTable = schemaTables.get(t.getName());
			// TH 06/15/2015, for adding additional table case, schemaTables
			// does not contain the tables which
			// already loaded.
			if (rightSchemaTable != null) {
				rightSchemaTable.getImportedForeignKeys().forEach(fk -> {
					schemacrawler.schema.Table leftSchemaTable = fk.getColumnReferences().get(
							0).getPrimaryKeyColumn().getParent();

					// ignore the self join
					if (!rightSchemaTable.equals(leftSchemaTable)) {
						Optional<com.hue.model.Table> leftTable = tables.stream().filter(
								t1 -> t1.getName().equals(leftSchemaTable.getName())).findFirst();
						if (leftTable.isPresent()) {
							// create join def
							Join joinDef = new Join();

							joinDef.setCardinalityType(CardinalityType.ONE_TO_MANY);
							joinDef.setJoinType(JoinType.INNER_JOIN);
							joinDef.setLeft(leftTable.get());
							joinDef.setRight(t);

							final StringBuffer formula = new StringBuffer("");
							fk.getColumnReferences().stream().filter(
									cr -> cr.getForeignKeyColumn().getParent().equals(rightSchemaTable)).forEach(cr -> {
								if (!formula.toString().equals("")) {
									formula.append(" and ");
								}

								formula.append(CommonUtils.getOptionallyQuotedName(leftTable.get().getName()) + ".")
									.append(CommonUtils.getOptionallyQuotedName(cr.getPrimaryKeyColumn().getName()))
									.append(" = ").append(CommonUtils.getOptionallyQuotedName(t.getName()) + ".")
									.append(CommonUtils.getOptionallyQuotedName(cr.getForeignKeyColumn().getName()));
							});

							joinDef.setSql(formula.toString());

							joinDefs.add(joinDef);
						}
					}
				});
			}
		});

		if (logger.isInfoEnabled()) {
			StringBuffer sb = new StringBuffer();
			joinDefs.forEach(jd -> sb.append(((Table) jd.getLeft()).getName()).append(" ").append(
					jd.getJoinType()).append(" ").append(((Table) jd.getRight()).getName()).append(" ").append(
							jd.getCardinalityType()).append(" ").append(jd.getSql()).append(
									System.lineSeparator()));
			logger.info(sb.toString());
		}
		return joinDefs;
	}

	public static RegularExpressionInclusionRule createSchemaInclusionRule(Datasource datasource, List<String> schemaNames) {
		RegularExpressionInclusionRule inclusionRule = null;
		
		if (datasource.getDatabaseType() == DBType.MSSQL || datasource.getDatabaseType() == DBType.REDSHIFT 
				|| datasource.getDatabaseType() == DBType.VERTICA || datasource.getDatabaseType() == DBType.AZURE) {
			String rule = schemaNames.stream().map(n -> String.format("%s\\.%s", datasource.getDatabaseName(), n)).collect(Collectors.joining("|"));
			inclusionRule = new RegularExpressionInclusionRule(Pattern.compile(rule, Pattern.CASE_INSENSITIVE));
		}
		else {
			inclusionRule = new RegularExpressionInclusionRule(Pattern.compile(String.join("|", schemaNames), Pattern.CASE_INSENSITIVE));
		}
		
		return inclusionRule;
	}
	
	public static RegularExpressionInclusionRule createSchemaInclusionRule(Datasource datasource, String schemaName) {
		return createSchemaInclusionRule(datasource, Arrays.asList(schemaName));
	}
	
	public static Map<String, Object> importDbProps(Datasource datasource) throws ServiceException {
		Map<String, Object> dbProps = new HashMap<String, Object>();
		
		try (Connection connection = ConnUtils.getSchemaCrawlerConnection(datasource)) {
			DatabaseMetaData metaData = connection.getMetaData();
			Method[] methods = metaData.getClass().getMethods();
			
			for (Method method : methods) {				
				if (method.getParameterCount() == 0 && !EXCLUDED_METHODS.contains(method.getName()) && !Modifier.isAbstract(method.getModifiers())
					&& (method.getReturnType() == int.class || method.getReturnType() == boolean.class || method.getReturnType() == String.class)) {
					try {
						dbProps.put(method.getName(), method.invoke(metaData));
					}
					catch (Exception e) {
						logger.error("Failed to import property - " + method.getName());
					}
				}
			}
			
//			dbProps.forEach((k, v) -> System.out.println("Key = " + k + " v = " + v));
		}
		catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new ServiceException(e);
		}
		
		return dbProps;
	}
}
