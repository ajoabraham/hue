package com.hue.utils;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.JDBCType;
import java.text.Normalizer;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.LoggerFactory;

import com.hue.common.DBType;
import com.hue.common.DataType;

public final class CommonUtils {	
	private static final String ENCODING = StandardCharsets.UTF_8.toString();
	// The last 8 bits are all one
	private static final int MASK = 255;

	private CommonUtils() {
	}

	// TH 09/24/2015, all formula related methods are moved into FormulaUtils
	// under
	// model project. Since there is one dependency on this method in common
	// package,
	// this method has to be duplicated here.
	public static String[] parseBlockDerivedName(String blockDerivedName) {
		// All possible valid case
		// @blockname.dimensionname
		// @blockname.[dimensionname]
		// @[blockname].dimensionname
		// @[blockname].[dimensionname]
		// @dimensionname
		// @[dimensionname]
		// @this.dimensionname
		// @this.[dimensionname]
		// @[this].dimensionname
		// @[this].[dimensionname]
		// @[block.name].[dimension.name]
		// @blockname.[dimension.name]
		// @[dimension.name]

		int dotIndex = -1;
		// if formula starts with "@[", then look for a dot after first ]
		if (blockDerivedName.matches("^@\\s*?\\[.+")) {
			int closeSquareIndex = blockDerivedName.indexOf("]");
			dotIndex = blockDerivedName.indexOf(".", closeSquareIndex);
		}
		else {
			// if formula starts with @ only, then look for first dot
			dotIndex = blockDerivedName.indexOf(".");
		}

		String[] names = new String[2];

		if (dotIndex == -1) {
			names[0] = "this";
			names[1] = extractObjectName(blockDerivedName);
		}
		else {
			names[0] = extractObjectName(blockDerivedName.substring(0, dotIndex));
			names[1] = extractObjectName(blockDerivedName.substring(dotIndex + 1, blockDerivedName.length()));
		}

		return names;
	}

	// Extract out name from @[objectName] format.
	private static String extractObjectName(String objectName) {
		int startIndex = objectName.indexOf("[");
		if (startIndex == -1) {
			startIndex = objectName.indexOf("@");
		}

		int endIndex = objectName.indexOf("]");
		if (endIndex == -1) {
			endIndex = objectName.length();
		}

		return objectName.substring(startIndex + 1, endIndex);
	}

	public static String createPercentageString(double n1, double n2) {
		return "" + (int) ((n1 / n2) * 100) + "%";
	}

	// No longer use this method to parse intersect/exclude function. Using
	// SetBlockParser instead.
	// public static List<String> parseIntersectExcludeFormula(String formula) {
	// List<String> retList = new ArrayList<String>();
	// // use split ("on", 2) in case there is partial string like "on" after
	// the first occurance. (ex. Contact)
	// String[] split = formula.split(" on ", 2);
	//
	// Pattern pattern1 =
	// Pattern.compile("(include|exclude)\\(\\s?(@\\[[^\\]]+\\])\\)\\s?");
	// Matcher matcher1 = pattern1.matcher(split[0]);
	// while (matcher1.find()) {
	// // System.out.println("matcher1.group(): " + matcher1.group(0) + " " +
	// matcher1.group(1) + " " + matcher1.group(2));
	// retList.add(matcher1.group(1));
	// retList.add(matcher1.group(2));
	// }
	//
	// Pattern pattern2 = Pattern.compile("\\s?(@\\[[^\\]]+\\])");
	// Matcher matcher2 = pattern2.matcher(split[1]);
	// while (matcher2.find()) {
	// // System.out.println("matche2r.group(): " + matcher2.group(0) + " " +
	// matcher2.group(1));
	// retList.add(matcher2.group(1));
	// }
	//
	// return retList;
	// }

	public static String getMD5HashValue(String input) throws Exception {
		MessageDigest md5 = MessageDigest.getInstance("MD5");
		return toHexString(md5.digest(input.getBytes(ENCODING)));
	}

	private static String toHexString(byte[] bArray) {
		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < bArray.length; i++) {
			sb.append(Integer.toHexString(bArray[i] & MASK));
		}

		return sb.toString();
	}

	public static Boolean equalsWithIgnoreCaseQuotes(String source, String target) {
		if ((source == null) && (target == null)) {
			return true;
		}
		else if ((source == null) || (target == null)) {
			return false;
		}
		else {
			return source.replaceAll("\"", "").equalsIgnoreCase(target.replaceAll("\"", ""));
		}
	}

	public static Boolean endsWithIgnoreCaseQuotes(String source, String target) {
		if ((source == null) && (target == null)) {
			return true;
		}
		else if ((source == null) || (target == null)) {
			return false;
		}
		else {
			return source.replaceAll("\"", "").endsWith(target.replaceAll("\"", ""));
		}
	}

	public static String removeQuotes(String input) {
		if (input.trim().matches("\".*\"")) {
			return input.trim().substring(1, input.trim().length() - 1);
		}
		else {
			return input;
		}
	}

	public static String singleQuotesInput(String input) {
		if (isBlank(input))
			return null;

		String output = input.trim();
		if (!output.startsWith("'")) {
			output = "'" + input;
		}

		if (!output.endsWith("'")) {
			output = output + "'";
		}

		return output;
	}

	public static String singleQuotesInputList(String list) {
		if (isBlank(list))
			return null;

		String output = list.trim();

		if (!output.contains("'")) {
			StringBuffer sb = new StringBuffer();
			String[] inputs = output.split(",");

			for (int i = 0; i < inputs.length; i++) {
				if (i > 0) {
					sb.append(",");
				}
				sb.append(singleQuotesInput(inputs[i]));
			}

			output = sb.toString();
		}

		return output;
	}

	public static String removeSingleQuotes(String input) {
		if (isBlank(input))
			return null;

		String output = input.trim();

		if (output.startsWith("'")) {
			output = output.substring(1, output.length());
		}

		if (output.endsWith("'")) {
			output = output.substring(0, output.length() - 1);
		}

		return output;

	}

	public static boolean isNumber(String input) {
		return NumberUtils.isNumber(input);
	}

	public static boolean isInteger(String input) {
		try {
			Integer.parseInt(input);
			return true;
		}
		catch (Exception e) {
			return false;
		}
	}

	public static String removeBackTicks(String input) {
		if (input.trim().matches("`.*`")) {
			return input.trim().substring(1, input.trim().length() - 1);
		}
		else {
			return input;
		}
	}

	public static boolean isWindows() {
		String os = System.getProperty("os.name").toLowerCase();
		return os.contains("win");
	}

	public static boolean isLinux() {
		String os = System.getProperty("os.name").toLowerCase();
		return os.contains("linux");
	}

	public static boolean isBlank(String str) {
		return str == null || str.trim().equals("");
	}

	public static DBType stringToDBType(String dbType) {
		DBType outDbType = DBType.UNKNOWN;

		switch (dbType) {
		case "POSTGRESQL":
			outDbType = DBType.POSTGRESQL;
			break;
		case "MSSQL":
			outDbType = DBType.MSSQL;
			break;
		case "AZURE":
			outDbType = DBType.AZURE;
			break;
		case "TERADATA":
			outDbType = DBType.TERADATA;
			break;
		case "MYSQL":
			outDbType = DBType.MYSQL;
			break;
		case "ORACLE":
			outDbType = DBType.ORACLE;
			break;
		case "EXCEL":
		case "DERBY_LOCAL":
		case "DERBY_REMOTE":
			outDbType = DBType.DERBY_LOCAL;
			break;
		case "REDSHIFT":
			outDbType = DBType.REDSHIFT;
			break;
		case "HIVE":
			outDbType = DBType.HIVE;
			break;
		case "ACCESS":
			outDbType = DBType.ACCESS;
			break;
        case "VERTICA":
            outDbType = DBType.VERTICA;
            break;
		default:
			break;
		}

		return outDbType;
	}

	public static int convertToInteger(String input, int defaultResult) {
		try {
			return Integer.parseInt(input);
		}
		catch (Exception e) {
		}

		return defaultResult;
	}

	public static boolean containsPassword(Throwable e, String userName, String password) {
		if (!isBlank(e.getMessage())) {
			String message = e.getMessage().toLowerCase();
			if (message.contains("password")) {
				return true;
			}

			if (userName != null && message.contains(userName)) {
				return true;
			}

			if (password != null && message.contains(password)) {
				return true;
			}
		}

		if (e.getCause() != null) {
			return containsPassword(e.getCause(), userName, password);
		}

		return false;
	}

	public static boolean doesPortForwardingAlreadyExist(Throwable e) {
		String alreadyBindMessage = "Address already in use".toLowerCase();

		if (!isBlank(e.getMessage())) {
			String message = e.getMessage().toLowerCase();
			if (message.contains(alreadyBindMessage)) {
				return true;
			}
		}

		if (e.getCause() != null) {
			return doesPortForwardingAlreadyExist(e.getCause());
		}

		return false;
	}

	public static String sanitizeMessage(Throwable e, String userName, String password) {
		StringWriter writer = new StringWriter();
		e.printStackTrace(new PrintWriter(writer));

		String message = writer.toString();

		if (!isBlank(userName)) {
			message = message.replace(userName, generateMask(userName.length()));
		}

		if (!isBlank(password)) {
			message = message.replace(password, generateMask(password.length()));
		}

		return message;
	}

	private static String generateMask(int length) {
		return StringUtils.leftPad("", length, "*");
	}

	public static String getJavaSqlTypeName(int type) {
		try {
			return JDBCType.valueOf(type).getName();
		}
		catch (Exception e) {
			LoggerFactory.getLogger(CommonUtils.class.getName()).error("Unknown JDBC type - " + type);
			return "UNKNOWN";
		}
	}

	// To determine an expressible subtype
	public static DataType mapToDataType(String javaTypeName, String javaSqlTypeName,
			String dbTypeName) {
		DataType subtype = DataType.TEXT;

		if (!isBlank(dbTypeName)) {
			String name = dbTypeName.toLowerCase();

			if (name.contains("date")) {
				subtype = DataType.DATE;
			}
			else if (name.contains("time")) {
				subtype = DataType.TIMESTAMP;
			}
			else if (name.contains("int") 
					|| name.contains("short")
					|| name.equalsIgnoreCase("serial")) {
				subtype = DataType.INTEGER;
			}
			else if (name.contains("bigint") || name.contains("bigserial")) {
				subtype = DataType.BIGINT;
			}
			else if (name.contains("float") || name.contains("numeric") || name.contains("real")
					|| name.contains("double")) {
				subtype = DataType.DECIMAL;
			}
			else if (name.contains("char") 
					|| name.contains("text") 
					|| name.contains("clob")
					|| name.contains("json")
					|| name.contains("inet")
					|| name.contains("xml")
					|| name.contains("uuid")) {
				subtype = DataType.TEXT;
			}
			else if (name.contains("bool") || name.contains("bit")) {
				subtype = DataType.BOOLEAN;
			}
			else {
				subtype = DataType.UNSUPPORTED;
				LoggerFactory.getLogger(CommonUtils.class.getName()).error("Unknown DB Type Name - " + dbTypeName);
			}
		}

		if (subtype == null && !isBlank(javaTypeName)) {
			if (javaTypeName.equalsIgnoreCase(String.class.getName())
					|| javaTypeName.equalsIgnoreCase(java.sql.Clob.class.getName())) {
				subtype = DataType.TEXT;
			}
			else if (javaTypeName.equalsIgnoreCase(java.math.BigDecimal.class.getName())
					|| javaTypeName.equalsIgnoreCase(java.lang.Byte.class.getName())
					|| javaTypeName.equalsIgnoreCase(byte.class.getName())
					|| javaTypeName.equalsIgnoreCase(java.lang.Long.class.getName())
					|| javaTypeName.equalsIgnoreCase(long.class.getName())
					|| javaTypeName.equalsIgnoreCase(java.lang.Float.class.getName())
					|| javaTypeName.equalsIgnoreCase(float.class.getName())
					|| javaTypeName.equalsIgnoreCase(java.lang.Double.class.getName())
					|| javaTypeName.equalsIgnoreCase(double.class.getName())) {
				subtype = DataType.DECIMAL;
			}
			else if (javaTypeName.equalsIgnoreCase(java.lang.Short.class.getName())
					|| javaTypeName.equalsIgnoreCase(short.class.getName())
					|| javaTypeName.equalsIgnoreCase(java.lang.Integer.class.getName())
					|| javaTypeName.equalsIgnoreCase(int.class.getName())) {
				subtype = DataType.INTEGER;
			}
			else if (javaTypeName.equalsIgnoreCase(java.sql.Date.class.getName())) {
				subtype = DataType.DATE;
			}
			else if (javaTypeName.equalsIgnoreCase(java.sql.Time.class.getName())
					|| javaTypeName.equalsIgnoreCase(java.sql.Timestamp.class.getName())) {
				subtype = DataType.TIMESTAMP;
			}
			else if (javaTypeName.equalsIgnoreCase(java.lang.Boolean.class.getName())
					|| javaTypeName.equalsIgnoreCase(boolean.class.getName())) {
				subtype = DataType.BOOLEAN;
			}
			else {
				LoggerFactory.getLogger(CommonUtils.class.getName()).error("Unknown type - " + javaTypeName);
			}
		}

		if (subtype == null && !isBlank(javaSqlTypeName)) {
			try {
				JDBCType sqlType = JDBCType.valueOf(javaSqlTypeName);

				switch (sqlType) {
				case BIT:
				case BOOLEAN:
					subtype = DataType.BOOLEAN;
					break;
				case DATE:
					subtype = DataType.DATE;
					break;
				case TIME:
				case TIME_WITH_TIMEZONE:
				case TIMESTAMP:
				case TIMESTAMP_WITH_TIMEZONE:
					subtype = DataType.TIMESTAMP;
					break;
				case BIGINT:
					subtype = DataType.BIGINT;
					break;
				case DECIMAL:
				case DOUBLE:
				case FLOAT:				
				case NUMERIC:
				case REAL:
					subtype = DataType.DECIMAL;
					break;
				case INTEGER:
				case SMALLINT:
				case TINYINT:
					subtype = DataType.INTEGER;
					break;
				case CHAR:
				case CLOB:
				case LONGNVARCHAR:
				case LONGVARCHAR:
				case NCHAR:
				case NCLOB:
				case NVARCHAR:
				case VARCHAR:
					subtype = DataType.TEXT;
					break;
				default:
					subtype = DataType.TEXT;
					LoggerFactory.getLogger(CommonUtils.class.getName()).error(
							"Unknown Java SQL Type - " + javaSqlTypeName);
				}
			}
			catch (Exception e) {
				LoggerFactory.getLogger(CommonUtils.class.getName()).error(
						"Unknown Java SQL Type - " + javaSqlTypeName);
			}

		}

		return subtype == null ? DataType.TEXT : subtype;
	}

	public static String convertToOrientDbUrl(String path) {
		return path.replace("/", "$").replace("\\", "$");
	}

//	public static Charset detectEncoding(File file) {
//		try (InputStream in = new FileInputStream(file); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
//			byte[] data = new byte[1024];
//			in.read(data);
//			out.write(data);
//
//			CharsetDetector detector = new CharsetDetector();
//			detector.setText(data);
//			CharsetMatch match = detector.detect();
//
//			if (match != null) {
//				return Charset.forName(match.getName());
//			}
//		}
//		catch (Exception e) {
//		}
//
//		return Charset.defaultCharset();
//	}

	private static String VALID_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789_";
	private static String INVALID_FIRST_CHARS = "0123456789$";
	private static String REPLACEMENT_CHAR = "_";

	public static String normalizeString(String input) {
		// Contain only ASCII letters, digits, or underscore characters (_).
		// Begin with an alphabetic character or underscore character.
		// Subsequent characters may include letters, digits, underscores.
		// Be between 1 and 127 characters in length, not including quotes for
		// delimited identifiers.
		// Contain no quotation marks and no spaces.
		StringBuffer sb = new StringBuffer();
		// To lowercase and remove all extra whitespaces.
		input = StringUtils.normalizeSpace(input.toLowerCase());
		for (char c : input.toCharArray()) {
			if (VALID_CHARS.contains(String.valueOf(c))) {
				if (sb.length() == 0 && INVALID_FIRST_CHARS.contains(String.valueOf(c))) {
					sb.append(REPLACEMENT_CHAR);
				}
				else {
					sb.append(c);
				}
			}
			else {
				sb.append(REPLACEMENT_CHAR);
			}
		}

		String normalizedName = sb.toString();
		// Remove leading and trailing underscore and multiple underscores
		normalizedName = StringUtils.replacePattern(normalizedName, "_{2,}", REPLACEMENT_CHAR);
		normalizedName = StringUtils.strip(normalizedName, REPLACEMENT_CHAR);

		return normalizedName.length() > 127 ? normalizedName.substring(0, 127) : normalizedName;
	}

//	public static boolean isProductionMode() {
//		return BuildInfo.getAppMode() == AppMode.PRODUCTION_MODE;
//	}
	
	public static boolean isTrackingOn() {
		return true;
	}

	public static String convertStackTraceToString(Exception e) {
		try (StringWriter writer = new StringWriter()) {
			e.printStackTrace(new PrintWriter(writer));
			return writer.toString();
		}
		catch (Exception ex) {
		}

		return "";
	}

	public static String generateSlug(String name) {
		return Normalizer.normalize(name == null ? "" : name.toLowerCase(), Normalizer.Form.NFD).replaceAll(
				"[^\\p{ASCII}]", "").replaceAll("[^\\w+]", "_").replaceAll("\\s+", "_").replaceAll("[-]+",
						"_").replaceAll("^-", "").replaceAll("-$", "");
	}

	public static boolean containsIgnoreCase(List<String> values, String value) {
		return values.stream().filter(v -> v.equalsIgnoreCase(value)).findAny().isPresent();
	}
	
	// Sample size = 500KB
	public static final int SAMPLE_SIZE = 512000;
	public static String sampleFile(File file) throws IOException {
		StringBuilder buffer = new StringBuilder();
		try (BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
			for (int i = 0; i < SAMPLE_SIZE; ++i) {
				buffer.append((char) in.read());
			}
		}
		return buffer.toString();
	}
	
	public static long rowCount(File file) {
		long rowCount = 0;
		
		try (BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {			
			while (in.readLine() != null) {
				rowCount++;
			}
		}
		catch (Exception e) {
			rowCount = -1;
		}
		
		return rowCount;
	}

//	public static FileType determineFileType(File file) {
//		String extension = FilenameUtils.getExtension(file.getName());
//		FileType fileType = null;
//
//		switch (extension) {
//		case "csv":
//			fileType = FileType.CSV;
//			break;
//		case "xlsx":
//		case "xls":
//			fileType = FileType.EXCEL;
//			break;
//		case "tsv":
//			fileType = FileType.TDF;
//			break;
//		case "txt":
//		case "dat":
//		case "log":
//			fileType = FileType.TEXT;
//			break;
//		}
//
//		return fileType;
//	}

//	private static final String NEWLINE = "[\r]?\n";
//	public static char detectDelimiter(final String source, final boolean isCsv) {
//		char c;
//		boolean inQuotes = false;
//		LineDelimiters delimiters = new LineDelimiters();
//
//		for (String line : source.split(NEWLINE)) {
//			// ignore comments and empty lines
//			if (CommonUtils.isBlank(line) || line.trim().charAt(0) == CSVDefaultSettings.COMMENT_MARKER_CHAR) { 
//				continue;
//			}
//			else {
//				for (int i = 0; i < line.length(); ++i) {
//					c = line.charAt(i);
//					switch (c) {
//    					case ',':
//    					case ';':
//    					case '|':
//    					case ' ':
//    					case '\t': {
//    						if (!inQuotes) {
//    							delimiters.increment(c);
//    						}
//    						break;
//    					}
//    					case '"': {
//    						inQuotes = !inQuotes;
//    						break;
//    					}
//					}
//				}
//			}
//		}
//
//		char delimiter = delimiters.max();
//		if (isCsv && delimiter == TextDelimiter.SPACE.charValue()) {
//			delimiter = delimiters.maxWithoutSpace();
//		}
//
//		return delimiter;
//	}

//	public static char detectQuoteCharacter(final String source, final boolean isCsv) {
//		char quotingCharacter = '"';
//
//		char c;
//		int singleQuotes = 0;
//		int doubleQuotes = 0;
//		for (String line : source.split(NEWLINE)) {
//			// ignore comments and empty lines
//			if (CommonUtils.isBlank(line) || line.trim().charAt(0) == CSVDefaultSettings.COMMENT_MARKER_CHAR) { 
//				continue;
//			}
//			else {
//    			for (int i = 0; i < line.length(); ++i) {
//    				c = line.charAt(i);
//    				switch (c) {
//        				case '"': {
//        					doubleQuotes++;
//        					break;
//        				}
//        				case '\'': {
//        					singleQuotes++;
//        					break;
//        				}
//    				}
//    			}
//			}
//		}
//
//		if (singleQuotes == 0 && doubleQuotes == 0) {
//			quotingCharacter = '\0';
//		}
//		else if (singleQuotes > doubleQuotes && (singleQuotes % 2 == 0)) {
//			quotingCharacter = '\'';
//		}
//
//		// TH 03/09/2016, if this is a CSV file and quote character did not
//		// detect, just use double
//		// quote as default.
//		if (isCsv && quotingCharacter == '\0') {
//			quotingCharacter = '"';
//		}
//
//		return quotingCharacter;
//	}
	
//	private static final class Delimiter {
//		private final char delimiter;
//		private int count;
//
//		Delimiter(char delimiter) {
//			this.delimiter = delimiter;
//		}
//
//		public static Delimiter newInstance(char delimiter) {
//			return new Delimiter(delimiter);
//		}
//
//		public int increment() {
//			this.count++;
//			return this.count;
//		}
//
//		public int count() {
//			return this.count;
//		}
//
//		public char delimiter() {
//			return this.delimiter;
//		}
//
//		@Override
//		public String toString() {
//			return "{" + "delimiter='" + delimiter + "', count=" + count + '}';
//		}
//	}

//	private static final class LineDelimiters {
//		final Map<Character, Delimiter> delimiters = Maps.newHashMap();
//
//		public void increment(final char c) {
//			Delimiter delimiter = delimiters.get(c);
//			if (delimiter == null) {
//				delimiter = Delimiter.newInstance(c);
//			}
//			delimiter.increment();
//			delimiters.put(c, delimiter);
//		}
//
//		public char max() {
//			Optional<Delimiter> max = delimiters.values().stream().max(
//					(delimiter1, delimiter2) -> Integer.compare(delimiter1.count(), delimiter2.count()));
//
//			if (max.isPresent()) {
//				return max.get().delimiter();
//			}
//			else {
//				return '\0';
//			}
//		}
//
//		public char maxWithoutSpace() {
//			Optional<Delimiter> max = delimiters.values().stream().filter(d -> d.delimiter() != ' ').max(
//					(delimiter1, delimiter2) -> Integer.compare(delimiter1.count(), delimiter2.count()));
//
//			if (max.isPresent()) {
//				return max.get().delimiter();
//			}
//			else {
//				return '\0';
//			}
//		}
//	}
	
	public static String getImageFileExtension(String fileName) {
		String extension = "png";
		if (!isBlank(fileName) && !fileName.trim().endsWith(".") && fileName.lastIndexOf(".") != -1) {
			extension = fileName.substring(fileName.lastIndexOf(".") + 1, fileName.length()).trim();
			
			if (!extension.equalsIgnoreCase("png") && !extension.equalsIgnoreCase("jpg") && !extension.equalsIgnoreCase("gif")) {
				extension = "png";
			}
		}
		
		return extension;
	}
	
	private static final int BUFFER_SIZE = 4096;
	public static void unzipFile(File basePath, String fileName) throws Exception {
		InputStream is = CommonUtils.class.getResourceAsStream(fileName);
		if (is == null)
			throw new Exception("Cannot find defaultProject.zip in classpath");

		try (ZipInputStream zis = new ZipInputStream(is)) {
			ZipEntry entry = zis.getNextEntry();

			while (entry != null) {
				File file = new File(basePath, entry.getName());
				if (!entry.isDirectory()) {
					try (FileOutputStream fos = new FileOutputStream(file);
							BufferedOutputStream bos = new BufferedOutputStream(fos)) {
						byte[] bytesIn = new byte[BUFFER_SIZE];
						int read;
						while ((read = zis.read(bytesIn)) != -1) {
							bos.write(bytesIn, 0, read);
						}
					}
				}
				else {
					file.mkdir();
				}

				zis.closeEntry();
				entry = zis.getNextEntry();
			}
		}
	}
	
	public static String getOptionallyQuotedName(String name){
		if(name.contains(" ")){
			name = "\""+  name + "\"";
		}
		return name;
	}

	public static void main(String[] args) throws Exception {
		System.out.println(getMD5HashValue("admin"));
		System.out.println(removeQuotes("\"first name\""));
		System.out.println(removeQuotes("This is my \"first name\""));
	}
}
