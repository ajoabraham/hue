package com.hue.translator.sql.formatter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.vero.common.constant.DBType;

/*
 * net/balusc/util/DateUtil.java
 *
 * Copyright (C) 2007 BalusC
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if
 * not, write to the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */
public final class VeroDateTimeUtils {
	private static final DateTimeFormat DEFAULT_DATE_FORMAT =
		new DateTimeFormat("[yyyy][-][MM][-][dd]", DateTimeFormat.Type.DATE);
	private static final DateTimeFormat DEFAULT_DATETIME_FORMAT =
		new DateTimeFormat("[yyyy][-][MM][-][dd][ ][HH24][:][mi][:][ss][.][ff3]", DateTimeFormat.Type.DATETIME);

	public static class DateTimeFormat {
		public enum Type {
			DATE,
			DATETIME
		}

		private String _parseFormat;
		private Type _formatType;

		public DateTimeFormat(String parseFormat, Type formatType) { _parseFormat = parseFormat; _formatType = formatType; }
		public String getFormat() { return _parseFormat; }
		public String getJavaFormat() { return _parseFormat.replaceAll("\\[", "").replaceAll("\\]", ""); }
		public Type getType() { return _formatType; }
        @Override
        public String toString() {
            return "DateTimeFormat [_parseFormat=" + _parseFormat + ", _formatType=" + _formatType + "]";
        }
	}

	public static DateTimeFormat getDefaultDateTimeFormat(DateTimeFormat.Type dateType) {
		if (dateType == DateTimeFormat.Type.DATE) {
			return DEFAULT_DATE_FORMAT;
		} else {
			return DEFAULT_DATETIME_FORMAT;
		}
	}

    // Init ---------------------------------------------------------------------------------------
    private static final Map<String, DateTimeFormat> DATE_FORMAT_REGEXPS = new HashMap<String, DateTimeFormat>() {
        private static final long serialVersionUID = 1L;
    {
        put("^\\d{8}$", new DateTimeFormat("[yyyy][MM][dd]", DateTimeFormat.Type.DATE));
        put("^\\d{1,2}-\\d{1,2}-\\d{4}$", new DateTimeFormat("[dd][-][MM][-][yyyy]", DateTimeFormat.Type.DATE));
        put("^\\d{4}-\\d{1,2}-\\d{1,2}$", new DateTimeFormat("[yyyy][-][MM][-][dd]", DateTimeFormat.Type.DATE));
        put("^\\d{1,2}/\\d{1,2}/\\d{4}$", new DateTimeFormat("[MM][/][dd][/][yyyy]", DateTimeFormat.Type.DATE));
        put("^\\d{4}/\\d{1,2}/\\d{1,2}$", new DateTimeFormat("[yyyy][/][MM][/][dd]", DateTimeFormat.Type.DATE));
        put("^\\d{1,2}\\s[A-Za-z]{3}\\s\\d{4}$", new DateTimeFormat("[dd][ ][MON][ ][yyyy]", DateTimeFormat.Type.DATE));
        put("^\\d{1,2}\\s[A-Za-z]{4,}\\s\\d{4}$", new DateTimeFormat("[dd][ ][MONTH][ ][yyyy]", DateTimeFormat.Type.DATE));
        put("^\\d{12}$", new DateTimeFormat("[yyyy][MM][dd][HH24][mi]", DateTimeFormat.Type.DATETIME));
        put("^\\d{8}\\s\\d{4}$", new DateTimeFormat("[yyyy][MM][dd][ ][HH24][mi]", DateTimeFormat.Type.DATETIME));
        put("^\\d{1,2}-\\d{1,2}-\\d{4}\\s\\d{1,2}:\\d{2}$", new DateTimeFormat("[dd][-][MM][-][yyyy][ ][HH24][:][mi]", DateTimeFormat.Type.DATETIME));
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}$", new DateTimeFormat("[yyyy][-][MM][-][dd][ ][HH24][:][mi]", DateTimeFormat.Type.DATETIME));
        put("^\\d{1,2}/\\d{1,2}/\\d{4}\\s\\d{1,2}:\\d{2}$", new DateTimeFormat("[MM][/][dd][/][yyyy][ ][HH24][:][mi]", DateTimeFormat.Type.DATETIME));
        put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}$", new DateTimeFormat("[yyyy][/][MM][/][dd][ ][HH24][:][mi]", DateTimeFormat.Type.DATETIME));
        put("^\\d{1,2}\\s[A-Za-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{2}$", new DateTimeFormat("[dd][ ][MON][ ][yyyy][ ][HH24][:][mi]", DateTimeFormat.Type.DATETIME));
        put("^\\d{1,2}\\s[A-Za-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}$", new DateTimeFormat("[dd][ ][MONTH][ ][yyyy][ ][HH24][:][mi]", DateTimeFormat.Type.DATETIME));
        put("^\\d{14}$", new DateTimeFormat("[yyyy][MM][dd][HH24][mi][ss]", DateTimeFormat.Type.DATETIME));
        put("^\\d{8}\\s\\d{6}$", new DateTimeFormat("[yyyy][MM][dd][ ][HH24][mi][ss]", DateTimeFormat.Type.DATETIME));
        put("^\\d{1,2}-\\d{1,2}-\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", new DateTimeFormat("[dd][-][MM][-][yyyy][ ][HH24][:][mi][:][ss]", DateTimeFormat.Type.DATETIME));
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}$", new DateTimeFormat("[yyyy][-][MM][-][dd][ ][HH24][:][mi][:][ss]", DateTimeFormat.Type.DATETIME));
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\..*$", new DateTimeFormat("[yyyy][-][MM][-][dd][ ][HH24][:][mi][:][ss][.][ff3]", DateTimeFormat.Type.DATETIME));
        put("^\\d{1,2}/\\d{1,2}/\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", new DateTimeFormat("[MM][/][dd][/][yyyy][ ][HH24][:][mi][:][ss]", DateTimeFormat.Type.DATETIME));
        put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}$", new DateTimeFormat("[yyyy][/][MM][/][dd][ ][HH24][:][mi][:][ss]", DateTimeFormat.Type.DATETIME));
        put("^\\d{1,2}\\s[A-Za-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", new DateTimeFormat("[dd][ ][MON][ ][yyyy][ ][HH24][:][mi][:][ss]", DateTimeFormat.Type.DATETIME));
        put("^\\d{1,2}\\s[A-Za-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", new DateTimeFormat("[dd][ ][MONTH][ ][yyyy][ ][HH24][:][mi][:][ss]", DateTimeFormat.Type.DATETIME));
    }};

    private VeroDateTimeUtils() {
        // Utility class, hide the constructor.
    }

    // Converters ---------------------------------------------------------------------------------

    /**
     * Convert the given date to a Calendar object. The TimeZone will be derived from the local
     * operating system's timezone.
     * @param date The date to be converted to Calendar.
     * @return The Calendar object set to the given date and using the local timezone.
     */
    public static Calendar toCalendar(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.setTime(date);
        return calendar;
    }

    /**
     * Convert the given date to a Calendar object with the given timezone.
     * @param date The date to be converted to Calendar.
     * @param timeZone The timezone to be set in the Calendar.
     * @return The Calendar object set to the given date and timezone.
     */
    public static Calendar toCalendar(Date date, TimeZone timeZone) {
        Calendar calendar = toCalendar(date);
        calendar.setTimeZone(timeZone);
        return calendar;
    }

    /**
     * Parse the given date string to date object and return a date instance based on the given
     * date string. This makes use of the {@link DateUtil#determineDateFormat(String)} to determine
     * the SimpleDateFormat pattern to be used for parsing.
     * @param dateString The date string to be parsed to date object.
     * @return The parsed date object.
     * @throws ParseException If the date format pattern of the given date string is unknown, or if
     * the given date string or its actual date is invalid based on the date format pattern.
     */
    public static Date parse(String dateString) throws ParseException {
        DateTimeFormat dateFormat = determineDateFormat(dateString, null);
        if (dateFormat == null) {
            throw new ParseException("Unknown date format.", 0);
        }
        return parse(dateString, dateFormat.getJavaFormat());
    }

    /**
     * Validate the actual date of the given date string based on the given date format pattern and
     * return a date instance based on the given date string.
     * @param dateString The date string.
     * @param dateFormat The date format pattern which should respect the SimpleDateFormat rules.
     * @return The parsed date object.
     * @throws ParseException If the given date string or its actual date is invalid based on the
     * given date format pattern.
     * @see SimpleDateFormat
     */
    public static Date parse(String dateString, String dateFormat) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
        simpleDateFormat.setLenient(false); // Don't automatically convert invalid date.
        return simpleDateFormat.parse(dateString);
    }

    // Validators ---------------------------------------------------------------------------------

    /**
     * Checks whether the actual date of the given date string is valid. This makes use of the
     * {@link DateUtil#determineDateFormat(String)} to determine the SimpleDateFormat pattern to be
     * used for parsing.
     * @param dateString The date string.
     * @return True if the actual date of the given date string is valid.
     */
    public static boolean isValidDate(String dateString) {
        try {
            parse(dateString);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    /**
     * Checks whether the actual date of the given date string is valid based on the given date
     * format pattern.
     * @param dateString The date string.
     * @param dateFormat The date format pattern which should respect the SimpleDateFormat rules.
     * @return True if the actual date of the given date string is valid based on the given date
     * format pattern.
     * @see SimpleDateFormat
     */
    public static boolean isValidDate(String dateString, String dateFormat) {
        try {
            parse(dateString, dateFormat);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    // Checkers -----------------------------------------------------------------------------------

    /**
     * Determine SimpleDateFormat pattern matching with the given date string. Returns null if
     * format is unknown. You can simply extend DateUtil with more formats if needed.
     * @param dateString The date string to determine the SimpleDateFormat pattern for.
     * @return The matching SimpleDateFormat pattern, or null if format is unknown.
     * @see SimpleDateFormat
     */
    public static DateTimeFormat determineDateFormat(
		String dateString,
		DBType dbType) {
        if (dbType == null) { dbType = DBType.UNKNOWN; }

        if (dateString == null) return getDefaultDateTimeFormat(DateTimeFormat.Type.DATE);

        for (String regexp : DATE_FORMAT_REGEXPS.keySet()) {
            if (dateString.toLowerCase().matches(regexp)) {
                return DATE_FORMAT_REGEXPS.get(regexp);
            }
        }

        // unknown
        // determine datetype based on string length
        if (dateString.length() > 15) {
            return getDefaultDateTimeFormat(DateTimeFormat.Type.DATETIME);
        } else {
            return getDefaultDateTimeFormat(DateTimeFormat.Type.DATE);
        }
    }

    public static String convertDateTimeFormat(String veroFormat, DBType dbType) {
		Pattern pattern = Pattern.compile("\\[([^]]*)\\]");
		Matcher matcher = pattern.matcher(veroFormat);
		String convertedFormat = new String();

        while (matcher.find()) {
    		String token = matcher.group(1);
    		String conversion = "";
    
    		switch (token) {
    			case "YYYY":
    			case "yyyy":
    				switch (dbType) {
    					case MYSQL:
    						conversion = "%Y";
    						break;
    					default:
    						conversion = token;
    						break;
    				}
    				break;
    			case "MM":
    			case "mm":
    				switch (dbType) {
    					case MYSQL:
    						conversion = "%m";
    					    break;
    					default:
    						conversion = token;
    						break;
    				}
    				break;
    			case "MON":
    			case "mon":
    				switch (dbType) {
    					case MYSQL:
    						conversion = "%b";
    						break;
    					default:
    						conversion = token;
    						break;
    				}
    				break;
    			case "MONTH":
    			case "month":
    				switch (dbType) {
    					case MYSQL:
    						conversion = "%M";
    						break;
    					default:
    						conversion = token;
    						break;
    				}
    				break;
    			case "DD":
    			case "dd":
    				switch (dbType) {
    					case MYSQL:
    						conversion = "%d";
    						break;
    					default:
    						conversion = token;
    						break;
    				}
    				break;
    			case "HH":
    			case "hh":
    			case "HH12":
    			case "hh12":
    				// hour of day (01-12)
    				switch (dbType) {
    					case MYSQL:
    						conversion = "%h";
    						break;
    					case ACCESS:
                        case HIVE:
                        case DRILL:
                            conversion = "hh";
                            break;
    					default:
    						conversion = token;
    						break;
    				}
    				break;
    			case "HH24":
    			case "hh24":
    				// hour of day (00-23)
    				switch (dbType) {
    					case MYSQL:
    						conversion = "%H";
    						break;
    					case ACCESS:
                        case HIVE:
                        case DRILL:
                            conversion = "HH";
                            break;
    					default:
    						conversion = token;
    						break;
    				}
    				break;
    			case "MI":
    			case "mi":
    				// minute (00-59)
    				switch (dbType) {
    					case MYSQL:
    						conversion = "%i";
    						break;
    					case ACCESS:
                        case HIVE:
                        case DRILL:
                            conversion = "mm";
                            break;
    					default:
    						conversion = token;
    						break;
    				}
    				break;
    			case "SS":
    			case "ss":
    				// second (00-59)
    				switch (dbType) {
    					case MYSQL:
    						conversion = "%s";
    						break;
    					default:
    						conversion = token;
    						break;
    				}
    				break;
    			case "FF3":
    			case "ff3":
    				// Fractional seconds of 3 digits (ie millisecond)
    				switch (dbType) {
    					case MYSQL:
    						// ToDo: %f is microsecond, there is no millisecond for MySQL
    						conversion = "%f";
    						break;
    					case POSTGRESQL:
    					case NETEZZA:
    						conversion = "MS";
    						break;
    					case ACCESS:
                        case HIVE:
                        case DRILL:
                            conversion = "SSS";
                            break;
                        case VERTICA:
                            conversion = "MS";
                            break;
    					default:
    						conversion = token;
    						break;
    				}
    				break;
    			default:
    				conversion = token;
    				break;
    		}
    
    		convertedFormat = convertedFormat.concat(conversion);
        }

        return convertedFormat;
    }
}
