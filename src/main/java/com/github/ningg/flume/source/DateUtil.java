package com.github.ningg.flume.source;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class DateUtil {
	
	public static boolean isContainsDateFormat(String path) {
		Pattern p = Pattern.compile(".*[0-9]{4}-[0-9]{2}-[0-9]{2}.*");
		return p.matcher(path).matches();
	}
	
	public static String convertDatetoString(Date date) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		return format.format(date);
	}
	
	public static String convertDatetoString2(Date date, String dateFormat) {
		SimpleDateFormat format = new SimpleDateFormat(dateFormat);
		return format.format(date);
	}
	
	public static String getLastTimeUnit(Date date, String dateFormat) {
		SimpleDateFormat format = new SimpleDateFormat(dateFormat);
		long diff = 0L;
		if (dateFormat.equalsIgnoreCase("yyyy-MM-dd")) {
			diff = date.getTime() - 86400*1000;
		}else if(dateFormat.equalsIgnoreCase("yyyy-MM-dd-HH")){
			diff = date.getTime() - 3600*1000;
		}else if(dateFormat.equalsIgnoreCase("yyyy-MM-dd-HH-mm")){
			diff = date.getTime() - 60*1000;
		}

		return format.format(diff);
	}
	
	public static String getDateFormatFromFileName(String name) {
		Pattern p = Pattern.compile(".*([0-9]{4}-[0-9]{2}-[0-9]{2}[\\-0-9]*).*");
		Matcher m = p.matcher(name);
		if (m.find()) {
			return m.group(1);
		}
		return null;
	}
	
}
