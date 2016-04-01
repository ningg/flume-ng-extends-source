package com.github.ningg.flume.source;

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
	
	public static String getDateFormatFromFileName(String name) {
		Pattern p = Pattern.compile(".*([0-9]{4}-[0-9]{2}-[0-9]{2}).*");
		Matcher m = p.matcher(name);
		if (m.find()) {
			return m.group(1);
		}
		return null;
	}
	
}
