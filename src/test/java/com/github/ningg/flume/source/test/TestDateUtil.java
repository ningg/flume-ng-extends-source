package com.github.ningg.flume.source.test;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Test;
import java.util.Date;
import java.util.regex.Pattern;

import com.github.ningg.flume.source.DateUtil;

public class TestDateUtil {

	@Test
	public void testisContainsDateFormat() {
		String date ="access_ipfilter.log.2015-9-02.gz";
		System.out.println(DateUtil.isContainsDateFormat(date));
	}
	
	@Test
	public void testgetDateFromFile() {
		String date ="/opt/app/msky/access_ipfilter.log.2015-09-02-11-01.gz";
		String s = DateUtil.getDateFormatFromFileName(date);
		if (s != null) {
			System.out.println(s);
		}
		if("2016-04-12-01".compareTo("2016-04-12-02") < 0) {
			System.out.println("1");
		}
	}
	
	@Test
	public void testgetYestDay() {
		String dateformat ="yyyy-MM-dd";
		Date date = new Date();
		String s = DateUtil.getLastTimeUnit(date,dateformat);
		if (s != null) {
			System.out.println(s);
		}
		Pattern targetPattern =  Pattern.compile(".*server.log.*");
		System.out.println(targetPattern.matcher("server.log.2016-04-13").matches());
	}
	
	@Test
	public void testString() {
		
		Event e = EventBuilder.withBody("\r", Charset.defaultCharset());
		try {
			String s = new String(e.getBody(),"UTF-8");
			System.out.println(s);
			System.out.println(s.length());
			System.out.println(s.getBytes());
		} catch (UnsupportedEncodingException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	@Test
	public void testEncoding() {
		String s = "中文";
		try {
			byte [] iso = s.getBytes("GBK");

			byte [] iso3 = s.getBytes("ISO-8859-1");
			String sg = new String(iso, "ISO-8859-1");
			String sr = new String(sg.getBytes("ISO-8859-1"), "GBK");
			byte [] iso2 = s.getBytes("GBK");
			System.out.println(sg);
			System.out.println(sr);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

	}

	@Test
	public void testEncoding2() {
		String s = "中文";
		try {
			byte [] iso = s.getBytes("UTF-8");
			byte [] iso1 = s.getBytes("GBK");
			String s1 = new String(iso, "UTF-8");
			String s2 = new String(iso1, "UTF-8");
			System.out.println(s1);
			System.out.println(s2);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

	}

	@Test
	public void testConverDateString() {
		String s = DateUtil.convertDatetoString2(new Date(), "yyyy-MM-dd-HH");
		if (s != null) {
			System.out.println(s);
		}
		if("2016-10-11-01".compareTo(s) < 0) {
			System.out.println("1");
		}
	}
}
