package com.github.ningg.flume.source.test;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Test;

import com.github.ningg.flume.source.DateUtil;

public class TestDateUtil {

	@Test
	public void testisContainsDateFormat() {
		String date ="access_ipfilter.log.2015-9-02.gz";
		System.out.println(DateUtil.isContainsDateFormat(date));
	}
	
	@Test
	public void testgetDateFromFile() {
		String date ="/opt/app/msky/access_ipfilter.log.2015-09-02.gz";
		String s = DateUtil.getDateFormatFromFileName(date);
		if (s != null) {
			System.out.println(s);
		}
	}
	
	@Test
	public void testgetYestDay() {
		String date ="2015-03-01";
		String s = DateUtil.getYesterDay(date);
		if (s != null) {
			System.out.println(s);
		}
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
}
