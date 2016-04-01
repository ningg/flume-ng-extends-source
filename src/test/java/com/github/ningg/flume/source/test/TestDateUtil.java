package com.github.ningg.flume.source.test;

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
}
