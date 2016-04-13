package com.github.ningg.flume.source.test;

import org.junit.Test;

import com.github.ningg.flume.source.CompleteFlagFileUtil;

public class TestCompleteFlagUtil {

	@Test
	public void testGetFlagFromFile() {
		String path ="D:/flume_test.txt";
		System.out.println(CompleteFlagFileUtil.getCompleteFlagFromFile(path,"yyy-MM-dd"));
	}
	
	@Test
	public void testUpdateFlagInFile() {
		String path ="D:/flume_test.txt";
		String date = "2016-02-11";
		CompleteFlagFileUtil.updateFlagInFile(path, date);
	}
}
