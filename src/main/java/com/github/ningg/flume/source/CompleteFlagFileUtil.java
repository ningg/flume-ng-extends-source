package com.github.ningg.flume.source;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 本地维护一个source唯一对应的completeflag文件，记录flume最新读到的文件的日期，只有在此日期和之后的文件会进行收集
 * @author qs
 *
 */
public class CompleteFlagFileUtil {
	
	private static final Logger logger = LoggerFactory.getLogger(ReliableSpoolDirectoryTailFileEventReader.class);
	
	public static String getCompleteFlagFromFile(String filePath) {
		
		File cfFile = new File(filePath);
		if (!cfFile.exists()) {
			try {  
				cfFile.createNewFile();
				logger.info("Successfully create file {}", cfFile.getName());
				
				Date date = new Date();
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				String dateFormat = sdf.format(date);
				writeToFile(cfFile, dateFormat);
            } catch (IOException e) {  
            	logger.error("Unable to create new completeflag file " +
            			 cfFile.getName(), e); 
            }  
		}
		BufferedReader in = null;
		try {
			in = new BufferedReader(
					new InputStreamReader(new BufferedInputStream(
							new FileInputStream(cfFile)), "utf-8"));
			String s;
			String flag = "";
			while ((s = in.readLine()) != null) {
				s = s.trim();
				flag = s;
			}
			logger.info("complete flag in file is {}", flag);
			return flag;
		} catch (Exception e) {
			logger.error("Unable to read completeflag file " +cfFile.getName()+
     	            e, e); 
		}
		return null;
	}


	/**
	 * @param cfFile
	 * @param dateFormat
	 * @throws FileNotFoundException
	 */
	private static void writeToFile(File cfFile, String dateFormat)
			throws FileNotFoundException {
		FileWriter fw;
		try {
			fw = new FileWriter(cfFile, true);
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(dateFormat);
			bw.write("\r\n");
			bw.close();
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
	}
	
	
	public static void updateFlagInFile (String filePath, String date) {
		File cfFile = new File(filePath);
		
		try {
			writeToFile(cfFile, date);
			logger.info("update {} to file {}", date, cfFile.getName());
		} catch (FileNotFoundException e) {
			logger.error("Unable to update completeflag file " +cfFile.getName()+
     	            e, e);
		}
	}
	
}
