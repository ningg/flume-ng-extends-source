package com.github.ningg.flume.source;

import org.apache.flume.serialization.DecodeErrorPolicy;

public class SpoolDirectoryTailFileSourceConfigurationConstants {
	/** Directory where files are deposited. */
	public static final String SPOOL_DIRECTORY = "spoolDir";
	
	/** Suffix appended to files when they are finished being sent. **/
	public static final String SPOOLED_FILE_SUFFIX = "fileSuffix";
	public static final String DEFAULT_SPOOLED_FILE_SUFFIX = ".COMPLETED";
	
	/** Header in which to put absolute path filename. **/
	public static final String FILENAME_HEADER_KEY = "fileHeaderKey";
	public static final String DEFAULT_FILENAME_HEADER_KEY = "file";
	
	/** Whether to include absolute path filename in a header. **/
	public static final String FILENAME_HEADER = "fileHeader";
	public static final boolean DEFAULT_FILENAME_HEADER = false;
	
	/** Header in which to put the basename of file. **/
	public static final String BASENAME_HEADER_KEY = "basenameHeaderKey";
	public static final String DEFAULT_BASENAME_HEADER_KEY = "basename";
	
	/** Whether to include basename of a file in a header. **/
	public static final String BASENAME_HEADER = "basenameHeader";
	public static final boolean DEFAULT_BASENAME_HEADER = false;
	
	/** What size to batch with before sending to ChannelProcessor. **/
	public static final String BATCH_SIZE = "batchSize";
	public static final int DEFAULT_BATCH_SIZE = 100;
	
	/** Pattern of files to ignore. **/
	public static final String IGNORE_PAT = "ignorePattern";
	public static final String DEFAULT_IGNORE_PAT = "^$";	// no effect
	
	/** Pattern of files to focus. **/
	public static final String TARGET_PAT = "targetPattern";
	public static final String DEFAULT_TARGET_PAT = ".*(\\d){4}-(\\d){2}-(\\d){2}.*";
	
	/** Refer to Class: SimpleDateFormat  **/
	public static final String TARGET_FILENAME = "targetFilename";
	public static final String DEFAULT_TARGET_FILENAME = "yyyy-MM-dd";
	
	
	/** Directory to store metadata about files being processed. **/
	public static final String TRACKER_DIR = "trackerDir";
	public static final String DEFAULT_TRACKER_DIR = ".flumespooltail";
	
	/** Deserializer to use to parse the file data into Flume Events **/
	public static final String DESERIALIZER = "deserializer";
	public static final String DEFAULT_DESERIALIZER = "LINE";
	
	/**  **/
	public static final String DELETE_POLICY = "deletePolicy";
	public static final String DEFAULT_DELETE_POLICY = "never";
	
	/** Character set used when reading the input. **/
	public static final String INPUT_CHARSET = "inputCharset";
	public static final String DEFAULT_INPUT_CHARSET = "UTF-8";
	
	/** What to do when there is a character set decoding error. **/
	public static final String DECODE_ERROR_POLICY = "decodeErrorPolicy";
	public static final String DEFAULT_DECODE_ERROR_POLICY = DecodeErrorPolicy.FAIL.name();
	
	public static final String MAX_BACKOFF = "msxBackoff";
	public static final Integer DEFAULT_MAX_BACKOFF = 4000;
	
	
	/** Consume order **/
	public enum ConsumeOrder {
		OLDEST, YOUNGEST, RANDOM
	}
	
	public static final String CONSUME_ORDER = "consumeOrder"; 
	public static final ConsumeOrder DEFAULT_CONSUME_ORDER = ConsumeOrder.OLDEST;

	public static final String ORIGNFILE_ENCODING = "originFileCharset";
	public static final String DEFAULT_ORIGN_FILEENCODING = "UTF-8";

	public static final String NEEDCONVERTAFTERSOURCE = "needConvertAfterSource";
	public static final String DEFAULT_NEEDCONVERTAFTERSOURCE = "false";
	
	
	
	
	
}
