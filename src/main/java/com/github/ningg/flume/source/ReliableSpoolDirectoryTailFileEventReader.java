package com.github.ningg.flume.source;

import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_BASENAME_HEADER;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_BASENAME_HEADER_KEY;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_CONSUME_ORDER;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_DECODE_ERROR_POLICY;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_DELETE_POLICY;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_DESERIALIZER;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_FILENAME_HEADER;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_FILENAME_HEADER_KEY;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_IGNORE_PAT;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_INPUT_CHARSET;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_TARGET_FILENAME;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_TARGET_PAT;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_TRACKER_DIR;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.SPOOLED_FILE_SUFFIX;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.client.avro.ReliableEventReader;
import org.apache.flume.client.avro.ReliableSpoolingFileEventReader;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.apache.flume.serialization.DurablePositionTracker;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.EventDeserializerFactory;
import org.apache.flume.serialization.PositionTracker;
import org.apache.flume.serialization.ResettableFileInputStream;
import org.apache.flume.serialization.ResettableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.ConsumeOrder;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;

/**
 * Refer to {@link ReliableSpoolingFileEventReader}
 * 
 * @author Ning Guo
 *
 */

public class ReliableSpoolDirectoryTailFileEventReader implements ReliableEventReader{

	
	private static final Logger logger = LoggerFactory.getLogger(ReliableSpoolDirectoryTailFileEventReader.class);
	static final String metaFileName = ".flumespooltailfile-main.meta";
	
	private final File spoolDirectory;
	private final String completedSuffix;
	private final String deserializerType;
	private final Context deserializerContext;
	private final Pattern ignorePattern;
	private final Pattern targetPattern;
	private final String targetFilename; //目标文件所带的日期的格式
	private final File metaFile;
	private final boolean annotateFileName;
	private final boolean annotateBaseName;
	private final String fileNameHeader;
	private final String baseNameHeader;
	private final String deletePolicy;
	private final Charset inputCharset;
	private final DecodeErrorPolicy decodeErrorPolicy;
	private final ConsumeOrder consumeOrder;
	
	private final String comFlagFileName;
	private String currentFileDateFlag; //当前在读的文件的日期标记，只在第一次打开此文件时记录
	private final String originFileEncoding; //原始文件的日志格式
	private final boolean needConvertAfterSource;//进去sink前是否需要转格式才能变成UTF-

	private Optional<FileInfo> currentFile = Optional.absent();
	/** Always contains the last file from which lines have been read. **/
	private Optional<FileInfo> lastFileRead = Optional.absent();
	private boolean committed = true;
	
	/**
	 * Create a ReliableSpoolingFileEventReader to watch the given directory.
	 */
	 private ReliableSpoolDirectoryTailFileEventReader(File spoolDirectory,
	      String completedSuffix, String ignorePattern, String targetPattern, String targetFilename, String trackerDirPath,
	      boolean annotateFileName, String fileNameHeader,
	      boolean annotateBaseName, String baseNameHeader,
	      String deserializerType, Context deserializerContext,
	      String deletePolicy, String inputCharset,
	      DecodeErrorPolicy decodeErrorPolicy, 
	      ConsumeOrder consumeOrder, String completedFileName, String originFileEncoding, boolean needConvertAfterSource) throws IOException {

	    // Sanity checks
	    Preconditions.checkNotNull(spoolDirectory);
//	    Preconditions.checkNotNull(completedSuffix);
	    Preconditions.checkNotNull(ignorePattern);
	    Preconditions.checkNotNull(targetPattern);
	    Preconditions.checkNotNull(targetFilename);
	    Preconditions.checkNotNull(trackerDirPath);
	    Preconditions.checkNotNull(deserializerType);
	    Preconditions.checkNotNull(deserializerContext);
	    Preconditions.checkNotNull(deletePolicy);
	    Preconditions.checkNotNull(inputCharset);

	    // validate delete policy
	    if (!deletePolicy.equalsIgnoreCase(DeletePolicy.NEVER.name()) &&
	        !deletePolicy.equalsIgnoreCase(DeletePolicy.IMMEDIATE.name())) {
	      throw new IllegalArgumentException("Delete policies other than " +
	          "NEVER and IMMEDIATE are not yet supported");
	    }

	    if (logger.isDebugEnabled()) {
	      logger.debug("Initializing {} with directory={}, metaDir={}, " +
	          "deserializer={}",
	          new Object[] { ReliableSpoolDirectoryTailFileEventReader.class.getSimpleName(),
	          spoolDirectory, trackerDirPath, deserializerType });
	    }

	    // Verify directory exists and is readable/writable
	    Preconditions.checkState(spoolDirectory.exists(),
	        "Directory does not exist: " + spoolDirectory.getAbsolutePath());
	    Preconditions.checkState(spoolDirectory.isDirectory(),
	        "Path is not a directory: " + spoolDirectory.getAbsolutePath());

	    // Do a canary test to make sure we have access to spooling directory
	    try {
	      File canary = File.createTempFile("flume-spooldir-perm-check-", ".canary",
	          spoolDirectory);
	      Files.write("testing flume file permissions\n", canary, Charsets.UTF_8);
	      List<String> lines = Files.readLines(canary, Charsets.UTF_8);
	      Preconditions.checkState(!lines.isEmpty(), "Empty canary file %s", canary);
	      if (!canary.delete()) {
	        throw new IOException("Unable to delete canary file " + canary);
	      }
	      logger.debug("Successfully created and deleted canary file: {}", canary);
	    } catch (IOException e) {
	      throw new FlumeException("Unable to read and modify files" +
	          " in the spooling directory: " + spoolDirectory, e);
	    }

	    this.spoolDirectory = spoolDirectory;
	    this.completedSuffix = completedSuffix;
	    this.deserializerType = deserializerType;
	    this.deserializerContext = deserializerContext;
	    this.annotateFileName = annotateFileName;
	    this.fileNameHeader = fileNameHeader;
	    this.annotateBaseName = annotateBaseName;
	    this.baseNameHeader = baseNameHeader;
	    this.ignorePattern = Pattern.compile(ignorePattern);
	    this.targetPattern = Pattern.compile(targetPattern);
	    this.targetFilename = targetFilename;
	    this.deletePolicy = deletePolicy;
	    this.inputCharset = Charset.forName(inputCharset);
	    this.decodeErrorPolicy = Preconditions.checkNotNull(decodeErrorPolicy);
	    this.consumeOrder = Preconditions.checkNotNull(consumeOrder);    
	    
	    File trackerDirectory = new File(trackerDirPath);

	    // if relative path, treat as relative to spool directory
	    if (!trackerDirectory.isAbsolute()) {
	      trackerDirectory = new File(spoolDirectory, trackerDirPath);
	    }

	    // ensure that meta directory exists
	    if (!trackerDirectory.exists()) {
	      if (!trackerDirectory.mkdir()) {
	        throw new IOException("Unable to mkdir nonexistent meta directory " +
	            trackerDirectory);
	      }
	    }

	    // ensure that the meta directory is a directory
	    if (!trackerDirectory.isDirectory()) {
	      throw new IOException("Specified meta directory is not a directory" +
	          trackerDirectory);
	    }

	    this.metaFile = new File(trackerDirectory, metaFileName);
	    //生成completeFlagFile的绝对路径
	    this.comFlagFileName = trackerDirectory.getAbsolutePath()+ "/"+ completedFileName;
		this.originFileEncoding = originFileEncoding;
		this.needConvertAfterSource = needConvertAfterSource;
	    logger.info("complete flag file path is {}", comFlagFileName);
	  }
	
	  /** Return the filename which generated the data from the last successful
	   * {@link #readEvents(int)} call. Returns null if called before any file
	   * contents are read. */
	  public String getLastFileRead() {
	    if (!lastFileRead.isPresent()) {
	      return null;
	    }
	    return lastFileRead.get().getFile().getAbsolutePath();
	  }
	  
	
	@Override
	public Event readEvent() throws IOException {
		List<Event> events = readEvents(1);
	    if (!events.isEmpty()) {
	      return events.get(0);
	    } else {
	      return null;
	    }
	}

	@Override
	public List<Event> readEvents(int numEvents) throws IOException {
		if (!committed) {
	      if (!currentFile.isPresent()) {
	        throw new IllegalStateException("File should not roll when " +
	            "commit is outstanding.");
	      }
	      /**
	       * Function: Monitor current file.
	       * Author: Ning Guo
	       * Time: 2015-02-27
	       */
	      // logger.info("Last read was never committed - resetting mark position.");
	      currentFile.get().getDeserializer().reset();
	    } else {
	      // Check if new files have arrived since last call
	      if (!currentFile.isPresent()) {
	        currentFile = getNextFile();
	      }
	      // Return empty list if no new files
	     
	      if (!currentFile.isPresent()) {
	        return Collections.emptyList();
	      }
	    }
		logger.info("file is {}", currentFile.get().getFile().getName());
	    EventDeserializer des = currentFile.get().getDeserializer();
	    List<Event> events = des.readEvents(numEvents);
	    /* It's possible that the last read took us just up to a file boundary.
	     * If so, try to roll to the next file, if there is one. */
	    if (events.isEmpty()) {
	    	
	    	/*
	         * Function: monitor current file
	         * Author: Ning Guo
	         * Time: 2015-02-25
	         * 
	         * deal with two kinds files:
	         * 1. old File: delete or rename ;
	         * 2. current fresh File: monitor it; 
	         */
	       if(!isTargetFile(currentFile) 		//	Only CurrentFile is no longer the target, at the meanwhile, next file exists.
	    		   && (isExistNextFile()) ){	//	Then deal with the history file(ever target file)
	    	logger.info("File:{} is no longer a TARGET File, which will no longer be monitored.", currentFile.get().getFile().getName());
	     	retireCurrentFile();
	     	currentFile = getNextFile();
	       }
	    	
	      if (!currentFile.isPresent()) {
	        return Collections.emptyList();
	      }
	      events = currentFile.get().getDeserializer().readEvents(numEvents);
	    }
	    
	    logger.info("size is " + events.size());
	    if (events.size() > 0) {
	    	logger.debug("first message is "+new String(events.get(0).getBody(),originFileEncoding));
	    	String firmessage = new String(events.get(0).getBody(),originFileEncoding);
	    	String lastmessage = new String(events.get(events.size()-1).getBody(),originFileEncoding);
	    	if (lastmessage.equals(" ")) {
	    		logger.debug("remove last message");
	    		/*\r可能被作为line，构造了一个事件，要删除*/
	    		events.remove(events.size()-1);
	    		/*此处弥补因为\r被上次batch给取走了，导致本次第一条消息开头没空格*/
	    		if (!firmessage.startsWith(" ")) {
	    			events.get(0).setBody((" "+firmessage).getBytes());
	    		}
	    	}
			if (needConvertAfterSource) {
				//进入kafka sink之前，统一给搞成UTF-8
				for (Event e : events) {
					e.setBody(new String(e.getBody(),originFileEncoding).getBytes("UTF-8"));
				}
			}
			logger.debug("first message after convert is "+new String(events.get(0).getBody(),"UTF-8"));
	    	logger.debug("last message is " +new String(events.get(events.size()-1).getBody(),"UTF-8"));
	    } else {
	    	logger.debug("first and last message is null");
	    }
	    
	    if (annotateFileName) {
	      String filename = currentFile.get().getFile().getAbsolutePath();
			logger.debug("file header key is {}, value is {}" , fileNameHeader,filename);
	      for (Event event : events) {
	        event.getHeaders().put(fileNameHeader, filename);
	      }
	    }

	    if (annotateBaseName) {
	      String basename = currentFile.get().getFile().getName();
	      for (Event event : events) {
	        event.getHeaders().put(baseNameHeader, basename);
	      }
	    }

	    committed = false;
	    lastFileRead = currentFile;
	    return events;
	}

	/**追踪滚动到新文件：判断是否有下一个可读文件时，不能再使用{符合条件的文件大于等于2个}（当前在读的，新生成的）这个条件了
	 * 原因：假设这一种情况，10-08号的a.log读的比较慢，到9号时，归档脚本将a.log变成a.log.2016-10-08.gz。过了会8号a.log读完后，
	 * 在进行判断是否有下一个可读文件时，发现只有一个可读的a.log（9号的，当前在读的8号的变成gz了），因此一直不能追踪到新文件
	 * refer to {@link #getNextFile()}
	 * @return
	 */
	 private boolean isExistNextFile() {
		 /* Filter to exclude finished or hidden files */
		FileFilter filter = filterFilesTobeDone();
		List<File> candidateFiles = Arrays.asList(spoolDirectory.listFiles(filter));
		if (candidateFiles.isEmpty()){ 	// No matching file in spooling directory.
			return false;
		}
		//文件不带日期时，不能用new date()作为文件日志， 比如昨天是8号，中间如果一直没打日志的话，到了9号 8号的那个文件还是不带日期的
		 //那么会将8号的文件误判为new date()的文件
		 for (File file : candidateFiles) {
			 String dateOfCandidateFile = DateUtil.getDateFormatFromFileName(file.getName()) == null ?
					 DateUtil.convertDatetoString2(new Date(file.lastModified()), targetFilename) : DateUtil.getDateFormatFromFileName(file.getName());
			 if (dateOfCandidateFile.compareTo(this.currentFileDateFlag) > 0) {
				 logger.info("New file to be collected is:{} ", file.getName());
				 return true;
			 } else {
				 logger.info("File is not a new file:{} ", file.getName());
			 }
		 }
//		if (candidateFiles.size() >= 2 ) {	// Only when two file exist. (Since current file is there.)
//			return true;
//		}
		return false;
	}

	/**重构
	 * 过滤掉已完成和不合要求的，筛选出需要被收集日志的文件集合
	 * @return 返回一个FileFilter对象
	 */
	public FileFilter filterFilesTobeDone() {
		FileFilter filter = new FileFilter() {
			@Override
			public boolean accept(File candidate) {
				String fileName = candidate.getName();
				if( (candidate.isDirectory()) || 
					(fileName.startsWith(".")) ||
					(ignorePattern.matcher(fileName).matches()) ||
					(isCompletedFile(fileName, candidate))){
					return false;
				}
				if( targetPattern.matcher(fileName).matches() ){
					logger.debug("File:{} is a target pattern File", fileName);
					return true;
				}
				return false;
			}
		};
		return filter;
	}
	
	/**
	 * 根据completedFlagFile判断是否是已完成收集的文件
	 * @return true --finished false--need collected
	 */
	public boolean isCompletedFile(String fileName, File candidate) {
		String dateInFile = DateUtil.getDateFormatFromFileName(fileName);
		String completeFlag = CompleteFlagFileUtil.getCompleteFlagFromFile(this.comFlagFileName, this.targetFilename);
		/*文件名中不包含日期，按我们现在的格式，代表是当天的文件，当然是未完成的*/
		if (dateInFile != null) {

			/*文件名有日期，且比completefile中存的消息小的时候，表示已完成收集*/
			if (dateInFile.compareTo(completeFlag) <= 0) {
				logger.debug("File:{} is a completed File", fileName);
				return true;
			} 
		} else {
			String dateOfFileModify = DateUtil.convertDatetoString2(new Date(candidate.lastModified()), targetFilename);
			//文件里不含日期的情况下，最后修改时间小于完成时间的认为已完成， 但等于的应该是当前文件
			if (dateOfFileModify.compareTo(completeFlag) < 0) {
				logger.debug("File:{} is a completed File with modifytime {}", fileName, dateOfFileModify);
				return true;
			}
		}
		logger.debug("File:{} is a to be collected File only base on time", fileName);
		return false;
	}
	
	/** currentFile2 是a.log这种情况时，代表是当天的，也要返回true
	   * Test if currentFile2 is the targetFile.
	   * @param currentFile2
	   * @return
	   */
	  private boolean isTargetFile(Optional<FileInfo> currentFile2) {
		
		String inputFilename = currentFile2.get().getFile().getName();
		SimpleDateFormat dateFormat = new SimpleDateFormat(targetFilename);
		String substringOfTargetFile = dateFormat.format(new Date());
		logger.info("check file {} is target when got an empty message", inputFilename);
		if(inputFilename.toLowerCase().contains(substringOfTargetFile.toLowerCase())){
			return true;
		} else if (!DateUtil.isContainsDateFormat(inputFilename)) {
			
			logger.info("current read file {} date is {}", inputFilename, this.currentFileDateFlag);
			if (this.currentFileDateFlag.equals(substringOfTargetFile)) {
				logger.info("current read file {} do not contains date info, we think it is current file by check", inputFilename);
				return true;
			}
		}
		logger.info("file {} is no longer target file", inputFilename);
		return false;
	}
	
	/**
	   * Closes currentFile and attempt to rename it.
	   *
	   * If these operations fail in a way that may cause duplicate log entries,
	   * an error is logged but no exceptions are thrown. If these operations fail
	   * in a way that indicates potential misuse of the spooling directory, a
	   * FlumeException will be thrown.
	   * @throws FlumeException if files do not conform to spooling assumptions
	   */
	  private void retireCurrentFile() throws IOException {
	    Preconditions.checkState(currentFile.isPresent());

	    File fileToRoll = new File(currentFile.get().getFile().getAbsolutePath());

	    currentFile.get().getDeserializer().close();

	    // Verify that spooling assumptions hold
	    if (fileToRoll.lastModified() == currentFile.get().getLastModified()) {
	      logger.info("File:{} has not been modified since being read.", fileToRoll.getName());
	    }
	    if (fileToRoll.length() == currentFile.get().getLength()) {
	      logger.info("File:{} has not changed size since being read.", fileToRoll.getName());
	    }

	    if (deletePolicy.equalsIgnoreCase(DeletePolicy.NEVER.name())) {
	      rollCurrentFile(fileToRoll);
	    } else if (deletePolicy.equalsIgnoreCase(DeletePolicy.IMMEDIATE.name())) {
	      deleteCurrentFile(fileToRoll);
	    } else {
	      // TODO: implement delay in the future
	      throw new IllegalArgumentException("Unsupported delete policy: " +
	          deletePolicy);
	    }
	  }

	  /**已完成的文件，仅仅把meta文件删除，不再加后缀，在flume挪到下一个新文件时，再去更新complete flag，
	   * 考虑到今天和明天的文件重名，因此不在这里更新complete flag
	   * Rename the given spooled file
	   * @param fileToRoll
	   * @throws IOException
	   */
	  private void rollCurrentFile(File fileToRoll) throws IOException {
		String completeFlag = this.currentFileDateFlag;
		logger.info("prepare to update complete flag {} when roll this file",
				completeFlag);
		CompleteFlagFileUtil.updateFlagInFile(comFlagFileName, completeFlag);
	    logger.info("Preparing to delete the meta file ");
	    // now we no longer need the meta file
	    deleteMetaFile();
	  }

	  /**
	   * Delete the given spooled file
	   * @param fileToDelete
	   * @throws IOException
	   */
	  private void deleteCurrentFile(File fileToDelete) throws IOException {
	    logger.info("Preparing to delete file {}", fileToDelete);
	    if (!fileToDelete.exists()) {
	      logger.warn("Unable to delete nonexistent file: {}", fileToDelete);
	      return;
	    }
	    if (!fileToDelete.delete()) {
	      throw new IOException("Unable to delete spool file: " + fileToDelete);
	    }
	    // now we no longer need the meta file
	    deleteMetaFile();
	  }
	
	
	
	
	/**
	 * Returns the next file to be consumed from the chosen directory.
	 * If the directory is empty or the chosen file is not readable,
	 * this will return an absent option.
	 * If the {@link #consumeOrder} variable is {@link ConsumeOrder#OLDEST}
	 * then returns the oldest file. If the {@link #consumeOrder} variable
	 * is {@link ConsumeOrder#YOUNGEST} then returns the youngest file.
	 * If two or more files are equally old/young, then the file name with
	 * lower lexicographical value is returned.
	 * If THE {@link #consumeOrder} variable is {@link ConsumeOrder#RANDOM}
	 * then returns any arbitrary file in the directory.
	 */
	private Optional<FileInfo> getNextFile(){
		
		FileFilter filter = filterFilesTobeDone();
		List<File> candidateFiles = Arrays.asList(spoolDirectory.listFiles(filter));
		if (candidateFiles.isEmpty()){	// No matching file in spooling directory.
			return Optional.absent();
		}
		for (File file : candidateFiles) {
			logger.info("Files are {}", file.getName());
		}

		File selectedFile = candidateFiles.get(0);	// Select the first random file.
		if (consumeOrder == ConsumeOrder.RANDOM) {	// Selected file is random.
			return openFile(selectedFile);
		} else if (consumeOrder == ConsumeOrder.YOUNGEST) {
			for (File candidateFile: candidateFiles) {
				long compare = selectedFile.lastModified() - candidateFile.lastModified();
				if (compare == 0) {	// timestamp is same pick smallest lexicographically.
					selectedFile = smallerLexicographical(selectedFile, candidateFile);
				} else if (compare < 0) {	// candidate is younger (cand-ts > selec-ts)
					selectedFile = candidateFile;
				}
			}
			
		} else {	// deafult order is OLDEST
			for (File candidateFile: candidateFiles) {
				long compare = selectedFile.lastModified() - candidateFile.lastModified();
				if (compare == 0) {	// timstamp is same pick smallest lexicographically.
					selectedFile = smallerLexicographical(selectedFile, candidateFile);
				} else if (compare > 0) {	// candidate is older (cand-ts < selec-ts)
					selectedFile = candidateFile;
				}
			}
		}
		logger.info("next file is {}", selectedFile.getName());
		return openFile(selectedFile);
	}
	
	
	private File smallerLexicographical(File f1, File f2){
		if (f1.getName().compareTo(f2.getName()) < 0){
			return f1;
		}
		return f2;
	}
	
	
	/**
	   * Opens a file for consuming
	   * @param file
	   * @return {@link FileInfo} for the file to consume or absent option if the
	   * file does not exists or readable.
	   */
	  private Optional<FileInfo> openFile(File file) {    
	    try {
	      /*如果即将读的新文件不带日期话，说明已经读到当天的日志文件了，将昨天的日期更新到completeflagfile中去*/
	      String dateOfNewFile = DateUtil.getDateFormatFromFileName(file.getName()) ==null ?
	    		  DateUtil.convertDatetoString2(new Date(file.lastModified()),targetFilename): DateUtil.getDateFormatFromFileName(file.getName());
	      // roll the meta file, if needed
	      String nextPath = file.getPath();
	      nextPath = appendDateInfo(nextPath, dateOfNewFile);
	      PositionTracker tracker =
	          DurablePositionTracker.getInstance(metaFile, nextPath);
	      if (!tracker.getTarget().equals(nextPath)) {
	        tracker.close();
	        deleteMetaFile();
	        tracker = DurablePositionTracker.getInstance(metaFile, nextPath);
	      }

	      // sanity check
	      Preconditions.checkState(tracker.getTarget().equals(nextPath),
	          "Tracker target %s does not equal expected filename %s",
	          tracker.getTarget(), nextPath);

	      ResettableInputStream in =
	          new ResettableFileInputStream(file, tracker,
	              ResettableFileInputStream.DEFAULT_BUF_SIZE, inputCharset,
	              decodeErrorPolicy);
	      EventDeserializer deserializer = EventDeserializerFactory.getInstance
	          (deserializerType, deserializerContext, in);
	      /*每次打开一个新的文件时都要记录下*/
	      this.currentFileDateFlag = dateOfNewFile;
	      
	      return Optional.of(new FileInfo(file, deserializer));
	    } catch (FileNotFoundException e) {
	      // File could have been deleted in the interim
	      logger.warn("Could not find file: " + file, e);
	      return Optional.absent();
	    } catch (IOException e) {
	      logger.error("Exception opening file: " + file, e);
	      return Optional.absent();
	    }
	}
	
	 /**
	  *  meta要与log文件名字相关联，假若即将要读取的新文件不带日期的话，人为地给加上，这样tracker实例中log文件与meta文件（target）名字关联起来
	  * 这样强制加上日期，主要是防止隔天重启的情况下，tracker记录的昨天的position反而变成了今天的position
	  * e.g. 03-31号，只有是当天的文件a.log，meta的Tracker实例才会在target加上a.log.2016-03-31
	  *
	  * @param nextPath
	  * @return
	  */
	private String appendDateInfo(String nextPath, String dateOfNewFile) {
		if (!DateUtil.isContainsDateFormat(nextPath)) {
			logger.info("file {} need to append dateInfo when to create tracker for meta with date {}", nextPath, dateOfNewFile);
			nextPath = nextPath + "." + dateOfNewFile;
		}
		return nextPath;
	}

	private void deleteMetaFile() throws IOException {
	    if (metaFile.exists() && !metaFile.delete()) {
	      throw new IOException("Unable to delete old meta file " + metaFile);
	    }
	}
	
	  @Override
	  public void close() throws IOException {
	    if (currentFile.isPresent()) {
	      currentFile.get().getDeserializer().close();
	      currentFile = Optional.absent();
	    }
	  }

	  /** Commit the last lines which were read. */
	  @Override
	  public void commit() throws IOException {
	    if (!committed && currentFile.isPresent()) {
	      currentFile.get().getDeserializer().mark();
	      committed = true;
	    }
	  }
	
	/** An immutable class with information about a file being processed. **/
	private static class FileInfo {
		
		private final File file;
		private final long length;
		private final long lastModified;
		private final EventDeserializer deserializer;
		
		public FileInfo(File file, EventDeserializer deserializer){
			this.file = file;
			this.length = file.length();
			this.lastModified = file.lastModified();
			this.deserializer = deserializer;
		}

		public File getFile() { return file; }
		public long getLength() { return length; }
		public long getLastModified() { return lastModified; }
		public EventDeserializer getDeserializer() { return deserializer; }
		
	}

	static enum DeletePolicy{
		NEVER,
		IMMEDIATE,
		DELAY
	}

	
	/**
	 * Special builder class for {@link ReliableSpoolDirectoryTailFileEventReader}
	 */
	public static class Builder {
		
		private File spoolDirectory;
		private String completedSuffix = SPOOLED_FILE_SUFFIX;
		private String ignorePattern = DEFAULT_IGNORE_PAT;
		private String targetPattern = DEFAULT_TARGET_PAT;
		private String targetFilename = DEFAULT_TARGET_FILENAME;
		private String trackerDirPath = DEFAULT_TRACKER_DIR;
		private boolean annotateFileName = DEFAULT_FILENAME_HEADER;
		private String fileNameHeader = DEFAULT_FILENAME_HEADER_KEY;
		private boolean annotateBaseName = DEFAULT_BASENAME_HEADER;
		private String baseNameHeader = DEFAULT_BASENAME_HEADER_KEY;
		private String deserializerType = DEFAULT_DESERIALIZER;
		private Context deserializerContext = new Context();
		private String deletePolicy = DEFAULT_DELETE_POLICY;
		private String inputCharset = DEFAULT_INPUT_CHARSET;
		private DecodeErrorPolicy decodeErrorPolicy = DecodeErrorPolicy.valueOf(DEFAULT_DECODE_ERROR_POLICY.toUpperCase());
		private ConsumeOrder consumeOrder = DEFAULT_CONSUME_ORDER;
		private String completeFlagFileName;
		private String originFileEncoding;
		private boolean needCovertAfterSource;

		public Builder spoolDirectory(File directory) {
	      this.spoolDirectory = directory;
	      return this;
	    }

	    public Builder completedSuffix(String completedSuffix) {
	      this.completedSuffix = completedSuffix;
	      return this;
	    }
	    
	    public Builder ignorePattern(String ignorePattern) {
	    	this.ignorePattern = ignorePattern;
	    	return this;
	    }
	    
	    public Builder targetPattern(String targetPattern) {
	    	this.targetPattern = targetPattern;
	    	return this;
	    }
	    
	    public Builder targetFilename(String targetFilename) {
	    	this.targetFilename = targetFilename;
	    	return this;
	    }

	    public Builder trackerDirPath(String trackerDirPath) {
	      this.trackerDirPath = trackerDirPath;
	      return this;
	    }

	    public Builder annotateFileName(Boolean annotateFileName) {
	      this.annotateFileName = annotateFileName;
	      return this;
	    }

	    public Builder fileNameHeader(String fileNameHeader) {
	      this.fileNameHeader = fileNameHeader;
	      return this;
	    }

	    public Builder annotateBaseName(Boolean annotateBaseName) {
	      this.annotateBaseName = annotateBaseName;
	      return this;
	    }

	    public Builder baseNameHeader(String baseNameHeader) {
	      this.baseNameHeader = baseNameHeader;
	      return this;
	    }

	    public Builder deserializerType(String deserializerType) {
	      this.deserializerType = deserializerType;
	      return this;
	    }

	    public Builder deserializerContext(Context deserializerContext) {
	      this.deserializerContext = deserializerContext;
	      return this;
	    }

	    public Builder deletePolicy(String deletePolicy) {
	      this.deletePolicy = deletePolicy;
	      return this;
	    }

	    public Builder inputCharset(String inputCharset) {
	      this.inputCharset = inputCharset;
	      return this;
	    }

	    public Builder decodeErrorPolicy(DecodeErrorPolicy decodeErrorPolicy) {
	      this.decodeErrorPolicy = decodeErrorPolicy;
	      return this;
	    }
	    
	    public Builder completeFlagFileName(String completeFileName) {
	      this.completeFlagFileName = completeFileName;
	      return this;
	    } 
	    
		public Builder consumeOrder(ConsumeOrder consumeOrder) {
			this.consumeOrder = consumeOrder;
			return this;
		}

		public Builder originFileEncoding(String originFileEncoding) {
			this.originFileEncoding = originFileEncoding;
			return this;
		}

		public Builder needCovertAfterSource(String needCovertAfterSource) {
			if (StringUtils.equalsIgnoreCase(needCovertAfterSource, "true")) {
				this.needCovertAfterSource = true;
				return this;
			}
			this.needCovertAfterSource = false;
			return this;
		}
		
	    public ReliableSpoolDirectoryTailFileEventReader build() throws IOException {
	        return new ReliableSpoolDirectoryTailFileEventReader(spoolDirectory, completedSuffix,
	            ignorePattern, targetPattern, targetFilename, trackerDirPath, annotateFileName, fileNameHeader,
	            annotateBaseName, baseNameHeader, deserializerType,
	            deserializerContext, deletePolicy, inputCharset, decodeErrorPolicy,
	            consumeOrder, completeFlagFileName, originFileEncoding, needCovertAfterSource);
	      }

	}
	

}
