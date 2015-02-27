package com.github.ningg.flume.source;

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
import org.apache.flume.tools.PlatformDetect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.ConsumeOrder;

import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.*;

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
	private final String targetFilename;
	private final File metaFile;
	private final boolean annotateFileName;
	private final boolean annotateBaseName;
	private final String fileNameHeader;
	private final String baseNameHeader;
	private final String deletePolicy;
	private final Charset inputCharset;
	private final DecodeErrorPolicy decodeErrorPolicy;
	private final ConsumeOrder consumeOrder;
	
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
	      ConsumeOrder consumeOrder) throws IOException {

	    // Sanity checks
	    Preconditions.checkNotNull(spoolDirectory);
	    Preconditions.checkNotNull(completedSuffix);
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
	       if(!isTargetFile(currentFile)){
	    	logger.info("File:{} is no longer a TARGET File, which will no longer be monitored.", currentFile.get().getFile().getName());
	     	retireCurrentFile();
	     	currentFile = getNextFile();
	       }
	    	
	      if (!currentFile.isPresent()) {
	        return Collections.emptyList();
	      }
	      events = currentFile.get().getDeserializer().readEvents(numEvents);
	    }

	    if (annotateFileName) {
	      String filename = currentFile.get().getFile().getAbsolutePath();
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

	 /**
	   * Test if currentFile2 is the targetFile.
	   * @param currentFile2
	   * @return
	   */
	  
	  private boolean isTargetFile(Optional<FileInfo> currentFile2) {
		
		String inputFilename = currentFile2.get().getFile().getName();
		SimpleDateFormat dateFormat = new SimpleDateFormat(targetFilename);
		String substringOfTargetFile = dateFormat.format(new Date());
		
		if(inputFilename.toLowerCase().contains(substringOfTargetFile.toLowerCase())){
			return true;
		}
		
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

	  /**
	   * Rename the given spooled file
	   * @param fileToRoll
	   * @throws IOException
	   */
	  private void rollCurrentFile(File fileToRoll) throws IOException {

	    File dest = new File(fileToRoll.getPath() + completedSuffix);
	    logger.info("Preparing to move file {} to {}", fileToRoll, dest);

	    // Before renaming, check whether destination file name exists
	    if (dest.exists() && PlatformDetect.isWindows()) {
	      /*
	       * If we are here, it means the completed file already exists. In almost
	       * every case this means the user is violating an assumption of Flume
	       * (that log files are placed in the spooling directory with unique
	       * names). However, there is a corner case on Windows systems where the
	       * file was already rolled but the rename was not atomic. If that seems
	       * likely, we let it pass with only a warning.
	       */
	      if (Files.equal(currentFile.get().getFile(), dest)) {
	        logger.warn("Completed file " + dest +
	            " already exists, but files match, so continuing.");
	        boolean deleted = fileToRoll.delete();
	        if (!deleted) {
	          logger.error("Unable to delete file " + fileToRoll.getAbsolutePath() +
	              ". It will likely be ingested another time.");
	        }
	      } else {
	        String message = "File name has been re-used with different" +
	            " files. Spooling assumptions violated for " + dest;
	        throw new IllegalStateException(message);
	      }

	    // Dest file exists and not on windows
	    } else if (dest.exists()) {
	      String message = "File name has been re-used with different" +
	          " files. Spooling assumptions violated for " + dest;
	      throw new IllegalStateException(message);

	    // Destination file does not already exist. We are good to go!
	    } else {
	      boolean renamed = fileToRoll.renameTo(dest);
	      if (renamed) {
	        logger.debug("Successfully rolled file {} to {}", fileToRoll, dest);

	        // now we no longer need the meta file
	        deleteMetaFile();
	      } else {
	        /* If we are here then the file cannot be renamed for a reason other
	         * than that the destination file exists (actually, that remains
	         * possible w/ small probability due to TOC-TOU conditions).*/
	        String message = "Unable to move " + fileToRoll + " to " + dest +
	            ". This will likely cause duplicate events. Please verify that " +
	            "flume has sufficient permissions to perform these operations.";
	        throw new FlumeException(message);
	      }
	    }
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
		/* Filter to exclude finished or hidden files */
		FileFilter filter = new FileFilter() {
			@Override
			public boolean accept(File candidate) {
				String fileName = candidate.getName();
				if( (candidate.isDirectory()) || 
					(fileName.endsWith(completedSuffix)) ||
					(fileName.startsWith(".")) ||
					(ignorePattern.matcher(fileName).matches()) ){
					return false;
				}
				if( targetPattern.matcher(fileName).matches() ){
					return true; 
				}
				return false;
			}
		};
		List<File> candidateFiles = Arrays.asList(spoolDirectory.listFiles(filter));
		if (candidateFiles.isEmpty()){	// No matching file in spooling directory.
			return Optional.absent();
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
	   * @return {@link #FileInfo} for the file to consume or absent option if the
	   * file does not exists or readable.
	   */
	  private Optional<FileInfo> openFile(File file) {    
	    try {
	      // roll the meta file, if needed
	      String nextPath = file.getPath();
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
	    
	    public Builder consumeOrder(ConsumeOrder consumeOrder) {
	      this.consumeOrder = consumeOrder;
	      return this;
	    } 
		
	    public ReliableSpoolDirectoryTailFileEventReader build() throws IOException {
	        return new ReliableSpoolDirectoryTailFileEventReader(spoolDirectory, completedSuffix,
	            ignorePattern, targetPattern, targetFilename, trackerDirPath, annotateFileName, fileNameHeader,
	            annotateBaseName, baseNameHeader, deserializerType,
	            deserializerContext, deletePolicy, inputCharset, decodeErrorPolicy,
	            consumeOrder);
	      }
		
		
	}
	

}
