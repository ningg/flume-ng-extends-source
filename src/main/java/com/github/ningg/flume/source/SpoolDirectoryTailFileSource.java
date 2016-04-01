package com.github.ningg.flume.source;

import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.BASENAME_HEADER;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.BASENAME_HEADER_KEY;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.BATCH_SIZE;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.CONSUME_ORDER;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DECODE_ERROR_POLICY;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_BASENAME_HEADER;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_BASENAME_HEADER_KEY;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_BATCH_SIZE;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_CONSUME_ORDER;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_DECODE_ERROR_POLICY;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_DELETE_POLICY;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_DESERIALIZER;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_FILENAME_HEADER;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_FILENAME_HEADER_KEY;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_IGNORE_PAT;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_INPUT_CHARSET;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_MAX_BACKOFF;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_SPOOLED_FILE_SUFFIX;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_TARGET_FILENAME;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_TARGET_PAT;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DEFAULT_TRACKER_DIR;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DELETE_POLICY;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.DESERIALIZER;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.FILENAME_HEADER;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.FILENAME_HEADER_KEY;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.IGNORE_PAT;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.INPUT_CHARSET;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.MAX_BACKOFF;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.SPOOLED_FILE_SUFFIX;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.SPOOL_DIRECTORY;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.TARGET_FILENAME;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.TARGET_PAT;
import static com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.TRACKER_DIR;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.SpoolDirectorySource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ningg.flume.source.SpoolDirectoryTailFileSourceConfigurationConstants.ConsumeOrder;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;


/**
 * Function: spooling directory, collect all the historical files and tail the target file;
 * 
 * refer to {@link SpoolDirectorySource}
 * 
 * @author Ning Guo
 * @Email guoning.gn@gmail.com
 * @time 2015-02-25
 * 
 */

public class SpoolDirectoryTailFileSource extends AbstractSource implements Configurable, EventDrivenSource{

	private static final Logger logger = LoggerFactory.getLogger(SpoolDirectoryTailFileSource.class);
	
	// Delay used when polling for new files
	private static final int POLL_DELAY_MS = 500;
	
	/* Config options */
	private String completedSuffix;
	private String spoolDirectory;
	private boolean fileHeader;
	private String fileHeaderKey;
	private boolean basenameHeader;
	private String basenameHeaderKey;
	private int batchSize;
	private String ignorePattern;
	private String targetPattern;
	private String targetFilename;
	private String trackerDirPath;
	private String deserializerType;
	private Context deserializerContext;
	private String deletePolicy;
	private String inputCharset;
	private DecodeErrorPolicy decodeErrorPolicy;
	private volatile boolean hasFatalError = false;
	
	private SourceCounter sourceCounter;
	ReliableSpoolDirectoryTailFileEventReader reader;
	private ScheduledExecutorService executor;
	private boolean backoff = true;
	private boolean hitChannelException = false;
	private int maxBackoff;
	private ConsumeOrder consumeOrder;
	private String completeFileName;
	
	@Override
	public synchronized void start(){
		logger.info("SpoolDirectoryTailFileSource source starting with directory: {}, targetFilename: {}", spoolDirectory, targetFilename);
		
		executor = Executors.newSingleThreadScheduledExecutor();
		File directory = new File(spoolDirectory);
		
		try {
			reader = (new ReliableSpoolDirectoryTailFileEventReader.Builder())
							.spoolDirectory(directory)
							.completedSuffix(completedSuffix)
							.ignorePattern(ignorePattern)
							.targetPattern(targetPattern)
							.targetFilename(targetFilename)
							.trackerDirPath(trackerDirPath)
							.annotateFileName(fileHeader)
							.fileNameHeader(fileHeaderKey)
							.annotateBaseName(basenameHeader)
							.baseNameHeader(basenameHeaderKey)
							.deserializerType(deserializerType)
							.deserializerContext(deserializerContext)
							.deletePolicy(deletePolicy)
							.inputCharset(inputCharset)
							.decodeErrorPolicy(decodeErrorPolicy)
							.consumeOrder(consumeOrder)
							.completeFlagFileName(completeFileName)
							.build();
		} catch (IOException e) {
			throw new FlumeException("Error instantiating spooling and tail event parser", e);
		}
		
		Runnable runner = new SpoolDirectoryTailFileRunnable(reader, sourceCounter);
		executor.scheduleWithFixedDelay(runner, 0, POLL_DELAY_MS, TimeUnit.MILLISECONDS);
		
		super.start();
		logger.debug("SpoolDirectoryTailFileSource source started");
		sourceCounter.start();
		
	}
	
	@Override
	public synchronized void stop() {
		executor.shutdown();
		try {
			executor.awaitTermination(10L, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			logger.info("Interrupted while awaiting termination", e);
		}
		executor.shutdownNow();
		
		super.stop();
		sourceCounter.stop();
		logger.info("SpoolDirTailFile source {} stopped. Metrics: {}", getName(), sourceCounter);
	}
	
	
	@Override
	public String toString() {
		return "Spool Directory Tail File source " + getName() + ": { spoolDir: " + spoolDirectory + ", targetFilename: " + targetFilename + " }";
	}
	
	@Override
	public synchronized void configure(Context context) {
		spoolDirectory = context.getString(SPOOL_DIRECTORY);
		Preconditions.checkState(spoolDirectory != null, "Configuration must specify a spooling directory");
		
		completedSuffix = context.getString(SPOOLED_FILE_SUFFIX, DEFAULT_SPOOLED_FILE_SUFFIX);
		deletePolicy = context.getString(DELETE_POLICY, DEFAULT_DELETE_POLICY);
		
		fileHeader = context.getBoolean(FILENAME_HEADER, DEFAULT_FILENAME_HEADER);
		fileHeaderKey = context.getString(FILENAME_HEADER_KEY, DEFAULT_FILENAME_HEADER_KEY);
		
		basenameHeader = context.getBoolean(BASENAME_HEADER, DEFAULT_BASENAME_HEADER);
		basenameHeaderKey = context.getString(BASENAME_HEADER_KEY, DEFAULT_BASENAME_HEADER_KEY);
		
		batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
		inputCharset = context.getString(INPUT_CHARSET, DEFAULT_INPUT_CHARSET);
		
		decodeErrorPolicy = DecodeErrorPolicy.valueOf(context.getString(DECODE_ERROR_POLICY, DEFAULT_DECODE_ERROR_POLICY).toUpperCase());
		ignorePattern = context.getString(IGNORE_PAT, DEFAULT_IGNORE_PAT);
		targetPattern = context.getString(TARGET_PAT, DEFAULT_TARGET_PAT);
		targetFilename = context.getString(TARGET_FILENAME, DEFAULT_TARGET_FILENAME);
		trackerDirPath = context.getString(TRACKER_DIR, DEFAULT_TRACKER_DIR);
		
		deserializerType = context.getString(DESERIALIZER, DEFAULT_DESERIALIZER);
		deserializerContext = new Context(context.getSubProperties(DESERIALIZER + "."));
		
		consumeOrder = ConsumeOrder.valueOf(context.getString(CONSUME_ORDER, DEFAULT_CONSUME_ORDER.toString()).toUpperCase());
		completeFileName = getName()+"_cflag.txt";
				
		maxBackoff = context.getInteger(MAX_BACKOFF, DEFAULT_MAX_BACKOFF);
		
		if(sourceCounter == null){
			sourceCounter = new SourceCounter(getName());
		}
		
	}

	private class SpoolDirectoryTailFileRunnable implements Runnable {
		private ReliableSpoolDirectoryTailFileEventReader reader;
		private SourceCounter sourceCounter;
		
		public SpoolDirectoryTailFileRunnable(ReliableSpoolDirectoryTailFileEventReader reader, SourceCounter sourceCounter) {
			this.reader = reader;
			this.sourceCounter = sourceCounter;
		}
		
		@Override
		public void run() {
		  int backoffInterval = 250;
	      try {
	        while (!Thread.interrupted()) {
	          List<Event> events = reader.readEvents(batchSize);
	          if (events.isEmpty()) {
	        	logger.info("got an empty message List");
	        	reader.commit();	// Avoid IllegalStateException while tailing file.
	            break;
	        	/*TimeUnit.SECONDS.sleep(1);
	        	System.out.println("here i sleep");
	        	continue;*/
	          }
	          sourceCounter.addToEventReceivedCount(events.size());
	          sourceCounter.incrementAppendBatchReceivedCount();

	          try {
	            getChannelProcessor().processEventBatch(events);
	            reader.commit();
	          } catch (ChannelException ex) {
	            logger.warn("The channel is full, and cannot write data now. The " +
	              "source will try again after " + String.valueOf(backoffInterval) +
	              " milliseconds");
	            hitChannelException = true;
	            if (backoff) {
	              TimeUnit.MILLISECONDS.sleep(backoffInterval);
	              backoffInterval = backoffInterval << 1;
	              backoffInterval = backoffInterval >= maxBackoff ? maxBackoff :
	                                backoffInterval;
	            }
	            continue;
	          }
	          backoffInterval = 250;
	          sourceCounter.addToEventAcceptedCount(events.size());
	          sourceCounter.incrementAppendBatchAcceptedCount();
	          logger.debug("source count is {}", sourceCounter);
	        }
	        
	        // logger.info("Spooling Directory Tail File Source runner has shutdown.");
	      } catch (Throwable t) {
	        logger.error("FATAL: " + SpoolDirectoryTailFileSource.this.toString() + ": " +
	            "Uncaught exception in SpoolDirectoryTailSourceSource thread. " +
	            "Restart or reconfigure Flume to continue processing.", t);
	        hasFatalError = true;
	        Throwables.propagate(t);
	      }
		}
	}
}
