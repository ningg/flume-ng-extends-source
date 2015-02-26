package com.github.ningg.flume.source;

import java.util.concurrent.ScheduledExecutorService;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.client.avro.ReliableSpoolingFileEventReader;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.SpoolDirectorySource;

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
	ReliableSpoolingFileEventReader reader;
	private ScheduledExecutorService executors;
	private boolean backoff = true;
	private boolean hitChannelException = false;
	private int maxBackoff;
	private ConsumerOrder consumerOrder;
	
	
	
	public void configure(Context context) {
		spoolDirectory = context.getString(key);
		
		
	}

}
