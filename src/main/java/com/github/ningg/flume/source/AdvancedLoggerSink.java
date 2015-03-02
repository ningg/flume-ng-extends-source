package com.github.ningg.flume.source;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventHelper;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.LoggerSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdvancedLoggerSink extends AbstractSink implements Configurable {

	private static final Logger logger = LoggerFactory
			.getLogger(LoggerSink.class);

	private static final int DEFAULT_MAX_BYTES = 16;
	private int maxBytes = DEFAULT_MAX_BYTES;
	
	@Override
	public void configure(Context context) {
		maxBytes = context.getInteger("maxBytes", DEFAULT_MAX_BYTES);
		logger.debug(this.getName() + " maximum bytes set to " + String.valueOf(maxBytes));
	}
	
	@Override
	public Status process() throws EventDeliveryException {
		Status result = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Event event = null;

		try {
			transaction.begin();
			event = channel.take();

			if (event != null) {
				if (logger.isInfoEnabled()) {
					// logger.info("Event: " + EventHelper.dumpEvent(event));
					logger.info("Event: " + EventHelper.dumpEvent(event, maxBytes));
				}
			} else {
				// No event found, request back-off semantics from the sink
				// runner
				result = Status.BACKOFF;
			}
			transaction.commit();
		} catch (Exception ex) {
			transaction.rollback();
			throw new EventDeliveryException("Failed to log event: " + event,
					ex);
		} finally {
			transaction.close();
		}

		return result;
	}

}
