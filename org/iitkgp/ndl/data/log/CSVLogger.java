package org.iitkgp.ndl.data.log;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.data.CSVConfiguration;
import org.iitkgp.ndl.util.NDLDataUtils;

import com.opencsv.CSVWriter;

/**
 * Encapsulating CSV logging.
 * See <b>csv.log.write.line.threshold.limit</b> in <b>/conf/default.global.configuration.properties</b>
 * @author Debasis
 */
public class CSVLogger extends LogWriter<String[], CSVConfiguration> {
	
	CSVWriter logger = null;
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	protected long thresholdLimit() {
		return Long.parseLong(NDLConfigurationContext.getConfiguration("csv.log.write.line.threshold.limit"));
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void open() throws IOException {
		logger = NDLDataUtils.openCSV(currentFile(), configuration.getSeparator(), configuration.getQuote());
		if(header != null) {
			logger.writeNext(header);
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void write(String message[]) throws IOException {
		logger.writeNext(message);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {
		IOUtils.closeQuietly(logger);
	}

}