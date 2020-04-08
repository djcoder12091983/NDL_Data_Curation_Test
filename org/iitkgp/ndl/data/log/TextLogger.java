package org.iitkgp.ndl.data.log;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.context.NDLConfigurationContext;

/**
 * Encapsulating text logging.
 * See <b>text.log.write.line.threshold.limit</b> in <b>/conf/default.global.configuration.properties</b> 
 * @author Debasis
 */
public class TextLogger extends LogWriter<String, TextLoggingConfiguration> {
	
	BufferedWriter logger = null;

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void open() throws IOException {
		logger = new BufferedWriter(new FileWriter(currentFile())); // open file with some numeric suffix
		if(StringUtils.isNotBlank(header)) {
			log(header);
		}
		// TODO text logger configuration (if required)
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	protected long thresholdLimit() {
		return Long.parseLong(NDLConfigurationContext.getConfiguration("text.log.write.line.threshold.limit"));
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void write(String message) throws IOException {
		logger.write(message);
		logger.newLine();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {
		IOUtils.closeQuietly(logger);	
	}
}