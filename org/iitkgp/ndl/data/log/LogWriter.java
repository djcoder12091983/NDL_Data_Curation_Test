package org.iitkgp.ndl.data.log;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * <pre>Logging writer encapsulation logic. It also encapsulates logger file rolling logic.</pre>
 * See {@link #thresholdLimit()}, this method defines after how many rows rolling takes place
 * @param <T> header/data part of logger file
 * @param <C> configuration part of logger file
 * @author Debasis 
 */
public abstract class LogWriter<T, C> {
	
	File parent;
	String fileName;
	String ext;
	long rowCount = 0;
	int fileIndex = 0;
	T header;
	C configuration;
	// row counter limit
	long thresholdLimit;
	
	static {
		// context initialization
		NDLConfigurationContext.init();
	}
	
	/**
	 * Initializes logging system.
	 * @param file logical file name to log messages
	 * @param header header of logging file if any
	 * @param configuration logging configuration if any
	 * @throws IOException throws exception if initialization fails
	 */
	public final void init(File file, T header, C configuration) throws IOException {
		this.header = header;
		this.configuration = configuration;
		parent = file.getParentFile();
		String name = file.getName();
		String details[] = NDLDataUtils.getFileNameAndExtension(name);
		fileName = details[0];
		ext = details[1];
		thresholdLimit = thresholdLimit(); // write limit
		open(); // open file for logging
	}
	
	/**
	 * Tells how many rows limit before rolling takes place in logging. Custom logger should define this method. 
	 * @return returns limit
	 */
	protected abstract long thresholdLimit();

	/**
	 * Logging file opening method
	 * @throws IOException throws exception if opening error occurs
	 */
	protected abstract void open() throws IOException;
	
	/**
	 * gets current file reference to log, this is required when logging splitted into multiple files (log roller).
	 * @return returns current file object
	 */
	protected File currentFile() {
		return new File(parent, fileName + "_" + ++fileIndex + (StringUtils.isNotBlank(ext) ? ("." + ext) : ""));
	}

	/**
	 * Logs message, internally log rolling takes place according to {@link #thresholdLimit()}
	 * @param data data to  log
	 * @throws IOException throws error if writing error occurs
	 */
	public final void log(T data) throws IOException {
		if(++rowCount > thresholdLimit) {
			// reset
			reset();
		}
		write(data);
	}
	
	/**
	 * writes log message
	 * @param data data to write
	 * @throws IOException throws error if writing error occurs
	 */
	protected abstract void write(T data) throws IOException;
	
	/**
	 * closes logger file
	 * @throws IOException throws error if closing fails
	 */
	public abstract void close() throws IOException;
	
	/**
	 * resets current logging system which in turn closes current logger file and re-open new logger file (log roller)
	 * @throws IOException throws error if opening/closing fails of logger file
	 */
	public void reset() throws IOException {
		close(); // close current file
		rowCount = 0; // reset
		open(); // re-open file with some numeric suffix
	}
}
