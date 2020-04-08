package org.iitkgp.ndl.data.container;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.CSVConfiguration;
import org.iitkgp.ndl.data.exception.InvalidConfigurationKeyException;
import org.iitkgp.ndl.data.log.CSVLogger;
import org.iitkgp.ndl.data.log.TextLogger;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * <pre>Abstract data container to process data.</pre>
 * <pre>Additionally it offers logging facility and loading configuration details which may help while processing data,
 * see {@link ConfigurationPool} for more details</pre>
 * <b>Note: Default logging file name date pattern: yyyyMMMdd</b>
 * @param C Data container initialization configuration
 * @author Debasis
 */
public abstract class AbstractDataContainer<C> {
	
	protected String input;
	protected String logLocation;
	/**
	 * This is logical name of the data container, may be NULL
	 */
	String name; // logical name
	// loggers
	Map<String, TextLogger> textLoggers = new HashMap<String, TextLogger>(2); 
	Map<String, CSVLogger> csvLoggers = new HashMap<String, CSVLogger>(2);
	String fileNameDatePattern = "yyyy.MMM.dd.HH.mm.ss";
	SimpleDateFormat dateFormatter = new SimpleDateFormat(fileNameDatePattern);
	String fileNameDateOnlyPattern = "yyyy.MMM.dd";
	SimpleDateFormat dateOnlyFormatter = new SimpleDateFormat(fileNameDateOnlyPattern);
	String globalLogger = null;
	boolean globalLoggingFlag = true; // default global logging flag
	ConfigurationPool configurationMappingPool = new ConfigurationPool(); // configuration table
	protected C containerConfiguration = null;
	
	/**
	 * Constructor
	 * @param input source data (either compressed version or folder version)
	 * @param logLocation logging location
	 */
	public AbstractDataContainer(String input, String logLocation) {
		this.input = input;
		this.logLocation = logLocation;
		this.globalLogger = NDLDataUtils.GLOBAL_LOGGER_FILE_NAME;
	}
	
	/**
	 * Constructor
	 * @param input source data (either compressed version or folder version)
	 * @param logLocation logging location
	 * @param containerConfiguration container configuration
	 */
	public AbstractDataContainer(String input, String logLocation, C containerConfiguration) {
		this(input, logLocation);
		this.containerConfiguration = containerConfiguration;
	}
	
	/**
	 * Constructor
	 * @param input source data (either compressed version or folder version)
	 * @param logLocation logging location
	 * @param name logical name which differentiates log file(s), output file(s) from other source
	 */
	public AbstractDataContainer(String input, String logLocation, String name) {
		this(input, logLocation);
		this.name = name;
		this.globalLogger = NDLDataUtils.GLOBAL_LOGGER_FILE_NAME;
	}
	
	/**
	 * Constructor
	 * @param input source data (either compressed version or folder version)
	 * @param logLocation logging location
	 * @param name logical name which differentiates log file(s), output file(s) from other source
	 * @param containerConfiguration container configuration
	 */
	public AbstractDataContainer(String input, String logLocation, String name, C containerConfiguration) {
		this(input, logLocation, name);
		this.containerConfiguration = containerConfiguration;
	}
	
	/**
	 * Sets logger file name java compatible date pattern
	 * @param loggerFileNameDatePattern logger file name date pattern
	 * @see SimpleDateFormat
	 * @see #setFileNameDatePattern(String)
	 * @see #setFileNameDateOnlyPattern(String)
	 */
	@Deprecated
	public void setLoggerFileNameDatePattern(String loggerFileNameDatePattern) {
		this.fileNameDatePattern = loggerFileNameDatePattern;
		dateFormatter = new SimpleDateFormat(loggerFileNameDatePattern); // reset
	}
	
	/**
	 * Sets logger file name java compatible date pattern
	 * @param fileNameDatePattern logger file name date pattern
	 * @see SimpleDateFormat
	 */
	public void setFileNameDatePattern(String fileNameDatePattern) {
		this.fileNameDatePattern = fileNameDatePattern;
		dateFormatter = new SimpleDateFormat(fileNameDatePattern); // reset
	}
	
	/**
	 * Sets logger file name java compatible date pattern
	 * @param fileNameDateOnlyPattern logger file name date pattern
	 * @see SimpleDateFormat
	 * @see #setFileNameDatePattern(String)
	 */
	public void setFileNameDateOnlyPattern(String fileNameDateOnlyPattern) {
		this.fileNameDateOnlyPattern = fileNameDateOnlyPattern;
		dateOnlyFormatter = new SimpleDateFormat(fileNameDateOnlyPattern); // reset
	}
	
	/**
	 * Gets global logger reference
	 * @return returns global reference if found, otherwise NULL
	 */
	public TextLogger getGlobalLogger() {
		if(globalLoggingFlag) {
			// found
			return textLoggers.get(globalLogger);
		} else {
			// not found
			return null;
		}
	}
	
	/**
	 * By default global logging mode is ON, to off it call this API
	 */
	public void turnOffGlobalLoggingFlag() {
		globalLoggingFlag = false;
	}
	
	/**
	 * Initializes container, opening logging files etc.
	 * @param configuration initialization configuration
	 * @throws IOException throws error while opening logging files etc.
	 */
	public void init(C configuration) throws IOException {
		// global text logger
		if(globalLoggingFlag) {
			addTextLogger(globalLogger);
		}
	}
	
	/**
	 * Adds configuration mapping file, see {@link ConfigurationPool} and {@link ConfigurationData} for more details
	 * @param file mapping file
	 * @param logicalName logical name to identify mapping file
	 * @throws IOException throws error while loading configurations
	 * @see #addMappingResource(String, String)
	 */
	public void addMappingResource(File file, String logicalName) throws IOException {
		configurationMappingPool.addResource(file, logicalName);
	}
	
	/**
	 * Adds configuration mapping file, see {@link ConfigurationPool} and {@link ConfigurationData} for more details
	 * @param file mapping file
	 * @param logicalName logical name to identify mapping file
	 * @param ignoreCase this flag determines whether to ignore case or not for primary key
	 * @throws IOException throws error while loading configurations
	 * @see #addMappingResource(String, String)
	 */
	public void addMappingResource(File file, String logicalName, boolean ignoreCase) throws IOException {
		configurationMappingPool.addResource(file, logicalName, ignoreCase);
	}
	
	/**
	 * Adds configuration mapping file, see {@link ConfigurationPool} and {@link ConfigurationData} for more details
	 * @param file mapping file name
	 * @param logicalName logical name to identify mapping file
	 * @throws IOException throws error while loading configurations
	 */
	public void addMappingResource(String file, String logicalName) throws IOException {
		configurationMappingPool.addResource(new File(file), logicalName);
	}
	
	/**
	 * Adds configuration mapping file, see {@link ConfigurationPool} and {@link ConfigurationData} for more details
	 * @param file mapping file name
	 * @param logicalName logical name to identify mapping file
	 * @param ignoreCase this flag determines whether to ignore case or not for primary key
	 * @throws IOException throws error while loading configurations
	 */
	public void addMappingResource(String file, String logicalName, boolean ignoreCase) throws IOException {
		configurationMappingPool.addResource(new File(file), logicalName, ignoreCase);
	}
	
	/**
	 * Adds resource mapping file, see {@link ConfigurationPool} and {@link ConfigurationData} for more details
	 * @param file file mapping file
	 * @param primaryIndex primary index column by which data is found
	 * @param logicalName logical name to identify mapping file
	 * @throws IOException throws error while loading configurations
	 * @see #addMappingResource(String, String, String)
	 */
	public void addMappingResource(File file, String primaryIndex, String logicalName) throws IOException {
		configurationMappingPool.addResource(file, primaryIndex, logicalName);
	}
	
	/**
	 * Adds resource mapping file, see {@link ConfigurationPool} and {@link ConfigurationData} for more details
	 * @param file file mapping file
	 * @param primaryIndex primary index column by which data is found
	 * @param logicalName logical name to identify mapping file
	 * @param ignoreCase this flag determines whether to ignore case or not for primary key
	 * @throws IOException throws error while loading configurations
	 * @see #addMappingResource(String, String, String)
	 */
	public void addMappingResource(File file, String primaryIndex, String logicalName, boolean ignoreCase)
			throws IOException {
		configurationMappingPool.addResource(file, primaryIndex, logicalName, ignoreCase);
	}
	
	/**
	 * Adds resource mapping file, see {@link ConfigurationPool} and {@link ConfigurationData} for more details
	 * @param file file mapping file
	 * @param primaryIndex primary index column by which data is found
	 * @param logicalName logical name to identify mapping file
	 * @throws IOException throws error while loading configurations
	 */
	public void addMappingResource(String file, String primaryIndex, String logicalName) throws IOException {
		configurationMappingPool.addResource(new File(file), primaryIndex, logicalName);
	}
	
	/**
	 * Adds resource mapping file, see {@link ConfigurationPool} and {@link ConfigurationData} for more details
	 * @param file file mapping file
	 * @param primaryIndex primary index column by which data is found
	 * @param logicalName logical name to identify mapping file
	 * @param ignoreCase this flag determines whether to ignore case or not for primary key
	 * @throws IOException throws error while loading configurations
	 */
	public void addMappingResource(String file, String primaryIndex, String logicalName, boolean ignoreCase)
			throws IOException {
		configurationMappingPool.addResource(new File(file), primaryIndex, logicalName, ignoreCase);
	}
	
	/**
	 * Gets mapped value by given key, see {@link ConfigurationPool} and {@link ConfigurationData} for more details
	 * @param key given key
	 * @return returns mapped value, otherwise NULL if no mapping found 
	 * @throws InvalidConfigurationKeyException throws error when key is invalid
	 */
	public String getMappingKey(String key) throws InvalidConfigurationKeyException {
		return configurationMappingPool.get(key);
	}
	
	/**
	 * Checks whether given key exists or not, see {@link ConfigurationPool} and {@link ConfigurationData} for more details
	 * <pre>If any key contains dot then use {@link ConfigurationData#escapeDot(String)}</pre>
	 * @param key given key
	 * @return returns true if found otherwise false
	 * @throws InvalidConfigurationKeyException throws error when key is invalid
	 * @see ConfigurationData#escapeDot(String)
	 * @see #containsMappingKeyByTokens(String...)
	 */
	public boolean containsMappingKey(String key) throws InvalidConfigurationKeyException {
		return configurationMappingPool.contains(key);
	}
	
	/**
	 * <pre>Checks whether given key exists or not, see {@link ConfigurationPool} and {@link ConfigurationData} for more details</pre>
	 * If you want to find XXX[.YYY][.ZZZ] then you need to call API like containsMappingKey(XXX, YYY, ZZZ) etc.
	 * @param tokens these are tokens of the key/expression to find,
	 * here no need to use {@link ConfigurationData#escapeDot(String)}
	 * @return returns true if found otherwise false
	 * @throws InvalidConfigurationKeyException throws error when key is invalid
	 */
	public boolean containsMappingKeyByTokens(String ... tokens) throws InvalidConfigurationKeyException {
		StringBuilder key = new StringBuilder();
		int l = tokens.length;
		for(int i = 0; i < l; i++) {
			key.append(ConfigurationData.escapeDot(tokens[i]));
			if(i < l - 1) {
				key.append(".");
			}
		}
		return containsMappingKey(key.toString());
	}
	
	/**
	 * Adds CSV logging file to container, for logging purpose while processing the container data
	 * @param filename file name to log
	 * @param configuration CSV configuration detail like escape character, quote character etc.
	 * @return returns corresponding logger
	 * @throws IOException throws exception if logger file coudn't be opened
	 * @see #addCSVLogger(String, String[], CSVConfiguration)
	 */
	public CSVLogger addCSVLogger(String filename, CSVConfiguration configuration) throws IOException {
		return addCSVLogger(filename, null, configuration); // without header
	}
	
	/**
	 * Adds CSV logging file to container, for logging purpose while processing the container data
	 * @param filename file name to log
	 * @return returns corresponding logger
	 * @throws IOException throws exception if logger file coudn't be opened
	 * @see #addCSVLogger(String, CSVConfiguration)
	 * @see #addCSVLogger(String, String[])
	 * @see #addCSVLogger(String, String[], CSVConfiguration)
	 */
	public CSVLogger addCSVLogger(String filename) throws IOException {
		return addCSVLogger(filename, null, NDLDataUtils.getDefaultCSVConfiguration()); // without header
	}
	
	/**
	 * Adds CSV logging file to container, for logging purpose while processing the container data
	 * @param filename file name to log
	 * @param header header columns if exists
	 * @param configuration CSV configuration detail like escape character, quote character etc.
	 * @return returns corresponding logger
	 * @throws IOException throws exception if logger file coudn't be opened
	 */
	public CSVLogger addCSVLogger(String filename, String header[], CSVConfiguration configuration) throws IOException {
		String logicalName = null;
		if(!StringUtils.endsWithIgnoreCase(filename, ".csv")) {
			// extension missing
			logicalName = filename;
			filename = filename + ".csv";
		} else {
			// remove uppercase extension
			String details[] = NDLDataUtils.getFileNameAndExtension(filename);
			filename = details[0] + ".csv";
			logicalName = details[0];
		}
		if(csvLoggers.containsKey(logicalName)) {
			// already exists
			throw new IllegalStateException("File name: " + filename + " already exists.");
		}
		CSVLogger logger = new CSVLogger();
		logger.init(new File(NDLDataUtils.createFolder(logLocation), getFileName(filename)), header, configuration);
		csvLoggers.put(logicalName, logger);
		return logger;
	}
	
	/**
	 * Adds CSV logging file to container, for logging purpose while processing the container data
	 * @param filename file name to log
	 * @param header header columns if exists
	 * @return returns corresponding logger
	 * @throws IOException throws exception if logger file coudn't be opened
	 * @see #addCSVLogger(String, String[], CSVConfiguration)
	 */
	public CSVLogger addCSVLogger(String filename, String header[]) throws IOException {
		return addCSVLogger(filename, header, NDLDataUtils.getDefaultCSVConfiguration());
	}
	
	/**
	 * Adds text logging file to container, for logging purpose while processing the container data
	 * @param filename file name to log
	 * @return returns corresponding logger
	 * @throws IOException throws exception if logger file coudn't be opened
	 * @see #addTextLogger(String, String)
	 */
	public TextLogger addTextLogger(String filename) throws IOException {
		return addTextLogger(filename, null); // without header
	}
	
	/**
	 * Adds text logging file to container, for logging purpose while processing the container data
	 * @param filename file name to log
	 * @param header header text if required
	 * @return returns corresponding logger
	 * @throws IOException throws exception if logger file coudn't be opened
	 */
	public TextLogger addTextLogger(String filename, String header) throws IOException {
		String logicalName = null;
		if(!StringUtils.endsWithIgnoreCase(filename, ".txt")) {
			// extension missing
			logicalName = filename;
			filename = filename + ".txt";
		} else {
			// remove uppercase extension
			String details[] = NDLDataUtils.getFileNameAndExtension(filename);
			filename = details[0] + ".txt";
			logicalName = details[0];
		}
		if(textLoggers.containsKey(logicalName)) {
			// already exists
			throw new IllegalStateException("File name: " + filename + " already exists.");
		}
		TextLogger logger = new TextLogger();
		// no text logging configuration
		logger.init(new File(NDLDataUtils.createFolder(logLocation), getFileName(filename)), header, null);
		textLoggers.put(logicalName, logger);
		return logger;
	}
	
	/**
	 * <pre>Logs message to global logging file.</pre>
	 * Note: if {@link #turnOffGlobalLoggingFlag()} is called then logging does not occur
	 * @param message message to log
	 * @throws IOException throws error if logic error occurs
	 * @see #turnOffGlobalLoggingFlag()
	 */
	public void log(String message) throws IOException {
		// write to global logger
		if(globalLoggingFlag) {
			log(globalLogger, message);
		}
	}
	
	/**
	 * <pre>Logs message to added text log file(s). </pre>
	 * See {@link #addTextLogger(String)} {@link #addTextLogger(String, String)}
	 * @param filename file name where to log text message
	 * @param message text message to log
	 * @throws IOException throws error if logging error occurs
	 * @throws IllegalStateException throws error if specified file name does not added to container,
	 * use {@link #addTextLogger(String)} or {@link #addTextLogger(String, String)}
	 */
	public void log(String filename, String message) throws IOException {
		TextLogger logger = textLoggers.get(filename);
		if(logger == null) {
			throw new IllegalStateException("File name: " + filename + " does not exist.");
		}
		logger.log(message);
	}
	
	/**
	 * <pre>Logs new-line to global logging file.</pre>
	 * Note: if {@link #turnOffGlobalLoggingFlag()} is called then logging does not occur
	 * @throws IOException throws error if logging error occurs
	 */
	public void newline() throws IOException {
		// write to global logger
		if(globalLoggingFlag) {
			log(globalLogger, NDLDataUtils.NEW_LINE);
		}
	}
	
	/**
	 * <pre>Logs new-line to added text log file(s). </pre>
	 * See {@link #addTextLogger(String)} {@link #addTextLogger(String, String)}
	 * @param filename file name where to log text message
	 * @throws IOException throws error if logging error occurs
	 * @throws IllegalStateException throws error if specified file name does not added to container,
	 * use {@link #addTextLogger(String)} or {@link #addTextLogger(String, String)}
	 */
	public void newline(String filename) throws IOException {
		TextLogger logger = textLoggers.get(filename);
		if(logger == null) {
			throw new IllegalStateException("File name: " + filename + " does not exist.");
		}
		logger.log(NDLDataUtils.NEW_LINE);
	}
	
	/**
	 * <pre>Logs new-row to added CSV log file(s). </pre>
	 * See {@link #addCSVLogger(String, CSVConfiguration)}
	 * @param filename file name where to log message
	 * @throws IOException throws error if logging error occurs
	 * @throws IllegalStateException throws error if specified file name does not added to container,
	 * use {@link #addCSVLogger(String, CSVConfiguration)} or {@link #addCSVLogger(String, String[], CSVConfiguration)}
	 */
	public void newrow(String filename) throws IOException {
		CSVLogger logger = csvLoggers.get(filename);
		if(logger == null) {
			throw new IllegalStateException("File name: " + filename + " does not exist.");
		}
		logger.log(new String[]{}); // blank row
	} 
	
	/**
	 * <pre>Logs message to added CSV log file(s). </pre>
	 * See {@link #addCSVLogger(String, CSVConfiguration)}
	 * {@link #addCSVLogger(String, String[], CSVConfiguration)}
	 * @param filename file name where to log message
	 * @param messages message (csv column values) to log
	 * @throws IOException throws error if logging error occurs
	 * @throws IllegalStateException throws error if specified file name does not added to container,
	 * use {@link #addCSVLogger(String, CSVConfiguration)} or {@link #addCSVLogger(String, String[], CSVConfiguration)}
	 */
	public void log(String filename, String messages[]) throws IOException {
		CSVLogger logger = csvLoggers.get(filename);
		if(logger == null) {
			throw new IllegalStateException("File name: " + filename + " does not exist.");
		}
		logger.log(messages);
	}
	
	// TODO need to think whether a template method should exist to handle close resources
	/**
	 * <pre>Process data, use defined custom logic to be added here. </pre>
	 * The data container which processes data should extend this class and define this method
	 * <pre>Process data should close resources using {@link #close()}</pre>  
	 * @throws Exception throws exception in case of any data processing error happens
	 */
	public abstract void processData() throws Exception;
	
	/**
	 * Destroys the container, typically costly resources (logging files, configuration files)
	 * should be closed when data processing is done.
	 * @throws IOException throws error if destroy function fails to close resources
	 */
	public void close() throws IOException {
		// close resources
		for(String key : textLoggers.keySet()) {
			textLoggers.get(key).close();
		}
		for(String key : csvLoggers.keySet()) {
			csvLoggers.get(key).close();
		}
	}
	
	/**
	 * Gets file name by logical file name.
	 * @param filename returned file name is prefixed with logical name (if provided), see {@link #name}
	 * @param dateOnly only date flag tells whether to consider time-stamp
	 * @return returns resultant file name
	 */
	protected String getFileName(String filename, boolean dateOnly) {
		return NDLDataUtils.getSourceFullFileName(name, filename, dateFormatter, dateOnlyFormatter, dateOnly);
	}
	
	/**
	 * Gets file name by logical file name.
	 * @param filename returned file name is prefixed with logical name (if provided), see {@link #name}
	 * @return returns resultant file name
	 */
	protected String getFileName(String filename) {
		return getFileName(filename, false);
	}
	
	/**
	 * Gets file name by logical file name.
	 * @param dateOnly only date flag tells whether to consider time-stamp
	 * @return returns resultant file name
	 */
	protected String getFileName(boolean dateOnly) {
		return getFileName(null, dateOnly);
	}
	
	/**
	 * Gets file name by logical file name.
	 * @return returns resultant file name
	 */
	protected String getFileName() {
		return getFileName(null, false);
	}
}