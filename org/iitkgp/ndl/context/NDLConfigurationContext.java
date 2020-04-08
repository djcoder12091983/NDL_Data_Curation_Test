package org.iitkgp.ndl.context;

import java.io.IOException;
import java.util.Properties;

import org.iitkgp.ndl.context.exception.NDLConfigurationLoadException;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * <pre>This class loads required configuration for NDL global context
 * See <b>/conf/default.global.configuration.properties</b> file for more details</pre>
 * @see NDLDataValidationContext
 * @author Debasis
 */
public class NDLConfigurationContext {
	
	// singletone class (init method ensures only one instance)
	
	// context file associated with it
	private static String GLOBAL_NDL_DATA_CONFIGURATION_FILE = "/conf/default.global.configuration.properties";
	private static NDLConfigurationContext context;
	
	private Properties configuration = new Properties();
	
	private NDLConfigurationContext() {
		// private constructor to stop creating instance from outside
	}
	
	// initialization validation
	static void validate() {
		if(context == null) {
			throw new IllegalStateException("NDLConfigurationContext not initialized yet.");
		}
	}
	
	/**
	 * Initializes context and load required configurations.
	 * See <b>/conf/default.global.configuration.properties</b> file
	 * @throws NDLConfigurationLoadException throws error in case of loading error
	 * (typically configuration file not found)
	 */
	public static void init() throws NDLConfigurationLoadException {
		// TODO singletone could be designed without using synchronized block
		init(GLOBAL_NDL_DATA_CONFIGURATION_FILE);
	}
	
	/**
	 * Initializes context and load required configurations defined in user defined file.
	 * @param resource user defined file to load
	 * @throws NDLConfigurationLoadException throws error in case of loading error
	 * (typically configuration file not found)
	 */
	public static synchronized void init(String resource) throws NDLConfigurationLoadException {
		// TODO singletone could be designed without using synchronized block
		if(context == null) {
			
			System.out.println("NDL global context initialization...");
			
			context = new NDLConfigurationContext();
			try {
				// load properties
				context.configuration.load(NDLConfigurationContext.class.getResourceAsStream(resource));
			} catch(IOException ex) {
				// error
				throw new NDLConfigurationLoadException("ERROR: " + ex.getMessage(), ex.getCause());
			}
		}
	}
	
	/**
	 * Adds another configurations defined in user defined file
	 * @param resource user defined file to load
	 * @throws NDLConfigurationLoadException throws error in case of loading error
	 * (typically configuration file not found)
	 */
	public static void addConfiguration(String resource) throws NDLConfigurationLoadException {
		validate(); // validate before operation
		try {
			// load properties
			context.configuration.load(NDLConfigurationContext.class.getResourceAsStream(resource));
		} catch(IOException ex) {
			// error
			throw new NDLConfigurationLoadException("ERROR: " + ex.getMessage(), ex.getCause());
		}
	}
	
	/**
	 * Adds configuration key-value pair
	 * @param key required key
	 * @param value associated value for that key
	 */
	public static void addConfiguration(String key, String value) {
		// set configuration by key/value pair
		validate(); // validate before operation
		context.configuration.setProperty(key, value);
	}
	
	/**
	 * Gets value for a given key
	 * @param key required key for which value is required
	 * @return Returns associated value for that key, otherwise NULL
	 * @see NDLConfigurationContext#getConfiguration(String, String)
	 */
	public static String getConfiguration(String key) {
		validate(); // validate before operation
		return context.configuration.getProperty(key);
	}
	
	/**
	 * checks whether configuration contains configuration key
	 * @param key given configuration key
	 * @return returns true if exists otherwise false
	 */
	public static boolean containsConfiguration(String key) {
		validate(); // validate before operation
		return context.configuration.contains(key);
	}
	
	/**
	 * removes existing configuration
	 * @param key given existing configuration parameter
	 */
	public static void removeConfiguration(String key) {
		validate(); // validate before operation
		context.configuration.remove(key);
	}
	
	/**
	 * Gets value for a given key
	 * <pre>if context not initialized then default value returns</pre>
	 * @param key required key for which value is required
	 * @param defaultValue default value is used when context not initialized
	 * @return Returns associated value for that key, otherwise default value
	 */
	public static String getConfiguration(String key, String defaultValue) {
		if(initialized()) {
			return NDLDataUtils.NVL(getConfiguration(key), defaultValue);
		} else {
			return defaultValue;
		}
	}
	
	/**
	 * Checks whether context is initialized or not
	 * @return returns true if so, otherwise false
	 */
	public static boolean initialized() {
		return context != null;
	}
}