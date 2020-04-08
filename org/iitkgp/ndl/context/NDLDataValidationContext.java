package org.iitkgp.ndl.context;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.iitkgp.ndl.context.exception.NDLConfigurationLoadException;
import org.iitkgp.ndl.data.NDLField;
import org.iitkgp.ndl.data.validator.NDLFieldSchemaDetail;
import org.iitkgp.ndl.data.validator.NDLSchemaDetail;
import org.iitkgp.ndl.data.validator.UniqueErrorTracker;
import org.iitkgp.ndl.validator.exception.NDLSchemaDetailLoadException;

/**
 * This class loads required configuration for NDL data validation context
 * See <b>/conf/default.data.validation.conf.properties</b> file for more details
 * @author Debasis
 */
public class NDLDataValidationContext {
	
	private static String NDL_DATA_VALIDATION_CONFIGURATION_FILE = "/conf/default.data.validation.conf.properties";
	private static NDLDataValidationContext context;
	
	private Properties configuration = new Properties();
	NDLSchemaDetail ndlSchemaDetail = null;
	// fields wise more schema details
	Map<String, NDLFieldSchemaDetail> moreSchemaDetails = new HashMap<String, NDLFieldSchemaDetail>(2);
	UniqueErrorTracker errorTracker = new UniqueErrorTracker();
	
	// default context loader
	NDLDataValidationContextLoader ctxLoader;
	
	// loads with custom loader
	private NDLDataValidationContext(NDLDataValidationContextLoader ctxLoader) {
		// private constructor to stop creating instance from outside
		this.ctxLoader = ctxLoader;
	}
	
	// initialization validation
	static void validate() {
		if(context == null) {
			throw new IllegalStateException("NDLConfigurationContext not initialized yet.");
		}
	}
	
	/**
	 * Initializes context and load required configurations.
	 * See <b>/conf/default.data.validation.conf.properties</b> file
	 * @throws NDLConfigurationLoadException throws error in case of loading error
	 * (typically configuration file not found)
	 */
	public static void init() throws NDLConfigurationLoadException {
		// TODO singletone could be designed without using synchronized block
		init(NDL_DATA_VALIDATION_CONFIGURATION_FILE);
	}
	
	/**
	 * Reloads context and load required configurations defined in user defined file.
	 * @param resource user defined file to load
	 * @param loader custom context loader
	 * @throws NDLConfigurationLoadException throws error in case of loading error
	 * (typically configuration file not found)
	 */
	public static synchronized void reload(String resource, NDLDataValidationContextLoader loader)
			throws NDLConfigurationLoadException {
		init(resource, loader);
	}
	
	/**
	 * Reloads context and load required configurations defined in user defined file.
	 * @param loader custom context loader
	 * @throws NDLConfigurationLoadException throws error in case of loading error
	 * (typically configuration file not found)
	 */
	public static synchronized void reload(NDLDataValidationContextLoader loader) throws NDLConfigurationLoadException {
		init(NDL_DATA_VALIDATION_CONFIGURATION_FILE, loader);
	}
	
	/**
	 * Initializes context and load required configurations defined in user defined file.
	 * @param resource user defined file to load
	 * @throws NDLConfigurationLoadException throws error in case of loading error
	 * (typically configuration file not found)
	 */
	public static synchronized void init(String resource) throws NDLConfigurationLoadException {
		init(resource, new DefaultNDLDataValidationContextLoader()); // default context loader
	}
	
	/**
	 * Initializes context and load required configurations defined in user defined file.
	 * @param resource user defined file to load
	 * @param loader custom context loader
	 * @throws NDLConfigurationLoadException throws error in case of loading error
	 * (typically configuration file not found)
	 */
	public static synchronized void init(String resource, NDLDataValidationContextLoader ctxLoader)
			throws NDLConfigurationLoadException {
		// TODO singletone could be designed without using synchronized block
		if(context == null) {
			
			System.out.println("NDL validation context initialization...");
			
			context = new NDLDataValidationContext(ctxLoader);
			try {
				// load properties
				context.configuration.load(NDLDataValidationContext.class.getResourceAsStream(resource));
			} catch(IOException ex) {
				// error
				throw new NDLConfigurationLoadException("ERROR: " + ex.getMessage(), ex.getCause());
			}
			try {
				// NDL field details load
				context.ndlSchemaDetail = context.ctxLoader.loadSchemaDetail();
			} catch(NDLSchemaDetailLoadException ex) {
				// suppress the error and put a warning
				System.err.println(ex.getMessage());
				// fallback strategy
				context.ndlSchemaDetail = NDLSchemaDetail.createBlankInstance();
			}
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
	 * removes existing configuration
	 * @param key given existing configuration parameter
	 */
	public static void removeConfiguration(String key) {
		validate(); // validate before operation
		context.configuration.remove(key);
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
	
	// loads individual constraint detail
	void loadSchemaDetail(String field) throws NDLSchemaDetailLoadException {
		validate(); // validate before operation
		if(moreSchemaDetails.containsKey(field)) {
			// already loaded, reuse it
			return;
		}
		// individual schema detail
		try {
			NDLFieldSchemaDetail detail = ctxLoader.loadSchemaDetail(field);
			moreSchemaDetails.put(field, detail); // put into cache
		} catch(Exception ex) {
			// suppress the error and put a warning
			System.err.println(ex.getMessage());
		}
	}
	
	/**
	 * Gets field wise more schema detail (typically control vocabulary details)
	 * @param field field name
	 * @return returns more details
	 */
	public static NDLFieldSchemaDetail getSchemaDetail(String field) {
		validate(); // validate before operation
		context.loadSchemaDetail(field);
		return context.moreSchemaDetails.get(field);
	}
	
	/**
	 * Gets global schema detail
	 * @return returns schema detail
	 */
	public static NDLSchemaDetail getSchemaDetail() {
		validate(); // validate before operation
		return context.ndlSchemaDetail;
	}
	
	/**
	 * Gets value for a given key
	 * @param key required key for which value is required
	 * @return Returns associated value for that key, otherwise NULL
	 */
	public static String getConfiguration(String key) {
		validate(); // validate before operation
		return context.configuration.getProperty(key);
	}
	
	/**
	 * Gets NDL JSON-keyed fields
	 * @return returns NDL JSON-keyed fields
	 */
	public static Set<String> getNDLJSONKeyedFields(){
		validate(); // validate before operation
		return context.ndlSchemaDetail.getNDLJSONKeyedFields();
	}
	
	/**
	 * Gets NDL fields which managed by control vocabulary
	 * @return returns NDL fields which managed by control vocabulary
	 */
	public static Set<String> getControlFields() {
		validate(); // validate before operation
		return context.ndlSchemaDetail.getControlFields();
	}
	
	/**
	 * Returns whether field is valid field or not
	 * @param field field name to check
	 * @return returns true if valid, otherwise false 
	 */
	public static boolean isValidField(String field) {
		validate(); // validate before operation
		return context.ndlSchemaDetail.isValidField(field);
	}
	
	/**
	 * Checks whether a value/key is valid for a given controller field
	 * @param field given field to check
	 * @param checkpoint given controlled value/key to check
	 * @return returns true whether validity succeeds otherwise false
	 */
	public static boolean isValidControlledFieldValue(String field, String checkpoint) {
		validate(); // validate before operation
		if(!isValidField(field)) {
			// not a valid field
			return false;
		}
		if(context.ndlSchemaDetail.isCtrl(field)) {
			// controlled value
			NDLFieldSchemaDetail detail = getSchemaDetail(field);
			return detail.getControlledValues(field).contains(checkpoint); // whether value exists
		} else if(context.ndlSchemaDetail.isCtrlKey(field)) {
			// controlled keyed value
			NDLFieldSchemaDetail detail = getSchemaDetail(field);
			return detail.getControlledKeys(field).contains(checkpoint); // whether key exists
		} else {
			// other than control vocabulary, assumes it's valid
			return true;
		}
	}
	
	/**
	 * Display error field along with value if any
	 * @param field field detail
	 * @param value associated value
	 */
	public static void displayError(NDLField field, String value) {
		validate(); // validate before operation
		context.errorTracker.displayError(field, value);
	}
	/**
	 * Display error field along with value if any
	 * @param field field detail
	 */
	public static void displayError(NDLField field) {
		validate(); // validate before operation
		context.errorTracker.displayError(field, null);
	}
}