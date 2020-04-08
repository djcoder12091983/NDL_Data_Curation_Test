package org.iitkgp.ndl.context.custom;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.context.NDLDataValidationContext;
import org.iitkgp.ndl.context.custom.exception.NDLContextSwitchLoadException;
import org.iitkgp.ndl.context.exception.NDLConfigurationLoadException;
import org.iitkgp.ndl.data.normalizer.NDLDataNormalizationPool;
import org.iitkgp.ndl.data.normalizer.NDLDataNormalizer;

/**
 * NDL context switching.
 * On each context has it's own configuration so that it can behave from it's own
 * @author Debasis
 */
public class NDLContextSwitch {
	
	private static final String CUSTOM_CONFIGURATION_FILE_PREFIX = "ndl.context.switch.";
	private static final String CUSTOM_CONFIGURATION_FILE_SUFFIX = ".conf.properties";
	
	private static final String CUSTOM_CONTEXT_LOADER_PREFIX = "ndl.context.";
	private static final String CUSTOM_CONTEXT_LOADER_SUFFIX = ".switch.loader";
	
	private static String CONFIGURATION_FILE = "/conf/custom/ndl.context.switch.conf.properties";
	private static NDLContextSwitch context;
	
	private Properties configuration = new Properties();
	// loaders map
	private Map<String, NDLContextSwitchLoader> loaders = new HashMap<>(2);
	
	// custom additional context specific configurations
	// hold previous values
	Map<String, NDLDataNormalizer> previousnormalizationConf = new HashMap<>();
	Map<String, String> previousValidationConf = new HashMap<>();
	Map<String, String> previousGlobalConf = new HashMap<>();
	NDLContextSwitchLoader previous; // previous loader
	
	private NDLContextSwitch() {
		// private constructor to stop creating instance from outside
	}
	
	// initialization validation
	static void validate() {
		if(context == null) {
			throw new IllegalStateException("NDLContextSwitch not initialized yet.");
		}
	}
	
	/**
	 * Initializes context with given default resource file
	 * @throws NDLContextSwitchLoadException throws error in case of loading error
	 */
	public static synchronized void init() throws NDLContextSwitchLoadException { 
		init(CONFIGURATION_FILE);
	}
	
	/**
	 * Initializes context with given resource file
	 * @param resource given resource file
	 * @throws NDLContextSwitchLoadException throws error in case of loading error
	 */
	public static synchronized void init(String resource) throws NDLContextSwitchLoadException {
		// TODO singletone could be designed without using synchronized block
		if(context == null) {
			System.out.println("Context switch configuration loading.....");
			context = new NDLContextSwitch();
			try {
				// load properties
				context.configuration.load(NDLContextSwitch.class.getResourceAsStream(resource));
			} catch(IOException ex) {
				// error
				throw new NDLConfigurationLoadException("ERROR: " + ex.getMessage(), ex.getCause());
			}
		}
	}
	
	/**
	 * returns context switch loader by given context name
	 * @param contextName given context name
	 * @return returns context switch loader if found otherwise throws exception
	 * @throws NDLContextSwitchLoadException throws error in case of loading error
	 */
	static NDLContextSwitchLoader getContextSwitchLoader(NDLContext contextName,
			NDLDataNormalizationPool normalizationPool) throws NDLContextSwitchLoadException {
		String name = CUSTOM_CONTEXT_LOADER_PREFIX + contextName.getContext() + CUSTOM_CONTEXT_LOADER_SUFFIX;
		if(context.loaders.containsKey(name)) {
			// returns cached loader
			return context.loaders.get(name);
		}
		try {
			String loaderClazz = context.configuration.getProperty(name);
			if(loaderClazz != null) {
				// try to load a new instance if not cached
				Class<?> clazz = Class.forName(loaderClazz);
				Constructor<?> constructor = clazz.getConstructor();
				NDLContextSwitchLoader loader = (NDLContextSwitchLoader)constructor.newInstance();
				
				// sets context(s)
				loader.setNormalizationPool(normalizationPool);
				
				context.loaders.put(name, loader); // cache it
				return loader;
			} else {
				// error
				throw new NDLContextSwitchLoadException("Loader: " + loaderClazz + " not found.");
			}
		} catch(Exception ex) {
			// error
			throw new NDLContextSwitchLoadException(ex.getMessage(), ex.getCause());
		}
	}
	
	/**
	 * Switching context to custom context
	 * @param contextName custom context
	 * @param normalizationPool this pool helps to register additional normalizers
	 * @throws NDLContextSwitchLoadException throws error in case of loading error
	 */
	public static synchronized void switchContext(NDLContext contextName, NDLDataNormalizationPool normalizationPool)
			throws NDLContextSwitchLoadException {
		validate(); // validate before operation
		
		if(contextName != null) {
			System.out.println("Switching to: " + contextName.getContext());
		}
		
		// clears previous settings
		for(String key : context.previousGlobalConf.keySet()) {
			String value = context.previousGlobalConf.get(key);
			if(value != null) {
				// replace with old value
				NDLConfigurationContext.addConfiguration(key, context.previousGlobalConf.get(key));
			} else {
				// straight way remove
				NDLConfigurationContext.removeConfiguration(key);
			}
		}
		for(String key : context.previousValidationConf.keySet()) {
			String value = context.previousValidationConf.get(key);
			if(value != null) {
				// replace with old value
				NDLDataValidationContext.addConfiguration(key, context.previousValidationConf.get(key));
			} else {
				// straight way remove
				NDLDataValidationContext.removeConfiguration(key);
			}
		}
		
		NDLContextSwitchLoader prev = context.previous;
		if(prev != null) {
			// clears previous settings
			for(String field : context.previousnormalizationConf.keySet()) {
				NDLDataNormalizer normalizer = context.previousnormalizationConf.get(field);
				if(normalizer != null) {
					// replace with old value
					prev.normalizationPool.addNormalizer(field, normalizer);
				} else {
					// straight way remove
					prev.normalizationPool.deregisterNormalizer(field);
				}
			}
			
			// use previous normalizer pool
			normalizationPool = prev.normalizationPool;
		}
		
		// clears all map
		context.previousGlobalConf.clear();
		context.previousnormalizationConf.clear();
		context.previousValidationConf.clear();
		
		if(contextName != null) {
			// validation
			if(normalizationPool == null) {
				// illegal state exception
				throw new IllegalArgumentException("Normalization pool can't be NULL.");
			}
			// switching context
			try {
				// loads context switching loader
				NDLContextSwitchLoader loader = getContextSwitchLoader(contextName, normalizationPool);
				
				// loads custom resource file
				String resource = "/conf/custom/" + CUSTOM_CONFIGURATION_FILE_PREFIX + contextName.getContext()
						+ CUSTOM_CONFIGURATION_FILE_SUFFIX;
				loader.load(context, resource);
				
				// saves to previous
				context.previous = loader;
			} catch(Exception ex) {
				// error
				throw new NDLConfigurationLoadException("ERROR: " + ex.getMessage(), ex.getCause());
			}
		}
	}
	
	/**
	 * Restores context to original
	 * @throws NDLContextSwitchLoadException throws error in case of loading error 
	 */
	public static void restoreContext() throws NDLContextSwitchLoadException {
		validate(); // validate before operation
		
		if(context.previous == null) {
			throw new NDLContextSwitchLoadException("No context switched yet.");
		}
		
		System.out.println("Restoring context....");
		
		// switching back to original
		switchContext(null, null);
		
		// TODO restoring context to original
		// NDLDataValidationContext.reload(new DefaultNDLDataValidationContextLoader());
		// NDLDataValidationUtils.reset();
	}
	
	/**
	 * context destroy
	 */
	public static void destroy() throws Exception {
		System.out.println("Context switch destroy....");
		// TODO
		// needs to think what could be the logic of context destroy
	}
}