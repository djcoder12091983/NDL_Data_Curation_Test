package org.iitkgp.ndl.context.custom;

import java.util.Properties;

import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.context.NDLDataValidationContext;
import org.iitkgp.ndl.data.normalizer.NDLDataNormalizationPool;
import org.iitkgp.ndl.data.normalizer.NDLDataNormalizer;

/**
 * NDL context switch loader
 * @author Debasis
 */
public abstract class NDLContextSwitchLoader {
	
	// domain to load into specific context
	static final String VALIDATION_DOMAIN = "validation";
	static final String NORMALIZATION_DOMAIN = "normalizer";
	static final String GLOBAL_DOMAIN = "global";
	
	// associated normalizer configuration pool
	NDLDataNormalizationPool normalizationPool;
	
	/**
	 * sets normalization pool
	 * @param normalizationPool normalization pool
	 */
	public void setNormalizationPool(NDLDataNormalizationPool normalizationPool) {
		this.normalizationPool = normalizationPool;
	}

	/**
	 * loads the context by given resource file
	 * <ul>
	 * <li>if property starts with <pre>validation</pre>, then it goes to validation context</li>
	 * <li>if property starts with <pre>normalizer</pre>, then it goes to normalization context</li>
	 * <li>if property starts with <pre>global</pre>, then it goes to global context</li>
	 * </ul>
	 * @param parent parent context switching loader
	 * @param given resource to load additional settings
	 * @throws Exception throws Exception in case of loading error occurs
	 */
	protected void load(NDLContextSwitch parent, String resource) throws Exception {
		Properties customConf = new Properties();
		// reads from settings file
		customConf.load(NDLContextSwitchLoader.class.getResourceAsStream(resource));
		for(Object key : customConf.keySet()) {
			
			String keyName = key.toString();
			int p = keyName.indexOf('.');
			String domain = keyName.substring(0, p); // specific domain
			String domainKey = keyName.substring(p + 1); // domain specific key
			
			// associated value
			String value = customConf.getProperty(keyName);
			
			if(domain.equals(VALIDATION_DOMAIN)) {
				// validation domain
				// saves previous settings
				String old = NDLDataValidationContext.getConfiguration(domainKey);
				parent.previousValidationConf.put(domainKey, old);
				// adds new settings
				NDLDataValidationContext.addConfiguration(domainKey, value);
			} else if(domain.equals(NORMALIZATION_DOMAIN)) {
				// normalization domain
				// saves previous settings
				NDLDataNormalizer prevn = normalizationPool.registeredNormalizer(domainKey);
				parent.previousnormalizationConf.put(domainKey, prevn);
				// adds new settings
				normalizationPool.addNormalizer(domainKey, value);
			} else if(domain.equals(GLOBAL_DOMAIN)) {
				// global settings domain
				// saves previous settings
				String old = NDLConfigurationContext.getConfiguration(domainKey);
				parent.previousGlobalConf.put(domainKey, old);
				// adds new settings
				NDLConfigurationContext.addConfiguration(domainKey, value);
			} else {
				// error
				throw new IllegalArgumentException("Unknown domain: " + domain);
			}
		}
	}
}