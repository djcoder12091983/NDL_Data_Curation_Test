package org.iitkgp.ndl.data.normalizer;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.normalizer.exception.DataNormalizationException;
import org.iitkgp.ndl.data.normalizer.exception.DataNormalizationLoadException;

/**
 * NDL data normalization service pool, where service normalizers available.
 * See <b>/conf/default.data.normalization.conf.properties</b> 
 * @author Debasis
 */
public class NDLDataNormalizationPool {
	
	static String DATA_NORMALIZATION_CONF_FILE = "/conf/default.data.normalization.conf.properties";
	
	Map<String, NDLDataNormalizer> normalizers = new HashMap<String, NDLDataNormalizer>(2);
	Set<String> excludes = new HashSet<String>(2); // deregistered normalizers list
	Properties normalizationConfiguration = new Properties(); // normalization mapped by class name
	boolean normalizationFlag = true; // flag to indicate whether to normalize or not
	
	/**
	 * Constructor, loads <b>default.data.normalization.conf.properties</b> file to pool.
	 * @throws DataNormalizationLoadException throws error in case of normalization configuration load error occurs 
	 */
	public NDLDataNormalizationPool() throws DataNormalizationLoadException {
		try {
			// loading configuration
			normalizationConfiguration.load(getClass().getResourceAsStream(DATA_NORMALIZATION_CONF_FILE));
		} catch(IOException ex) {
			// error
			throw new DataNormalizationLoadException(ex.getMessage(), ex.getCause());
		}
	}
	
	/**
	 * Adds normalizer against a field, if additionally normalizer needs to be loaded which is not defined
	 * in <b>/conf/default.data.normalization.conf.properties</b>
	 * @param field field with which normalizer is associated
	 * @param normalizer normalizer class, it should be type of {@link NDLDataNormalizer}
	 */
	public void addNormalizer(String field, String normalizer) {
		// add or override
		normalizationConfiguration.setProperty(field, normalizer);
	}
	
	/**
	 * Adds normalizer as class instance
	 * @param field for which field normalizer to be added
	 * @param normalizer normalizer to be added
	 */
	public void addNormalizer(String field, NDLDataNormalizer normalizer) {
		normalizers.put(field, normalizer);
	}
	
	/**
	 * Deregisters normalizer for a given field
	 * @param field given field for which normalizer to be deregistered. 
	 */
	public void deregisterNormalizer(String field) {
		excludes.add(field);
	}
	
	/**
	 * Deregisters all normalizer
	 */
	public void deregisterAllNormalizers() {
		// remove all normalizers
		normalizationFlag = false;
	}
	
	/**
	 * Normalizes field value by a registered normalizer, if normalizer not found then without normalization original value is returned
	 * @param field field name, by which system finds mapped normalizer
	 * @param multivalueSeparator values to be splitted by a character to multiple tokens if required
	 * @param value value to be normalized
	 * @return returns normalized values
	 * @throws DataNormalizationException throws error normalization fails
	 * @throws DataNormalizationLoadException throws error when normalizer is not found 
	 */
	public Set<String> normalize(String field, Character multivalueSeparator, String value)
			throws DataNormalizationException, DataNormalizationLoadException {
		if(StringUtils.isBlank(value)) {
			// invalid
			return Collections.<String>emptySet();
		}
		String normalizerClazz = normalizationConfiguration.getProperty(field);
		NDLDataNormalizer loadedNormalizer = normalizers.get(field); // already loaded/mapped normalizer
		Set<String> normalizedValues = new LinkedHashSet<String>(2); // maintains the order
		if(!normalizationFlag || excludes.contains(field) || (normalizerClazz == null && loadedNormalizer == null)) {
			// no normalizer
			if(multivalueSeparator != null) {
				// multiple tokens
				StringTokenizer tokenizer = new StringTokenizer(value.trim(), String.valueOf(multivalueSeparator));
				while(tokenizer.hasMoreTokens()) {
					String val = tokenizer.nextToken().trim();
					normalizedValues.add(val);
				}
			} else {
				// single token
				String val = value.trim();
				normalizedValues.add(val);
			}
		} else {
			// normalizer mapped
			NDLDataNormalizer normalizer = (loadedNormalizer != null ? loadedNormalizer
					: loadNormalizer(normalizerClazz, multivalueSeparator));
			normalizedValues.addAll(normalizer.normalize(value));
		}
		return normalizedValues;
	}
	
	/**
	 * returns registered normalizer for a given field
	 * @param field given field
	 * @return returns registered normalizer if found otherwise NULL
	 */
	public NDLDataNormalizer registeredNormalizer(String field) {
		return normalizers.get(field);
	}
	
	// loads normalizer for a given field, otherwise NULL
	// normalizers are mapped with field and multiple-value separator character
	// it may happen, same field is normalized using different multiple separator
	NDLDataNormalizer loadNormalizer(String clazz, Character multivalueSeparator) throws DataNormalizationLoadException {
		String key= getClassKey(clazz, multivalueSeparator);
		if(normalizers.containsKey(key)) {
			// return from cached values
			return normalizers.get(key);
		}
		try {
			// try to load a new instance if not cached
			Class<?> normalizerClazz = Class.forName(clazz);
			NDLDataNormalizer normalizer = null;
			if(multivalueSeparator != null) {
				// with separator
				Constructor<?> constructor = normalizerClazz.getConstructor(new Class[]{char.class});
				normalizer = (NDLDataNormalizer)constructor.newInstance(multivalueSeparator);
			} else {
				// without separator
				normalizer = (NDLDataNormalizer)normalizerClazz.newInstance();
			}
			normalizers.put(key, normalizer); // cache it
			return normalizer;
		} catch(Exception ex) {
			// error
			throw new DataNormalizationLoadException(ex.getMessage(), ex.getCause());
		}
	}
	
	// class key to register class instance
	String getClassKey(String clazz, Character multivalueSeparator) {
		String key;
		if(multivalueSeparator != null) {
			key = "[" + clazz + "][" + multivalueSeparator + "]";
		} else {
			key = "[" + clazz + "]";
		}
		return key;
	}
	
	/**
	 * Normalizes field value
	 * @param field field name
	 * @param value value to be normalized
	 * @return returns normalized values
	 * @throws DataNormalizationException throws error normalization fails
	 * @throws DataNormalizationLoadException throws error when normalizer is not found
	 * @see #normalize(String, Character, String)
	 */
	public Set<String> normalize(String field, String value)
			throws DataNormalizationException, DataNormalizationLoadException {
		return normalize(field, null, value);
	}

	/**
	 * checks whether given field is registered with normalizer
	 * @param field given field
	 * @return returns true if so otherwise false
	 */
	public boolean isregistered(String field) {
		return normalizers.containsKey(field);
	}
}