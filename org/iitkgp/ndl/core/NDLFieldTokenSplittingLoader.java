package org.iitkgp.ndl.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.iitkgp.ndl.core.exception.NDLFieldTokenSplittingConfigurationLoadingException;
import org.iitkgp.ndl.data.validator.AbstractNDLDataValidator;

/**
 * Context loader for tokens split for text to load into Prefix-Tree
 * See <b>/conf/field.token.splitter.properties</b> for more details.
 * This file maps field wise splitting logic if required.
 * See {@link AbstractNDLDataValidator} for unique field tracking for a given source-data
 * @see NDLMap
 * @see NDLSet
 * @author Debasis
 */
public class NDLFieldTokenSplittingLoader {
	
	// configuration file
	static String FIELD_TOKEN_SPLITTER_FILE = "/conf/field.token.splitter.properties";
	
	// splitter cache
	Map<String, NDLFieldTokenSplitter<String, String[]>> splitters = new HashMap<String, NDLFieldTokenSplitter<String, String[]>>(2);
	// mapping
	Properties splittingConfiguration = new Properties();

	/**
	 * Constructor
	 * @throws NDLFieldTokenSplittingConfigurationLoadingException Throws error if loading error occurs
	 */
	public NDLFieldTokenSplittingLoader() throws NDLFieldTokenSplittingConfigurationLoadingException {
		try {
			// loading configuration
			splittingConfiguration.load(getClass().getResourceAsStream(FIELD_TOKEN_SPLITTER_FILE));
		} catch(IOException ex) {
			// error
			throw new NDLFieldTokenSplittingConfigurationLoadingException(ex.getMessage(), ex.getCause());
		}
	}
	
	/**
	 * Loads token splitter for a given field
	 * @param field field for which splitter needs to be loaded
	 * @return returns splitter if found otherwise NULL
	 * @throws NDLFieldTokenSplittingConfigurationLoadingException Throws error if loading error(class loading error) occurs
	 */
	public NDLFieldTokenSplitter<String, String[]> loadTokenSplitter(String field)
			throws NDLFieldTokenSplittingConfigurationLoadingException {
		NDLFieldTokenSplitter<String, String[]> splitter = splitters.get(field);
		if(splitter == null) {
			// frist time
			String clazz = splittingConfiguration.getProperty(field);
			if(clazz == null) {
				// error (mapping not found)
				return null;
			}
			try {
				return (NDLFieldTokenSplitter)Class.forName(clazz).newInstance();
			} catch(Exception ex) {
				// error
				throw new NDLFieldTokenSplittingConfigurationLoadingException(ex.getMessage(), ex.getCause());
			}
		} else {
			// cached
			return splitter;
		}
	}
}