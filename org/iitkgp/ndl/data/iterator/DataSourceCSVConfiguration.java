package org.iitkgp.ndl.data.iterator;

import org.iitkgp.ndl.data.CSVConfiguration;

/**
 * <pre>Data source CSV configuration: escape, quote, separator details</pre>
 * This class simply extends {@link CSVConfiguration} 
 * @see CSVConfiguration
 * @author Debasis
 */
public class DataSourceCSVConfiguration extends CSVConfiguration {
	
	int multilineLimit = 10000;
	
	/**
	 * Constructor
	 * @param separator separator character to differentiate multiple values 
	 * @param quote quote character to wrap cell values
	 */
	public DataSourceCSVConfiguration(char separator, char quote) {
		super(separator, quote);
	}
	
	/**
	 * Constructor
	 * @param separator separator character to differentiate multiple values
	 * @param quote quote character to wrap cell values
	 * @param escape escape character
	 */
	public DataSourceCSVConfiguration(char separator, char quote, char escape) {
		super(separator, quote, escape);
	}
	
	/**
	 * Sets multiple line limit
	 * @param multilineLimit multiple line limit
	 */
	public void setMultilineLimit(int multilineLimit) {
		this.multilineLimit = multilineLimit;
	}
	
	/**
	 * Gets multiple line limit
	 * @return returns multiple line limit
	 */
	public int getMultilineLimit() {
		return multilineLimit;
	}
}