package org.iitkgp.ndl.data.iterator;

import java.io.IOException;

/**
 * Data reader abstract logic
 * @author Debasis
 */
public interface DataReader {
	
	/**
	 * Initializes reader (open source to start reading data)
	 * @throws IOException throws error when reader initialization fails
	 * (typically when source opening error occurs)
	 */
	void init() throws IOException;

	/**
	 * Returns next entry and associated contents
	 * @return returns data
	 * @throws IOException throws error when reading error occurs
	 */
	DataItem next() throws IOException;
	
	/**
	 * Close the source after reading is completed
	 * @throws IOException throws error when source closing error occurs
	 */
	void close() throws IOException;

}