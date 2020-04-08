package org.iitkgp.ndl.data.compress;

import java.io.IOException;

import org.iitkgp.ndl.data.iterator.DataReader;

/**
 * Compress data reader abstract logic
 * @author Debasis
 */
public interface CompressedDataReader extends DataReader {
	
	/**
	 * Initializes reader (open source to start reading data)
	 * @throws IOException throws error when reader initialization fails
	 * (typically when source opening error occurs)
	 */
	void init() throws IOException;

	/**
	 * Returns next compressed entry and associated contents
	 * @return returns {@link CompressedDataItem}
	 * @throws IOException throws error when reading error occurs
	 */
	CompressedDataItem next() throws IOException;
	
	/**
	 * Close the source after reading is completed
	 * @throws IOException throws error when source closing error occurs
	 */
	void close() throws IOException;
}