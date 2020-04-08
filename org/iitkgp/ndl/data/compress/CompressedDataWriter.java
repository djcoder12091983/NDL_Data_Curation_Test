package org.iitkgp.ndl.data.compress;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Compress data writer abstract logic
 * @author Debasis
 */
public interface CompressedDataWriter extends Closeable {
	
	/**
	 * Initializes writer (open target to start write data)
	 * @throws IOException throws error when writer initialization fails
	 * (typically when target opening error occurs)
	 */
	void init() throws IOException;

	/**
	 * Writes multiple entry details into target
	 * @param contents Contents to writer (entry name and it's associated byte contents)
	 * @throws IOException throws error when writing error occurs
	 */
	void write(Map<String, byte[]> contents) throws IOException;
	
	/**
	 * Writes single entry details into target
	 * @param name entry name
	 * @param bytes associated contents
	 * @throws IOException throws error when writing error occurs
	 */
	void write(String name, byte[] bytes) throws IOException;
	
	/**
	 * Closes target after writing is complete
	 * @throws IOException throws error target closing error occurs
	 */
	void close() throws IOException;
	
	/**
	 * Returns file
	 * @return returns file
	 */
	File file();
}