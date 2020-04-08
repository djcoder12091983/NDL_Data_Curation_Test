package org.iitkgp.ndl.data.writer;

import java.io.Closeable;
import java.io.IOException;

/**
 * This contract defines the specification of data writing process
 * @param <T> Which type of data to write
 * @author Debasis
 */
public interface DataWriter<T> extends Closeable {

	/**
	 * Initializes writer, open the writer output stream
	 * @throws IOException throws exception if initialization fails
	 */
	public void init() throws IOException;
	
	/**
	 * Writes data item
	 * @param item data item to write
	 * @throws IOException throws exception if writing fails
	 */
	public void write(T item) throws IOException;
	
	/**
	 * Destroys the writer, typically closes writer
	 * @throws IOException throws error if writer closing fails
	 */
	public void close() throws IOException;
}