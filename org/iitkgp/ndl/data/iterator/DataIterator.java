package org.iitkgp.ndl.data.iterator;

import java.io.IOException;

import org.xml.sax.SAXException;

/**
 * Data iterator to iterate data for a data collection
 * @param <D> Data row representation
 * @param <C> Source configuration if any
 * @author Debasis, Vishal
 */
public abstract class DataIterator<D, C> {
	
	C configuration; // saves configuration for reset
	
	/**
	 * Initializes data iterator, typically opening resources
	 * @param configuration source configuration to initializes data source
	 * @throws IOException throws error if initialization fails
	 */
	public void init(C configuration) throws IOException {
		this.configuration = configuration; // saves configuration for reset
	}
	
	/**
	 * Resets data reader
	 * @throws Exception throws error if operation fails to do so
	 */
	public void reset() throws Exception {
		close(); // closes then initializes again
		init(configuration);
	}
	
	/**
	 * Turn off asset loading flag when asset loading is not required
	 */
	public abstract void turnOffAssetLoadingFlag();
	
	/**
	 * Destroys data iterator, typically closing resources
	 * @throws IOException throws error if destruction fails
	 */
	public abstract void close() throws IOException;
	
    /**
     * returns next item if any, otherwise NULL
     * @return returns the next item if any otherwise NULL
     * @throws IOException throws error if data extraction I/O related error occurs
     * @throws SAXException throws error if XML parsing error occurs to generate data item
     */
	public abstract D next() throws IOException, SAXException;
    
    /**
     * checks whether next item available or not
     * @return returns true if next item available, otherwise false
     * @throws IOException throws error if data extraction I/O related error occurs
     * @throws SAXException throws error if XML parsing error occurs to generate data item
     */
	public abstract boolean hasNext() throws IOException, SAXException;
    
    /**
	 * Returns true/false whether compression mode is on for reading
	 * @return if compression mode is ON then returns true otherwise false
	 */
	public abstract boolean isCompressed();
}