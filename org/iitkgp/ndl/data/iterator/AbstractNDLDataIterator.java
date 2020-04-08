package org.iitkgp.ndl.data.iterator;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.data.NDLDataItem;
import org.iitkgp.ndl.data.compress.CompressedDataItem;
import org.iitkgp.ndl.data.compress.CompressedDataReader;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.xml.sax.SAXException;

/**
 * Abstract data iterator which specific to NDL data item (SIP/AIP etc.)
 * @param T Data row representation
 * @author Debasis, Vishal
 */
public abstract class AbstractNDLDataIterator<T extends NDLDataItem>
		extends DataIterator<T, DataSourceNULLConfiguration> implements Closeable {

	File input; // input source
	CompressedDataReader compressedDataReader = null; // compressed data reader if source is compressed
	boolean compress = false; // compress data flag
	String prevZipFolder = null;
	Map<String, byte[]> contents = null; // current ZIP item contents
	CompressedDataItem unusedCompressedDataItem = null;
	boolean assetLoadingFlag = true; // default asset loading strategy is ON
	
	File items[] = null;
	int itemsIndex = 0;
	
	static {
		// context startup
		NDLConfigurationContext.init();
	}
	
	/**
	 * Constructor
	 * @param input input source
	 */
	public AbstractNDLDataIterator(String input) {
		this.input = new File(input);
	}
	
	/**
	 * Constructor
	 * @param input input source
	 */
	public AbstractNDLDataIterator(File input) {
		this.input = input;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void init(DataSourceNULLConfiguration configuration) throws IOException {
		super.init(configuration); // super call
		
		if(!input.exists()) {
			// no exists
			throw new IOException(input.getAbsolutePath() + " does not exist.");
		}
		itemsIndex = 0; // reset
		if(input.isFile()) {
			// supposed to be tar.gz
			compressedDataReader = NDLDataUtils.getCompressedDataReader(input);
			compressedDataReader.init();
		    compress = true;
		} else {
			// directory scanning (SIP directory)
			items = input.listFiles();
		}
		// reset
		unusedCompressedDataItem = null;
		contents = new HashMap<String, byte[]>(4);
		prevZipFolder = null;
	}
	
	/**
	 * Default initialization with null data source configuration
	 * @throws IOException throws error while initializing iterator
	 * @see #init(DataSourceNULLConfiguration)
	 */
	public void init() throws IOException {
		init(null); // null configuration
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isCompressed() {
		return compress;
	}
	
	/**
	 * {@inheritDoc}
	 */
	public void turnOffAssetLoadingFlag() {
		assetLoadingFlag = false;
	}
	
	/**
	 * Gets group identifier for compressed data item, default implementation is item's parent entry 
	 * @param entryName compressed item entry name
	 * @return returns group identifier
	 */
	public String getCompressedItemGroupIdentifier(String entryName) {
		int lastPos = entryName.lastIndexOf("/");
    	return lastPos != -1 ? entryName.substring(0, lastPos) : "";
	}
	
	// load zip items into buffer in case of compressed input source
	void loadZipItems() throws IOException, SAXException {
		CompressedDataItem compressedDataItem = null;
		// take unused if unavailable
		if(unusedCompressedDataItem != null) {
			contents.put(unusedCompressedDataItem.getEntryName(), unusedCompressedDataItem.getContents());
			unusedCompressedDataItem = null; // reset
		}
		// iterate items
	    while ((compressedDataItem = compressedDataReader.next()) != null) {
        	// item
        	String entryName = compressedDataItem.getEntryName();
        	// valid files
        	String folder = getCompressedItemGroupIdentifier(entryName);
        	if(prevZipFolder != null && !prevZipFolder.equals(folder)) {
        		// new item found under same folder
        		// saved as unused compressed data item
        		unusedCompressedDataItem = compressedDataItem;
        		// previous zip folder
            	prevZipFolder = folder;
            	break;
        	} else {
	        	// each sub-item
	        	contents.put(entryName, compressedDataItem.getContents());
        	}
        	// previous zip folder
        	prevZipFolder = folder;
	    }	    
	}
	
	/**
	 * Gets data item from given set of data contents which is SIP/AIP etc. specific 
	 * @param contents set of data contents
	 * @return returns converted data item
	 * @throws IOException throws error I/O related error occurs
	 * @throws SAXException throws error if XML parsing error
	 */
	public final T getItem(Map<String, byte[]> contents) throws IOException, SAXException {
		return getItem(contents, true);
	}
	
	/**
	 * Gets data item from given set of data contents which is SIP/AIP etc. specific 
	 * @param contents set of data contents
	 * @param assetLoadingFlag this flag indicates whether to load asset or not
	 * @return returns converted data item
	 * @throws IOException throws error I/O related error occurs
	 * @throws SAXException throws error if XML parsing error
	 */
	public abstract T getItem(Map<String, byte[]> contents, boolean assetLoadingFlag) throws IOException, SAXException;
	
	/**
	 * Returns item for non compressed version
	 * @return returns item for non compressed version if found, otherwise NULL
	 * @throws IOException throws error I/O related error occurs 
	 * @throws SAXException throws error if XML parsing error
	 */
	public abstract T getNonCompressedNext() throws IOException, SAXException;
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {
		if(compress) {
			compressedDataReader.close();
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasNext() throws IOException, SAXException {
		if(!compress) {
			// ordinary SIP directory
			return itemsIndex < items.length;
		} else {
			// compress tar-gz
			if(contents.isEmpty()) {
				// not loaded yet
				loadZipItems();
			}
			return !contents.isEmpty();
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public T next() throws IOException, SAXException {
		T dataitem = null;
		if(!compress) {
			// flat folder structure
			// get next item
			dataitem = getNonCompressedNext();
		} else {
			// handle compress file
			if(contents.isEmpty()) {
				// not loaded yet
				loadZipItems();
			}
			if(!contents.isEmpty()) {
				// valid index
				dataitem = getItem(contents, assetLoadingFlag);
			} else {
				// no data available
				return null;
			}
			contents = new HashMap<String, byte[]>(4); // reset value
		}
		
		itemsIndex++; // move index ahead
		return dataitem; // data item returned
	}
}