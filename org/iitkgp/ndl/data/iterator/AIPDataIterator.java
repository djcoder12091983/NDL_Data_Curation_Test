package org.iitkgp.ndl.data.iterator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.compress.CompressedFileMode;
import org.xml.sax.SAXException;

/**
 * AIP data iterator
 * @see DataIterator
 * @see CompressedFileMode
 * @author Debasis
 */
public class AIPDataIterator extends AbstractNDLDataIterator<AIPDataItem> {
	
	/**
	 * Constructor
	 * @param input input source
	 */
	public AIPDataIterator(String input) {
		super(input);
	}
	
	/**
	 * Constructor
	 * @param input input source
	 */
	public AIPDataIterator(File input) {
		super(input);
	}
	
	/**
	 * For AIP item each entry identifies item
	 */
	@Override
	public String getCompressedItemGroupIdentifier(String entryName) {
		return entryName.substring(entryName.lastIndexOf('/')+1); // item name
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public AIPDataItem getItem(Map<String, byte[]> contents, boolean assetLoadingFlag) throws IOException, SAXException {
		AIPDataItem dataitem = new AIPDataItem();
		dataitem.load(contents, assetLoadingFlag);
		return dataitem;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public AIPDataItem getNonCompressedNext() throws IOException, SAXException {
		// get next item
		if(itemsIndex < items.length) {
			// valid index
			File file = items[itemsIndex];
			Map<String, byte[]> contents = new HashMap<String, byte[]>(2);
			contents.put(file.getName(), IOUtils.toByteArray(new FileInputStream(file))); // only one item
			return getItem(contents); // item conversion
		} else {
			return null;
		}
	}
}