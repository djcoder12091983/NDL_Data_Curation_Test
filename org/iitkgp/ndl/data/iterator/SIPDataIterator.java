package org.iitkgp.ndl.data.iterator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.compress.CompressedFileMode;
import org.xml.sax.SAXException;

/**
 * SIP data iterator
 * @see DataIterator
 * @see CompressedFileMode
 * @author Debasis
 */
public class SIPDataIterator extends AbstractNDLDataIterator<SIPDataItem> {
	
	/**
	 * Constructor
	 * @param input input source
	 */
	public SIPDataIterator(String input) {
		super(input);
	}
	
	/**
	 * Constructor
	 * @param input input source
	 */
	public SIPDataIterator(File input) {
		super(input);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public SIPDataItem getItem(Map<String, byte[]> contents, boolean assetLoadingFlag) throws IOException, SAXException {
		SIPDataItem dataitem = new SIPDataItem();
		dataitem.load(contents, assetLoadingFlag);
		return dataitem;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public SIPDataItem getNonCompressedNext() throws IOException, SAXException {
		// get next item
		if(itemsIndex < items.length) {
			// valid index
			File parentFile = items[itemsIndex];
			if(parentFile.isFile()) {
				// error
				throw new IOException(parentFile.getAbsolutePath() + " is a file, but exptected a folder.");
			}
			File subitems[] = parentFile.listFiles();
			Map<String, byte[]> contents = new HashMap<String, byte[]>(4); // at least 4 items
			// contents mapping
			for(File subitem : subitems) {
				// contents
				contents.put(parentFile.getName() + "/" + subitem.getName(),
						IOUtils.toByteArray(new FileInputStream(subitem)));
			}
			return getItem(contents); // item conversion
		} else {
			// next not possible
			return null;
		}
	}

}