package org.iitkgp.ndl.data;

import java.io.IOException;
import java.util.Map;

import javax.xml.transform.TransformerException;

/**
 * Indicates an item is writable
 * @author Debasis
 */
public interface DataItemWritable {

	/**
	 * Gets contents for current SIP/AIP data item
	 * @return returns contents for current item
	 * @throws IOException throws error while data extraction error occurs
	 * @throws TransformerException throws error when XML to byte contents conversion error occurs
	 */
	Map<String, byte[]> getContents() throws IOException, TransformerException;
}