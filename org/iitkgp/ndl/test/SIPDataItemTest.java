package org.iitkgp.ndl.test;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.junit.Test;
import org.xml.sax.SAXException;

/**
 * Test cases of {@link SIPDataItem}
 * @author Aurghya, Debasis
 */
public class SIPDataItemTest {
	
	String sipItem = "/data/sample-SIP-data/14108347";
	
	// loads SIP item
	SIPDataItem load(String sipItem) throws IOException, SAXException {
		SIPDataItem item = new SIPDataItem();
		Map<String, byte[]> contents = new HashMap<String, byte[]>(4);
		File[] files = new File(NDLDataUtils.getResourcePath(sipItem)).listFiles();
		for(File file : files) {
			contents.put(file.getName(), IOUtils.toByteArray(new FileInputStream(file)));
		}
		item.load(contents);
		return item;
	}
	
	/**
	 * Tests handle ID
	 * @throws Exception throws error if data loading error occurs
	 */
	@Test
	public void getIDTest() throws Exception {
		SIPDataItem item = load(sipItem);
		assertEquals("1234567_who/14108347", item.getId());
	}
}