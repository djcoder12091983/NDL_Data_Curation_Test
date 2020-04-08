package org.iitkgp.ndl.test;

import static org.junit.Assert.assertEquals;

import org.iitkgp.ndl.data.iterator.DataSourceNULLConfiguration;
import org.iitkgp.ndl.data.iterator.SIPDataIterator;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.junit.Test;

/**
 * Test cases of {@link SIPDataIterator}
 * @author Vishal, Debasis
 */
public class SIPDataIteratorTest {
	String input = "/data/sample-SIP-data";
	String compressedInput = "/data/sample-SIP-data.tar.gz";
	
	// common test cases
	void commonTest(String input) throws Exception {
		SIPDataIterator i = new SIPDataIterator(NDLDataUtils.getResourcePath(input));
		i.init(new DataSourceNULLConfiguration()); // initialization
		long c = 0;
		while(i.hasNext()) {
			i.next();
			c++;
		}
		i.close(); // close
		assertEquals(1261, c);
	}
	
	/**
	 * test
	 * @throws Exception throws exception if iterator loading error occurs
	 */
	@Test
	public void folderIteratorTest() throws Exception {
		commonTest(input);
		commonTest(compressedInput);
	}
}