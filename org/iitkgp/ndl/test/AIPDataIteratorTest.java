package org.iitkgp.ndl.test;

import static org.junit.Assert.assertEquals;

import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.iterator.AIPDataIterator;
import org.iitkgp.ndl.data.iterator.DataSourceNULLConfiguration;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.junit.Test;

/**
 * Test cases of {@link AIPDataIterator}
 * @author Vishal, Debasis
 */
public class AIPDataIteratorTest {
	
	String input = "/data/sample-AIP-data";
	String compressedInput = "/data/sample-AIP-data.tar.gz";
	
	// common test cases
	void commmonTest(String input) throws Exception {
		AIPDataIterator i = new AIPDataIterator(NDLDataUtils.getResourcePath(input));
		i.init(new DataSourceNULLConfiguration()); // initialization
		long c = 0;
		long itemCount = 0, collectionCount = 0;
		while(i.hasNext()) {
			AIPDataItem item = i.next();
			if(item.isItem()) {
				// item
				itemCount++;
			} else if(item.isCollection()) {
				collectionCount++;
			}
			c++;
		}
		i.close(); // close
		assertEquals(2456, c);
		assertEquals(2428, itemCount);
		assertEquals(27, collectionCount);
	}
	
	/**
	 * test
	 * @throws Exception throws exception if iterator loading error occurs
	 */
	@Test
	public void folderIteratorTest() throws Exception {
		commmonTest(input);
		commmonTest(compressedInput);
	}
}