package org.iitkgp.ndl.test;

import org.iitkgp.ndl.data.iterator.FileDataItem;
import org.iitkgp.ndl.data.iterator.FileSystemDataReader;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.junit.Test;

/**
 * Test of {@link FileSystemDataReader}
 * @author Debasis
 */
public class FileSystemDataReaderTest {
	
	String inputLocation = "/data/sample-SIP-data";
	
	// test
	@Test
	public void test() throws Exception {
		FileSystemDataReader reader = new FileSystemDataReader(NDLDataUtils.getResourcePath(inputLocation));
		reader.init();
		
		FileDataItem f = null;
		while((f = reader.next()) != null) {
			System.out.println(f.getEntryName());
		}
		
		reader.close();
	}
}