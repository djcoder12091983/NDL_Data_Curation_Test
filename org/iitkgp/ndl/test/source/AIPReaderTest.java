package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.container.NDLAIPDataContainer;

public class AIPReaderTest extends NDLAIPDataContainer {

	public AIPReaderTest(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}
	
	@Override
	protected boolean readItem(AIPDataItem item) throws Exception {
		// TODO
		return true;
	}
	
	// test
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/Tripura_Board-215BKP-28.06.2019.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/logs";
		String name = "aip.reader.test";
		
		AIPReaderTest p = new AIPReaderTest(input, logLocation, name);
		// p.turnOffGlobalLoggingFlag();
		p.processData();
		
		System.out.println("Done.");
	}
}