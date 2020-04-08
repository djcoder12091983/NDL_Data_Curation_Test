package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;

public class NDLLongDataReadWriteTest extends NDLSIPCorrectionContainer {
	
	// construction
	public NDLLongDataReadWriteTest(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		// testing, no transformations
		return true;
	}
	
	// correct
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/generated_xml_data/SIP_TEST/pbslearningmedia-08.10.2018.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/SIP_TEST/logs";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/SIP_TEST/out";
		String name = "STP.long.test";
		
		NDLLongDataReadWriteTest p = new NDLLongDataReadWriteTest(input, logLocation, outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		p.correctData();
		
		System.out.println("Done.");
	}

}