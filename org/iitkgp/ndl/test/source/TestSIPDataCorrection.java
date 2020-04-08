package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;

public class TestSIPDataCorrection extends NDLSIPCorrectionContainer {
	
	// constructor
	public TestSIPDataCorrection(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	// correction
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		normalize("dc.subject.ddc"); // DDC normalization
		return true;
	}
	
	// test
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/TEST/aman_sip.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/TEST/";
		String outputLocation = "/home/dspace/debasis/NDL/TEST/test_out";
		String name = "test.data";
		
		TestSIPDataCorrection p = new TestSIPDataCorrection(input, logLocation, outputLocation, name);
		p.correctData();
		
		System.out.println("Done.");
	}

}