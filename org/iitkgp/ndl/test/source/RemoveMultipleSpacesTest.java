package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;

public class RemoveMultipleSpacesTest extends NDLSIPCorrectionContainer {

	// constructor
	public RemoveMultipleSpacesTest(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name, false);
	}
	
	// correction logic
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		String id = target.getId();
		if(id.equals("projecteuclid/1019160496")) {
			System.err.println("Before : " + target.getValue("dc.description"));
			removeMultipleSpaces("dc.description");
			removeMultipleLines("dc.description");
			System.err.println("After: " + target.getValue("dc.description"));
		}
		
		return true;
	}
	
	// testing
	public static void main(String[] args) throws Exception {
		// flat SIP location or compressed SIP location
		String input = "/home/dspace/debasis/NDL/NDL_sources/euclid/2019.Apr.09.10.18.14.Euclid.V1.Corrected.tar.gz";
		String logLocation = "<log_location>"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/euclid/out";
		String name = "remove.mul.spaces.test";
		
		RemoveMultipleSpacesTest p = new RemoveMultipleSpacesTest(input, logLocation, outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		p.processData();
		
		System.out.println("Done.");
	}
}