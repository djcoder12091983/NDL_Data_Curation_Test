package org.iitkgp.ndl.test;

import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.correction.NDLAIPCorrectionContainer;

/**
 * Testing of {@link NDLAIPCorrectionContainer}
 * @author Debasis
 */
public class NDLAIPCorrectionContainerTest extends NDLAIPCorrectionContainer {
	
	// constructor
	public NDLAIPCorrectionContainerTest(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	// correction logic
	@Override
	protected boolean correctTargetItem(AIPDataItem target) throws Exception {
		// some sample corrections
		add("dc.xxx.yyy", "<some value>"); // add some value to field dc.xxx.yyy
		move("dc.xxx.yy1", "dc.xxx.yy2"); // move dc.xxx.yy1 to dc.xxx.yy2
		deleteIfContains("dc.xxx.yy3", "wrong_value1", "wrong_value2"); // delete values with some filters
		delete("dc.xxx.yy4"); // dc.xxx.yy4 field delete
		// etc.
		
		// success correction
		return true;
	}
	
	// testing
	public static void main(String[] args) throws Exception {
		String input = "<input source>"; // flat AIP location or compressed AIP location
		String logLocation = "<log location>"; // log location if any
		String outputLocation = "<output location where to write the data>";
		String name = "logical source name";
		NDLAIPCorrectionContainerTest p = new NDLAIPCorrectionContainerTest(input, logLocation, outputLocation, name);
		
		p.correctData(); // corrects data
	}
}