package org.iitkgp.ndl.test;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;

/**
 * Testing of {@link NDLSIPCorrectionContainer}
 * @author Debasis
 */
public class NDLSIPCorrectionContainerTest extends NDLSIPCorrectionContainer {
	
	// constructor
	public NDLSIPCorrectionContainerTest(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	// correction logic
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		// some sample corrections
		
		String id = target.getId();
		if(containsMappingKey("delete." + id)) {
			// to be deleted
			return false;
		}
		
		add("dc.xxx.yyy", "some value"); // add some value to field dc.xxx.yyy
		move("dc.xxx.yy1", "dc.xxx.yy2"); // move dc.xxx.yy1 to dc.xxx.yy2
		deleteIfContains("dc.xxx.yy3", "wrong_value1", "wrong_value2"); // delete values with some filters
		delete("dc.xxx.yy4"); // dc.xxx.yy4 field delete
		// etc.
		
		// success correction
		return true;
	}
	
	// testing
	public static void main(String[] args) throws Exception {
		
		// NDLConfigurationContext.addConfiguration("ndl.service.base.url1", "http://10.72.22.155:65/services/");
		
		String input = "<input source>"; // flat SIP location or compressed SIP location
		String logLocation = "<log location>"; // log location if any
		String outputLocation = "<output location where to write the data>";
		String name = "logical_source_name";
		NDLSIPCorrectionContainerTest p = new NDLSIPCorrectionContainerTest(input, logLocation, outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		
		String deleteFile = "<delete file>"; // which has single column contains handles to delete
		p.addMappingResource(deleteFile, "delete"); // this logical name used to access the handle
		
		p.correctData(); // corrects data
	}
}