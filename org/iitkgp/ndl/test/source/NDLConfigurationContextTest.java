package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.util.NDLServiceUtils;

// NDLConfigurationContext testing
public class NDLConfigurationContextTest extends NDLSIPCorrectionContainer {
	
	// constructor
	public NDLConfigurationContextTest(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		// getting normalized data from service
		System.out.println(NDLServiceUtils.normalilzeLanguage("en"));
		
		return true;
	}
	
	// testing
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/SIP_TEST/ACM-Newsletter-SIP-BKP.tar.gz";
		String logLocation = "<not required>";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/SIP_TEST";
		String name = "sip.test";
		
		NDLConfigurationContext.addConfiguration("ndl.service.base.url", "http://dataentry.ndl.iitkgp.ac.in/services/");
		
		NDLConfigurationContextTest t = new NDLConfigurationContextTest(input, logLocation, outputLocation, name);
		t.correctData();
		
		System.out.println("Done.");
	}
}