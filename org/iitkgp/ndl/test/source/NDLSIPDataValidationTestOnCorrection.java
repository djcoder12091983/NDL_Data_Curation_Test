package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;

// data validation test on correction
public class NDLSIPDataValidationTestOnCorrection extends NDLSIPCorrectionContainer {
	
	// constructor
	public NDLSIPDataValidationTestOnCorrection(String input, String logLocation, String outputLocation, String name) {
		// super(input, logLocation, outputLocation, name);
		super(input, logLocation, outputLocation, name, false);
	}
	
	// correction
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		// wrong correction logic
		//target.add("dc.title", "extra_title");
		target.add("dc.xxx.yyy", "wrong_field_value1", "wrong_field_value2");
		target.add("dc.language.iso", "engxx", "frayyy", "arbzzz");
		target.add("dc.contributor.other:xxkey", "wrong_key_value1", "wrong_key_value2", "wrong_key_value3");
		
		target.updateSingleValue("dc.description.searchVisibility", "FALSE");
		
		return true;
	}
	
	// main function
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/SIP_TEST/data_validation_SIP_test.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/SIP_TEST/validation_logs";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/SIP_TEST/out";
		String name = "data.validation.test";
		NDLSIPDataValidationTestOnCorrection p = new NDLSIPDataValidationTestOnCorrection(input, logLocation,
				outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		/*p.turnOffFirstFailOnValidation();
		p.turnOffControlFieldsValidationFlag();
		p.turnOffJsonKeyValidationFlag();
		p.turnOnDetailValidationLogging();*/
		
		p.correctData(); // corrects data
		
		System.out.println("Done.");
		
		//System.out.println(StringUtils.contains("debasis\\/jana", "\\/"));
	}
}