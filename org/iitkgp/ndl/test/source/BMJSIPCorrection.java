package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;

public class BMJSIPCorrection extends NDLSIPCorrectionContainer {

	public BMJSIPCorrection(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}

	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		if(target.getId().equals("bmj/1_5279_709_2")) {
			System.err.println("Before: " + target.getSingleValue("dc.title"));
		}
		
		removeMultipleSpaces("dc.title");
		
		if(target.getId().equals("bmj/1_5279_709_2")) {
			System.err.println("After: " + target.getSingleValue("dc.title"));
		}
		
		return true;
	}
	
	// testing
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/BMJ/in/2019.Feb.18.11.33.48.BMJ.V3.Corrected.tar.gz"; // flat SIP location or compressed SIP location
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/BMJ/logs"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/BMJ/out";
		String name = "bmj.test";
		
		BMJSIPCorrection p = new BMJSIPCorrection(input, logLocation, outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		p.correctData();
		
		System.out.println("Done.");
	}
	
}