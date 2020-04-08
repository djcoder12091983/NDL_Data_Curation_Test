package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;

public class WHOMiscSIPCorrection extends NDLSIPCorrectionContainer {
	
	// constructor
	public WHOMiscSIPCorrection(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	// correct
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		target.replace("dc.description.searchVisibility", "false", "true");
		if(!target.exists("lrmi.learningResourceType")) {
			target.add("lrmi.learningResourceType", "monograph");
		}
		
		return true;
	}
	
	// test
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/WHO-Misc-201Bkp-23.08.2018.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/logs";
		String outputLocation = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others";
		String name = "WHO.Misc";
		
		WHOMiscSIPCorrection p = new WHOMiscSIPCorrection(input, logLocation, outputLocation, name);
		p.correctData();
		
		System.out.println("Done.");
	}

}