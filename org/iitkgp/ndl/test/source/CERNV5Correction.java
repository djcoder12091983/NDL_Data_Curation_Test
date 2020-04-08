package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class CERNV5Correction extends NDLSIPCorrectionContainer {
	
	// construction
	public CERNV5Correction(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	// correct
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		// correction logic
		String id = NDLDataUtils.getHandleSuffixID(target.getId());
		
		removeMultipleLines("dc.title", "dc.description.abstract", "dc.title.alternative", "dc.subject",
				"dc.description");
		deleteIfContains("dc.subject", ",");
		target.deleteDuplicateFieldValues("dc.publisher.date", "dc.date.created");
		target.deleteDuplicateFieldValues("dc.title", "dc.description.abstract");
		
		
		return true;
	}
	
	// correct
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/generated_xml_data/CERN/in/2018.Nov.28.17.17.27.CERN.V4.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/CERN/logs";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/CERN/out";
		String name = "CERN.V5";
		
		CERNV5Correction p = new CERNV5Correction(input, logLocation, outputLocation, name);
		p.correctData();
		
		System.out.println("Done.");
	}

}