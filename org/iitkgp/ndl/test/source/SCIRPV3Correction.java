package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;

public class SCIRPV3Correction extends NDLSIPCorrectionContainer {
	
	// construction
	public SCIRPV3Correction(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		target.replaceByRegex("ndl.sourceMeta.additionalInfo:references", "\\r\\n", " ");
		target.move("ndl.sourceMeta.additionalInfo:relatedContentUrl", "dc.rights.uri");
		
		removeMultipleSpaces("ndl.sourceMeta.additionalInfo:references");
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/generated_xml_data/SCRIP/in/SCIRP-08.11.2018.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/SCRIP/logs";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/SCRIP/out";
		String name = "SCIRP.V3";
		
		SCIRPV3Correction p = new SCIRPV3Correction(input, logLocation, outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		p.correctData();
		
		System.out.println("Done.");
		
	}

}