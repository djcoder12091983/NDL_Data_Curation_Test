package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;

// HAL final correction
public class HALFinalCorrection extends NDLSIPCorrectionContainer {
	
	int c = 0;
	
	public HALFinalCorrection(String input, String logLocation, String outputLocation, String name) {
		// validation off
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		// journal update
		c += transformFieldsById("jtitle", "<Title,dc.identifier.other:journal>");
		target.replaceByRegex("dc.identifier.other:journal", "@", "");
		removeMultipleSpaces("dc.identifier.other:journal");
		
		return true;
	}
	
	@Override
	protected void intermediateProcessHandler() {
		System.out.println("Total items affected: " + c);
	}
	
	public static void main(String[] args) throws Exception {
		
		// flat SIP location or compressed SIP location
		String input = "/home/dspace/debasis/NDL/NDL_sources/HAL/in/HAL-V2-19.09.2019.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/HAL/out"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/HAL/out";
		String name = "hal.final";
		
		String jfile = "/home/dspace/debasis/NDL/NDL_sources/HAL/in/jtitles";
		
		HALFinalCorrection p = new HALFinalCorrection(input, logLocation, outputLocation, name);
		p.addMappingResource(jfile, "ID", "jtitle");
		p.turnOnManualMultipleSpacesAndLinesRemoval();
		p.correctData();
		
		 System.out.println("Done.");
	}

}