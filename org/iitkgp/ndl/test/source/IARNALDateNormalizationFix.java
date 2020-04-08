package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;

public class IARNALDateNormalizationFix extends NDLSIPCorrectionContainer {
	
	// constructor
	public IARNALDateNormalizationFix(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		normalize("dc.publisher.date");
		return true;
	}
	
	// testing
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/IAR/raw_data/NAL/2018.Aug.07.15.24.03.NAL.v5.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/IAR/raw_data/NAL/logs";
		String outputLocation = "/home/dspace/debasis/NDL/IAR/raw_data/NAL/out";
		String name = "NAL.v5_test";
		
		IARNALDateNormalizationFix p = new IARNALDateNormalizationFix(input, logLocation, outputLocation, name);
		p.correctData();
		
		System.out.println("Done.");
	}

}