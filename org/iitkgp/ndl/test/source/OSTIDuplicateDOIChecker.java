package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.duplicate.checker.NDLDuplicateDataChecker;

// OSTI duplicate DOI checking
public class OSTIDuplicateDOIChecker extends NDLDuplicateDataChecker {

	public OSTIDuplicateDOIChecker(String input, String logLocation, String duplicateField) {
		super(input, logLocation, duplicateField);
	}
	
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/OSTI_test/DOI_logs";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/OSTI_test/duplicate_logs";
		String duplicateField = "doi";
		
		OSTIDuplicateDOIChecker dc = new OSTIDuplicateDOIChecker(input, logLocation, duplicateField);
		dc.setColumnIndex(1); // 2nd column to fetch DOI
		dc.setDataSplitSize(100000);
		dc.check();
		
		System.out.println("Done.");
	}

}