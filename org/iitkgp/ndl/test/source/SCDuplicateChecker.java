package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.duplicate.checker.NDLDuplicateDataChecker;

public class SCDuplicateChecker extends NDLDuplicateDataChecker {

	public SCDuplicateChecker(String input, String logLocation, String duplicateField) {
		super(input, logLocation, duplicateField, 3); // 3 threads
	}
	
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/csv_data/sc_csv/doiList.csv";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/duplicate_logs/sc_duplicates";
		String duplicateField = "doi";
		
		SCDuplicateChecker dc = new SCDuplicateChecker(input, logLocation, duplicateField);
		dc.setColumnIndex(1); // 2nd column to fetch DOI
		dc.setDataSplitSize(75000);
		dc.check();
		
		System.out.println("Done.");
	}
}