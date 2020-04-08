package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.context.NDLConfigurationContext;

public class MainProgramTest {
	
	// runs program from this function
	public static void main(String[] args) throws Exception {
		
		NDLConfigurationContext.addConfiguration("ndl.service.base.url","http://dataentry.ndl.gov.in/services/");
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/SIP_TEST/CSV_2_SIP/csv_1"; // flat CSV location
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/SIP_TEST/CSV_2_SIP/logs"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/SIP_TEST/CSV_2_SIP/out";
		String name = "csv2sip.test";
		// multiple value separator is pipe
		JSTORCSV2SIPGenerationTest p = new JSTORCSV2SIPGenerationTest(input, logLocation, outputLocation, name);
		p.turnOnFullHandleIDColumnFlag("Handle_ID");
		//p.setMultipleValueSeparator(';');
		p.genrateData(); // generates data
		
		System.out.println("Done.");
	}

}