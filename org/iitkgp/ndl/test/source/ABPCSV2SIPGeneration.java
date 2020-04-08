package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.RowData;
import org.iitkgp.ndl.data.generation.NDLCSVToSIPGeneration;

// ABP SIP generation
public class ABPCSV2SIPGeneration extends NDLCSVToSIPGeneration {

	public ABPCSV2SIPGeneration(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean generateTargetItem(RowData csv, RowData target) throws Exception {
		
		copy("VolIssue", "dc.identifier.other");
		copy("Publishers", "dc.publisher");
		copy("Editors", "dc.contributor.editor");
		copy("Subject", "dc.subject");
		//copy("File_Path", "dc.identifier.uri");
		copy("Key", "dc.publisher.date");
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/generated_xml_data/ABP/final_mergedFilemod.csv"; // flat CSV location
		String logLocation = "<log location>"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/ABP/output";
		String name = "abp";
		
		ABPCSV2SIPGeneration p = new ABPCSV2SIPGeneration(input, logLocation, outputLocation, name);
		p.deregisterAllNormalizers();
		p.setHandleIDPrefix("123456789_abp");
		p.genrateData();
		
		System.out.println("Done.");
	}
	
}