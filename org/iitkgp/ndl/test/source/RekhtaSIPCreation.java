package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.RowData;
import org.iitkgp.ndl.data.generation.NDLCSVToSIPGeneration;

public class RekhtaSIPCreation extends NDLCSVToSIPGeneration {
	
	String copyFields[] = {
			"dc.publisher.date",
			"dc.language.iso",
			"dc.identifier.uri",
			"lrmi.learningResourceType",
			"dc.description.searchVisibility",
			"dc.source",
			"dc.source.uri",
			"dc.type",
			"dc.contributor.other",
			"dc.publisher",
			"dc.title"
	};
	
	// constructor
	public RekhtaSIPCreation(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	// generation logic
	@Override
	protected boolean generateTargetItem(RowData csv, RowData target) throws Exception {
		
		for(String field : copyFields) {
			copy(field, field);
		}
		
		add("lrmi.educationalUse", "research", "selfLearning");
		add("lrmi.educationalRole", "student", "teacher", "parent");
		
		return true;
	}
	
	// testing
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/generated_xml_data/SIP_TEST/Rekhta.Data.csv";
		String logLocation = "<not required>";
		
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/SIP_TEST";
		String name = "rekhta.sip";
		
		RekhtaSIPCreation p = new RekhtaSIPCreation(input, logLocation, outputLocation, name);
		p.setHandleIDPrefix("123456789_REKHTA");
		p.setPrimaryIDColumn("Id");
		p.genrateData();
		
		System.out.println("Done.");
	}
}