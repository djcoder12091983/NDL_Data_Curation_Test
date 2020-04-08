package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.RowData;
import org.iitkgp.ndl.data.generation.NDLCSVToSIPGeneration;

// CSV 2 SIP
public class JSTORCSV2SIPGenerationTest extends NDLCSVToSIPGeneration {
	
	// constructor
	public JSTORCSV2SIPGenerationTest(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name, false);
	}
	
	// generation logic
	@Override
	protected boolean generateTargetItem(RowData csv, RowData target) throws Exception {
		
		// copy title
		copy("dc.contributor.author", "dc.contributor.author");
		copy("dc.contributor.advisor", "dc.contributor.advisor");
		copy("dc.language.iso", "dc.language.iso");
		copy("dc.description.abstract", "dc.description.abstract");
		copy("dc.description.tableofcontents", "dc.description.tableofcontents");
		copy("dc.identifier.uri", "dc.identifier.uri");
		copy("dc.format.extent", "dc.format.extent");
		copy("dc.format.mimetype", "dc.format.mimetype");
		copy("dc.type.degree", "dc.type.degree");
		copy("dc.type", "dc.type");
		copy("dc.title", "dc.title");
		copy("dc.date.awarded", "dc.date.awarded");
		copy("dc.rights.holder", "dc.rights.holder");
		copy("dc.publisher", "dc.publisher");
		copy("dc.publisher.institution", "dc.publisher.institution");
		copy("dc.publisher.department", "dc.publisher.department");
		copy("dc.publisher.place", "dc.publisher.place");
		copy("lrmi.educationalUse", "lrmi.educationalUse");
		copy("lrmi.typicalAgeRange", "lrmi.typicalAgeRange");
		copy("lrmi.educationalRole", "lrmi.educationalRole");
		copy("lrmi.learningResourceType", "lrmi.learningResourceType");
		copy("lrmi.educationalAlignment.educationalLevel", "lrmi.educationalAlignment.educationalLevel");
		copy("lrmi.educationalAlignment.difficultyLevel", "lrmi.educationalAlignment.difficultyLevel");
		copy("lrmi.interactivityType", "lrmi.interactivityType");
		copy("lrmi.educationalAlignment.educationalFramework", "lrmi.educationalAlignment.educationalFramework");
		copy("lrmi.educationalAlignment.pedagogicObjective", "lrmi.educationalAlignment.pedagogicObjective");

		return true;
	}
}