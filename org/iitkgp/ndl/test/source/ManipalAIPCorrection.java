package org.iitkgp.ndl.test.source;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.correction.NDLAIPCorrectionContainer;

public class ManipalAIPCorrection extends NDLAIPCorrectionContainer {
	
	// constructor
	public ManipalAIPCorrection(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(AIPDataItem target) throws Exception {
		normalize("dc.publisher.date", "dc.date.copyright");
		String tabstract = target.getSingleValue("dc.description.abstract");
		if(StringUtils.isNotBlank(tabstract)) {
			int p = tabstract.indexOf(':');
			if(p != -1) {
				target.updateSingleValue("dc.description.abstract", tabstract.substring(p + 1));
			}
		}
		delete("dc.identifier.other", "dc.description");
		target.moveByRegex("dc.relation", "dc.identifier.other:alternateContentUri", "^http://.*$");
		delete("dc.relation");
		
		transformFieldsById("conf", "<Journal,dc.identifier.other:journal>", "<Volume,dc.identifier.other:volume>",
				"<Issue,dc.identifier.other:issue>", "<Start-Page,dc.format.extent:startingPage>",
				"<End-Page,dc.format.extent:endingPage>", "<Page-Count,dc.format.extent:pageCount>",
				"<ISSN,dc.identifier.issn>", "<EISSN,dc.identifier.other:eissn>", "<ISBN,dc.identifier.isbn>",
				"<Institute,dc.publisher.institution>", "<Place,dc.publisher.place>", "<DOI,dc.identifier.other:doi>");
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/ManipalUniv.tar.gz";
		String logLocation = "<logs>";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/out";
		String name = "manipal";
		
		String confFile = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/conf/2018.Dec.28.16.38.43.manipal.log_csv_1.csv";
		
		ManipalAIPCorrection p = new ManipalAIPCorrection(input, logLocation, outputLocation, name);
		p.addMappingResource(confFile, "ID", "conf");
		p.correctData();
		
		System.out.println("Done.");
	}
}