package org.iitkgp.ndl.test.source;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.correction.NDLAIPCorrectionContainer;

public class MysoreAIPCorrection extends NDLAIPCorrectionContainer {

	public MysoreAIPCorrection(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(AIPDataItem target) throws Exception {
		// correction logic
		normalize("dc.publisher.date");
		String copyright = target.getSingleValue("dc.date.copyright");
		if(StringUtils.isNotBlank(copyright)) {
			String t[] = copyright.split("-");
			for(String t1 : t) {
				if(t1.length() == 4) {
					target.updateSingleValue("dc.date.copyright", t1);
					break;
				}
			}
		}
		
		delete("dc.identifier.other", "dc.relation", "dc.description");
		transformFieldsById("conf", "<Journal,dc.identifier.other:journal>", "<Volume,dc.identifier.other:volume>",
				"<Issue,dc.identifier.other:issue>", "<Start-Page,dc.format.extent:startingPage>",
				"<End-Page,dc.format.extent:endingPage>", "<Page-Count,dc.format.extent:pageCount>",
				"<ISSN,dc.identifier.issn>", "<ISBN,dc.identifier.other:eisbn>",
				"<AlternateUri,dc.identifier.other:alternateContentUri>", "<DOI,dc.identifier.other:doi>");
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/2018.Nov.26.15.23.56.University_Of_Mysore.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/logs";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/out";
		String name = "mysore";
		
		String confFile = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/conf/2018.Dec.27.18.22.35.mysore.log_csv_1.csv";
		
		MysoreAIPCorrection p = new MysoreAIPCorrection(input, logLocation, outputLocation, name);
		p.addMappingResource(confFile, "ID",  "conf");
		p.correctData();
		
		System.out.println("Done.");
	}

}