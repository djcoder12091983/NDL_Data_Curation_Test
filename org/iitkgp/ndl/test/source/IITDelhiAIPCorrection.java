package org.iitkgp.ndl.test.source;

import java.util.regex.Pattern;

import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.correction.NDLAIPCorrectionContainer;

public class IITDelhiAIPCorrection extends NDLAIPCorrectionContainer {
	
	static Pattern PATTERN1 = Pattern.compile("^[0-9]+(\\.|-|:|/|\\(|( [A-Z]+))");
	static Pattern PATTERN2 = Pattern.compile("(.*)( *)?\\((.*)\\)");
	static Pattern PATTERN3 = Pattern.compile(".*(-|–).*");

	public IITDelhiAIPCorrection(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(AIPDataItem target) throws Exception {
		// correction logic
		target.updateSingleValue("dc.source.uri", "http://www.iitd.ac.in");
		removeMultipleSpaces("dc.description", "dc.description.abstract");
		target.replaceByRegex("dc.relation", ";$", "");
		
		delete("dc.identifier.other", "dc.relation");
		transformFieldsById("conf", "<Journal,dc.identifier.other:journal>", "<Desc,dc.description>",
				"<Volume,dc.identifier.other:volume>", "<Issue,dc.identifier.other:issue>",
				"<Start-Page,dc.format.extent:startingPage>", "<End-Page,dc.format.extent:endingPage>",
				"<Page-Count,dc.format.extent:pageCount>", "<AccessionNo,dc.identifier.other:accessionNo>",
				"<UniqueID,dc.identifier.other:uniqueId>", "<Item-ID,dc.identifier.other:itemId>");
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/2018.Oct.30.19.35.24.iit_Delhi.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/logs";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/out";
		String name = "iitd";
		
		String confFile = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/conf/2018.Dec.26.16.03.42.iitd.csv_log_1.csv";
		
		IITDelhiAIPCorrection p = new IITDelhiAIPCorrection(input, logLocation, outputLocation, name);
		// p.addTextLogger("extraction.log");
		p.addMappingResource(confFile, "ID",  "conf");
		p.correctData();
		
		System.out.println("Done.");
	}

}