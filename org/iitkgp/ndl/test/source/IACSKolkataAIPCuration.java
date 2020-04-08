package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.correction.NDLAIPCorrectionContainer;

public class IACSKolkataAIPCuration extends NDLAIPCorrectionContainer {

	public IACSKolkataAIPCuration(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(AIPDataItem target) throws Exception {
		delete("dc.relation");
		delete("dc.identifier.other");
		transformFieldsById("conf", "<Journal,dc.identifier.other:journal>", "<Volume,dc.identifier.other:volume>",
				"<Issue,dc.identifier.other:issue>", "<Start-Page,dc.format.extent:startingPage>",
				"<End-Page,dc.format.extent:endingPage>", "<Page-Count,dc.format.extent:pageCount>",
				"<Date,dc.date.issued>", "<DOI,dc.identifier.other:doi>");
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/2018.Nov.13.15.00.05.IACS KOLKATA.tar.gz";
		String logLocation = "<logs>";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/out";
		String name = "iacs.kol";
		
		String confFile = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/logs/2018.Dec.26.15.04.57.iacs.kolkata.iacs_csv_1.csv";
		
		IACSKolkataAIPCuration p = new IACSKolkataAIPCuration(input, logLocation, outputLocation, name);
		p.addMappingResource(confFile, "ID", "conf");
		p.correctData();
		
		System.out.println("Done.");
		
	}

}