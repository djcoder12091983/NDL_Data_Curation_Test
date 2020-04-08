package org.iitkgp.ndl.test.source;

import java.util.List;

import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.correction.NDLAIPCorrectionContainer;

public class DOAJAIPCorrection extends NDLAIPCorrectionContainer {
	
	// constructor
	public DOAJAIPCorrection(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(AIPDataItem target) throws Exception {
		transformFieldByExactMatch("dc.rights", "rights");
		move("dc.rights", "dc.rights.license");
		
		List<String> others = getValue("dc.identifier.other");
		delete("dc.identifier.other");
		add("dc.identifier.issn", others.get(0));
		if(others.size() > 1) {
			add("dc.identifier.other:eissn", others.get(1));
		}
		move("dc.relation", "dc.identifier.other:alternateContentUri");
		
		transformFieldsById("conf", "<rights,dc.rights.accessRights>", "<visibility,dc.description.searchVisibility>");
		String key = "ddc." + getCurrentCollectionSuffixId();
		if(containsMappingKey(key)) {
			add("dc.subject.ddc", getMappingKey(key), ';');
		}
		
		List<NDLDataNode> subjects = target.getNodes("dc.subject");
		for(NDLDataNode subject : subjects) {
			String val = subject.getTextContent();
			if(val.startsWith("LCC:")) {
				subject.remove();
				target.add("dc.subject.other:lcc", val.substring(5));
			}
		}
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/DirOpenAccJournal.tar.gz";
		String logLocation = "<logs>";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/out_done";
		String name = "doaj";
		
		String rightsFile = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/conf/doaj.rights.csv";
		String confFile = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/conf/DirOpenAccJournal.csv";
		String ddcFile = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/conf/DirOpenAccJournal_csv.collectionsDDC.csv";
		
		DOAJAIPCorrection t = new DOAJAIPCorrection(input, logLocation, outputLocation, name);
		t.addMappingResource(rightsFile, "Old", "rights");
		t.addMappingResource(confFile, "ID",  "conf");
		t.addMappingResource(ddcFile, "ID",  "ddc");
		t.correctData();
		
		System.out.println("Done.");
	}
}
