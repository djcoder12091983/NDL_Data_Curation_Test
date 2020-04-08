package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.correction.NDLAIPCorrectionContainer;

// AIP correction test
public class AIPCorrectionTest extends NDLAIPCorrectionContainer {
	
	//Set<String> values = null;
	
	// constructor
	public AIPCorrectionTest(String input, String logLocation, String outputLocation, String name) {
		// validation off
		super(input, logLocation, outputLocation, name, false);
	}
	
	@Override
	protected boolean correctTargetItem(AIPDataItem target) throws Exception {
		// target.move("dc.creator", "dc.contributor.author");
		/*String id = getCurrentItemSuffixId();
		String key = "conf." + id;
		if(containsMappingKey(key)) {
			String title = NDLDataUtils.removeNewLines(getMappingKey(key + ".title"));
			String desc = NDLDataUtils.removeNewLines(getMappingKey(key + ".desc"));
			
			target.updateSingleValue("dc.title", title);
			target.updateSingleValue("dc.description", desc);
		}*/
		
		/*transformFieldsById("conf", ';', "<ddc,dc.subject.ddc>", "<Visibility,dc.description.searchVisibility>",
				"<Rights,dc.rights.accessRights>");*/
		
		//add("dc.subject.ddc", "20");
		/*String collection = target.getParentId();
		String key = "ddc." + NDLDataUtils.getHandleSuffixID(collection);
		if(containsMappingKey(key)) {
			add("dc.subject.ddc", getMappingKey(key), ';');
		}*/
		/*removeMultipleSpaces("dc.description");
		normalize("dc.publisher.date");
		target.replaceByRegex("dc.identifier.other:doi", "http:\\/\\/dx\\.doi\\.org\\/", "");
		move("dc.identifier.other:otherLink", "dc.identifier.other:alternateContentUri");*/
		/*removeHTMLTags("dc.description");
		removeMultipleSpaces("dc.description");
		removeMultipleLines("dc.description");*/
		
		//moveIfContains("", "", "", "");
		
		System.out.println(target.getSingleValue("dc.format.extent:startingPage"));
		System.out.println(target.getSingleValue("dc.format.extent:endingPage"));
		System.out.println(target.getSingleValue("dc.format.extent:pageCount"));
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/SIP_TEST/Sarvodaya_11march_2019.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/SIP_TEST/logs"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/SIP_TEST/out";
		String name = "aip.error.test";
		
		//String confFile = "/home/dspace/debasis/NDL/generated_xml_data/AIP_TEST/IIT_Bomby/IIT_Bombay";
		//String ddcFile = "/home/dspace/debasis/NDL/generated_xml_data/AIP_TEST/IIT_Bomby/ddc.csv";
		
		AIPCorrectionTest t = new AIPCorrectionTest(input, logLocation, outputLocation, name);
		//t.addMappingResource(confFile, "ID", "conf");
		//t.addMappingResource(ddcFile, "ID", "ddc");
		//t.values = NDLDataUtils.loadSet(file);
		//t.turnOnAccessHierarchyFlag();
		t.turnOffLoadHierarchyFlag();
		t.correctData();
		
		System.out.println("Done.");
	}
}