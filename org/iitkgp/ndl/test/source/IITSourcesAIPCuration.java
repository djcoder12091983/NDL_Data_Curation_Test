package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.correction.NDLAIPCorrectionContainer;

public class IITSourcesAIPCuration extends NDLAIPCorrectionContainer {
	
	// constructor
	public IITSourcesAIPCuration(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	// correct
	@Override
	protected boolean correctTargetItem(AIPDataItem target) throws Exception {
		
		String id = getCurrentItemSuffixId();
		/*String collection = getCurrentCollectionSuffixId();
		String key = "ddc." + collection;
		if(containsMappingKey(key)) {
			add("dc.subject.ddc", getMappingKey(key), ';');
		}*/
		String key = "conf." + id + ".Visibility";
		if(containsMappingKey(key)) {
			target.add("dc.description.searchVisibility", getMappingKey(key).toLowerCase());
		}
		key = "conf." + id + ".Rights";
		if(containsMappingKey(key)) {
			target.add("dc.rights.accessRights", getMappingKey(key));
		}
		
		//target.updateSingleValue("dc.publisher", "IIT Kharagpur");
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/samim_source/IIT-Hyderabad.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/samim_source/logs";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/samim_source/out";
		String name = "iithyd";
		
		//String ddcFile = "/home/dspace/debasis/NDL/generated_xml_data/samim_source/conf/IIT-Kharagpur.ddc.csv";
		String confFile = "/home/dspace/debasis/NDL/generated_xml_data/samim_source/conf/IIT.HYDERABAD.conf.csv";
		
		IITSourcesAIPCuration t = new IITSourcesAIPCuration(input, logLocation, outputLocation, name);
		//t.addMappingResource(ddcFile, "ID", "ddc");
		t.addMappingResource(confFile, "ID", "conf");
		t.correctData();
		
		System.out.println("Done.");
	}
}