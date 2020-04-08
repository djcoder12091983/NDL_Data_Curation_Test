package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;

public class ACMDataCuration extends NDLSIPCorrectionContainer {
	
	// constructor
	public ACMDataCuration(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		String id = target.getId();
		id = id.substring(id.indexOf('/') + 1);
		
		if(containsMappingKey("delete." + id)) {
			return false;
		}
		
		target.setId(target.getId().replaceAll("proc", "nl")); // ID change
		
		/*if(target.getId().contains("proc")) {
			System.out.println();
		}*/
		
		return true;
	}
	
	// correct
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/generated_xml_data/SIP_TEST/2018.Aug.28.12.36.34.sip.test/2018.Aug.28.12.36.34.sip.test.Corrected.tar.gz";
		String logLocation = "<not required>";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/SIP_TEST/";
		String name = "sip.test";
		
		String deleteFile = "/home/dspace/debasis/NDL/generated_xml_data/SIP_TEST/handle.txt";
		
		ACMDataCuration p = new ACMDataCuration(input, logLocation, outputLocation, name);
		p.addMappingResource(deleteFile, "delete");
		p.correctData();
		
		System.out.println("Done.");
	}
	
}