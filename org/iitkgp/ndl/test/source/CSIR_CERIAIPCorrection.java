package org.iitkgp.ndl.test.source;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.correction.NDLAIPCorrectionContainer;

public class CSIR_CERIAIPCorrection extends NDLAIPCorrectionContainer {
	
	// constructor
	public CSIR_CERIAIPCorrection(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(AIPDataItem target) throws Exception {
		
		normalize("dc.publisher.date");
		String cp = target.getSingleValue("dc.date.copyright");
		if(StringUtils.isNotBlank(cp)) {
			String tokens[] = cp.split("-");
			target.updateSingleValue("dc.date.copyright", tokens[tokens.length - 1]);
		}
		
		target.move("dc.relation", "dc.identifier.other:journal");
		
		removeMultipleSpaces("dc.publisher", "dc.title", "dc.description.abstract");
		target.replace("dc.rights.accessRights", "authorised", "authorized");
	
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/2018.Nov.26.15.36.50.CSIR-CERI.tar.gz";
		String logLocation = "<logs>";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/out_done";
		String name = "csir.ceri";
		
		CSIR_CERIAIPCorrection p = new CSIR_CERIAIPCorrection(input, logLocation, outputLocation, name);
		p.correctData();
		
		System.out.println("Done.");
	}

}