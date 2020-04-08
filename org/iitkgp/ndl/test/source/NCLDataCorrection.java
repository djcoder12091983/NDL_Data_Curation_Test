package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.correction.NDLAIPCorrectionContainer;

public class NCLDataCorrection extends NDLAIPCorrectionContainer {

	public NCLDataCorrection(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(AIPDataItem target) throws Exception {
		
		transformFieldsById("ddc", "<ddc,dc.subject.ddc>");
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/NCL/NCL_20Feb_2019";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/NCL/logs";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/NCL/out";
		String name = "ncl";
		
		String ddcMapFile = "/home/dspace/debasis/NDL/generated_xml_data/NCL/DDC_New.csv";
		
		NCLDataCorrection p = new NCLDataCorrection(input, logLocation, outputLocation, name);
		p.turnOffJsonKeyValidationFlag();
		p.turnOffControlFieldsValidationFlag();
		p.turnOffLoadHierarchyFlag();
		p.turnOffFirstFailOnValidation();
		p.addMappingResource(ddcMapFile, "ID", "ddc");
		p.correctData();
		
		System.out.println("Done.");
	}
}