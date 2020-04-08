package org.iitkgp.ndl.test.source;

import java.util.List;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;

// IAR NAL SIP correction
public class IARNALSIPCorrection extends NDLSIPCorrectionContainer {
	
	/*Map<String, String> lrtMapping = null;
	Map<String, String> langMapping = null;
	Map<String, String> ddcMapping = null;*/
	
	// constructor
	public IARNALSIPCorrection(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	// loads configuration
	/*void loadConfiguration(String lrtMappingFile, String langMappingFile, String ddcMappingFile) throws Exception {
		lrtMapping = NDLDataUtils.loadKeyValue(lrtMappingFile);
		langMapping = NDLDataUtils.loadKeyValue(langMappingFile);
		ddcMapping = NDLDataUtils.loadKeyValue(ddcMappingFile);
	}*/
	
	// correction logic
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		/*normalize("dc.subject", ';');
		delete("dc.subject.ddc");
		transformFieldByExactMatch("dc.subject.ddc", "dc.subject", ddcMapping, true);
		if(!StringUtils.equals("article", getSingleValue("lrmi.learningResourceType"))) {
			// if not article
			transformFieldByExactMatch("lrmi.learningResourceType", "dc.title", lrtMapping, true);
		}
		transformFieldByExactMatch("dc.language.iso", langMapping);*/
		
		List<String> bibid = target.getAdditionalInfoJSONKeyedValue("identifier");
		if(!bibid.isEmpty()) {
			System.out.println(bibid);
		}
		
		// success correction
		return true;
	}
	
	// testing
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/IAR/raw_data/NAL/in/InternetArch-NAL-201Bkp-16.08.2018.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/IAR/raw_data/NAL/temp_logs";
		String outputLocation = "/home/dspace/debasis/NDL/IAR/raw_data/NAL/out";
		String name = "NAL.V5";
		
		/*String lrtMappingFile = "/home/dspace/debasis/NDL/IAR/raw_data/NAL/conf/lrtmap.csv";
		String langMappingFile = "/home/dspace/debasis/NDL/IAR/raw_data/NAL/conf/language.csv";
		String ddcMappingFile = "/home/dspace/debasis/NDL/IAR/raw_data/NAL/conf/ddc.csv";*/
		
		IARNALSIPCorrection p = new IARNALSIPCorrection(input, logLocation, outputLocation, name);
		//p.loadConfiguration(lrtMappingFile, langMappingFile, ddcMappingFile);
		p.correctData(); // corrects data
		
		System.out.println("End.");
	}

}