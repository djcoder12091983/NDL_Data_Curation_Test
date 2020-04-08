package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.NDLAssetType;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;

// NASA correction 2
public class IARNasaReportsSIPCorrection2 extends NDLSIPCorrectionContainer {
	
	// constructor
	public IARNasaReportsSIPCorrection2(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		// initially remove all assets
		target.removeAsset(NDLAssetType.FULLTEXT);
		target.removeAsset(NDLAssetType.THUMBNAIL);
		
		target.move("dc.description", "dc.description.abstract");
		boolean visibility = Boolean.valueOf(target.getSingleValue("dc.description.searchVisibility"));
		if(!visibility) {
			delete("dc.identifier.uri");
			// move
			target.move("dc.identifier.other:alternateContentUri", "dc.identifier.uri");
			target.updateSingleValue("dc.description.searchVisibility", String.valueOf("true"));
		}
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/IAR/raw_data/NASA.report.server/in/IAR-NASA-TechRept-201Bkp-30.08.2018.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/IAR/raw_data/NASA.report.server/logs";
		String outputLocation = "/home/dspace/debasis/NDL/IAR/raw_data/NASA.report.server/out";
		String name = "NASA.reports.v1";
		
		IARNasaReportsSIPCorrection2 p = new IARNasaReportsSIPCorrection2(input, logLocation, outputLocation, name);
		p.correctData();
		
		System.out.println("Done.");
	}
}