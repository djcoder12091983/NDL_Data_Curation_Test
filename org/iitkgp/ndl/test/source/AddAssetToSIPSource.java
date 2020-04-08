package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.NDLAssetType;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

// adding assets to SIP source
public class AddAssetToSIPSource extends NDLSIPCorrectionContainer {
	
	// constructor
	public AddAssetToSIPSource(String input, String logLocation, String outputLocation, String name) {
		// validation flag OFF
		super(input, logLocation, outputLocation, name, false);
	}
	
	// allow all items
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		return true;
	}
	
	@Override
	protected String getAssetID(SIPDataItem item) {
		return "log" + NDLDataUtils.getHandleSuffixID(item.getId());
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/EF/2019.Nov.07.16.30.55.Exam_Fear_5_5.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/EF/out";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/EF/out";
		String name = "ef.th";
		
		String thumbnailLocaiton = "/home/dspace/debasis/NDL/NDL_sources/EF/6/assets";
		//String fulltextLocation = "/home/dspace/debasis/NDL/NDL_sources/EF/6";
		
		AddAssetToSIPSource p = new AddAssetToSIPSource(input, logLocation, outputLocation, name);
		p.addAssetLocation(NDLAssetType.THUMBNAIL, thumbnailLocaiton);
		//p.addAssetLocation(NDLAssetType.FULLTEXT, fulltextLocation);
		p.correctData(); // correct data
		
		System.out.println("Done.");
	}

}