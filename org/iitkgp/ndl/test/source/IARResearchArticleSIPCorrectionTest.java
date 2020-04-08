package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;

public class IARResearchArticleSIPCorrectionTest extends NDLSIPCorrectionContainer {
	
	int deleted = 0;
	
	// constructor
	public IARResearchArticleSIPCorrectionTest(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	// test
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		deleted += target.delete("dc.publisher");
		return true;
	}
	
	// correct
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/IAR/raw_data/research_article/out/2018.Sep.13.10.19.27.JSTOR.v2/2018.Sep.13.10.19.27.JSTOR.v2.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/IAR/raw_data/research_article/logs";
		String outputLocation = "/home/dspace/debasis/NDL/IAR/raw_data/research_article/out/";
		String name = "Research_article.V3";
	
		IARResearchArticleSIPCorrectionTest p = new IARResearchArticleSIPCorrectionTest(input, logLocation, outputLocation, name);
		p.correctData();
		
		System.out.println("Deleted: " + p.deleted);
		
		System.out.println("Done.");
	}

}