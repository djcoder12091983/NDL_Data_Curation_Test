package org.iitkgp.ndl.test.source;

import java.util.Random;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;

public class DebugRemoveMultipleLines extends NDLSIPCorrectionContainer {
	
	Random random = new Random();

	public DebugRemoveMultipleLines(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		//System.out.println(target.getSingleValue("dc.description.abstract"));
		//String abstractv = target.getSingleValue("dc.description.abstract");
		//removeMultipleLines("dc.description.abstract");
		//String abstractv1 = target.getSingleValue("dc.description.abstract");
		//System.out.println("Cross_Check: " + StringUtils.equals(abstractv, abstractv1));
		
		// random error generation
		if((random.nextInt() & 1) == 0) {
			// error on even
			throw new IllegalStateException("Processing error.");
		}
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/SIP_TEST/data_validation_SIP_test.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/SIP_TEST/logs";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/SIP_TEST/out";
		String name = "debug.errors";
		
		DebugRemoveMultipleLines p = new DebugRemoveMultipleLines(input, logLocation, outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		p.turnOnContinueOnCorrectionError();
		//p.dontShowWarnings();
		p.correctData();
		
		System.out.println("Done.");
	}
	
	public static void main1(String[] args) throws Exception {
		/*String text = "&#27;(3z&#27;(Bbarreling&#27;(3y. &#27;";
		System.out.println(StringEscapeUtils.escapeHtml4(StringEscapeUtils.unescapeHtml4(text)));*/
		//System.out.println(NDLDataUtils.normalizeSimpleName("PADM VATHI DR ,D R"));
		
		/*try {
			BufferedReader reader = new BufferedReader(new FileReader(new File("d:/test")));
		} catch(Exception ex) {
			//System.out.println(ex.getMessage());
			System.out.println(CommonUtilities.exceptionDetail(ex));
		}*/
	}

}