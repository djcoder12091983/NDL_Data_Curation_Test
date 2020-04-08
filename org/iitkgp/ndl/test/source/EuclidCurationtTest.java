package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;

public class EuclidCurationtTest extends NDLSIPCorrectionContainer {

	public EuclidCurationtTest(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		if(target.getId().equals("projecteuclid/1478833220")) {
			System.out.println("Before: " + target.getSingleValue("dc.identifier.other:doi"));
			target.replaceByRegex("dc.identifier.other:doi", "^doi: *", "");
			//target.replaceByRegex("dc.identifier.other:doi", "^((doi: *)|[^0-9]|(DOI: *))", "");
			System.out.println("After: " + target.getSingleValue("dc.identifier.other:doi"));
		}
		
		return false;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/euclid/2019.Apr.09.10.18.14.Euclid.V1.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/euclid/logs";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/euclid/out";
		String name = "euclid.v3";
		
		EuclidCurationtTest p = new EuclidCurationtTest(input, logLocation, outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		p.dontShowWarnings();
		p.correctData();
		
		System.out.println("Done.");
	}
}
