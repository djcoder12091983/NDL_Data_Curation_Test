package org.iitkgp.ndl.test.source;

import java.util.Set;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class WELTestCorrection extends NDLSIPCorrectionContainer {
	
	Set<String> wrongsubjects;
	Set<String> wrongdesc;

	public WELTestCorrection(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		deleteIfContainsByRegex("dc.description", "^((http)|([0-9]+)|v).*$");
		deleteIfContains("dc.description", wrongdesc);
		
		splitByRegex("dc.subject", ";|#");
		deleteIfContainsByRegex("dc.subject", "^[0-9]+.*$");
		deleteIfContains("dc.subject", wrongsubjects);
		normalize("dc.subject");
		
		target.replaceByRegex("dc.language.iso", "^(eth)|(its)|(law)$", "eng");
		
		deleteIfContains("dc.format.extent:pageCount", "0");
		deleteIfContains("dc.format.extent:endingPage", "0");
		
		if(isUnassigned("dc.format.mimetype")) {
			target.add("dc.format.mimetype", "application/pdf");
		}
		
		if(isUnassigned("dc.type")) {
			target.add("dc.type", "text");
		}
		
		deleteIfContains("dc.date.copyright", "206-09-02", "216-01-01");
		target.replaceByRegex("dc.publisher.date", "-00", "-01");
		target.replaceByRegex("dc.publisher", "(^(#|&|~|\\.|,|\\?|;|:|!|-))|((#|&|~|\\.|,|\\?|;|:|!|-)$)", "");
		
		if(target.contains("lrmi.learningResourceType", "thesis")) {
			move("dc.contributor.author", "dc.creator.researcher");
		}
		
		//System.out.println("before: " + getValue("ndl.sourceMeta.additionalInfo"));
		
		target.delete("ndl.sourceMeta.additionalInfo:thumbnail");
		target.move("ndl.sourceMeta.additionalInfo:References", "ndl.sourceMeta.additionalInfo:references");
		
		//System.out.println("after: " + getValue("ndl.sourceMeta.additionalInfo"));
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/SIP_TEST/WEL_TEST/2019.Jul.31.11.19.01.FinalIngestion4.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/SIP_TEST/WEL_TEST/logs"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/SIP_TEST/WEL_TEST/out";
		String name = "wel.test";
		
		String wrongSubjectsFile = "/home/dspace/debasis/NDL/NDL_sources/SIP_TEST/WEL_TEST/conf/removalfromsubject";
		String wrongDescFile = "/home/dspace/debasis/NDL/NDL_sources/SIP_TEST/WEL_TEST/conf/removalfromsubject";
		
		WELTestCorrection p = new WELTestCorrection(input, logLocation, outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		p.wrongsubjects = NDLDataUtils.loadSet(wrongSubjectsFile);
		p.wrongdesc = NDLDataUtils.loadSet(wrongDescFile);
		
		p.correctData(); // corrects data
		
		System.out.println("Done.");
	}

}