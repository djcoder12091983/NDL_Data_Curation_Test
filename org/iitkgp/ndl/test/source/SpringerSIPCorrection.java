package org.iitkgp.ndl.test.source;

import java.util.Set;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class SpringerSIPCorrection extends NDLSIPCorrectionContainer {
	
	Set<String> coValues = null;
	
	// constructor
	public SpringerSIPCorrection(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		move("dc.subject.lcsh", "dc.subject.other:lcsh");
		move("dc.subject.lcc", "dc.subject.other:lcc");
		delete("dc.identifier.other:ISBN");
		moveIfContains("dc.contributor.other", "dc.title.alternative", "Martinus Nijhoff");
		String co = getSingleValue("dc.contributor.other");
		if(co != null && co.contains("Martinus Nijhoff")) {
			target.delete("dc.contributor.other");
			target.add("dc.contributor.author", "Martinus Nijhoff");
		} else {
			target.move("dc.contributor.other", "dc.contributor.other:organization");
		}
		add("dc.rights.accessRights", "subscribed");
		add("dc.description.searchVisibility", "true");
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/SpringerEbooks-SIP-05.02.2018.tar.gz";
		String logLocation = "<logs>";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/out";
		String name = "springer";
		
		String coFile = "/home/dspace/debasis/NDL/generated_xml_data/Nirupam/conf/springer.co.values";
		
		SpringerSIPCorrection p = new SpringerSIPCorrection(input, logLocation, outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		p.coValues = NDLDataUtils.loadSet(coFile);
		p.correctData();
		
		System.out.println("Done.");
	}

}