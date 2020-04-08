package org.iitkgp.ndl.test.source;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.correction.NDLAIPCorrectionContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class JUITCorrectionV1 extends NDLAIPCorrectionContainer {
	
	Map<String, String> degree = null;
	Map<String, String> lrt = null;
	Map<String, String> titleLRT = null;
	
	// constructor
	public JUITCorrectionV1(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(AIPDataItem target) throws Exception {
		
		System.out.println(getCurrentCollectionName());
		
		// correction logic
		split("dc.contributor.author", ';');
		split("dc.contributor.advisor", ';');
		target.add("dc.language.iso", "eng");
		target.add("dc.description.searchVisibility", "true");
		target.move("dc.identifier.other", "dc.identifier.other:alternateContentUri");
		target.add("dc.source", "Jaypee University of Information Technology (JUIT)");
		target.add("dc.source.uri", "http://www.ir.juit.ac.in:8080/jspui/");
		target.add("dc.rights.accessRights", "open");
		move("dc.date.issued", "dc.publisher.date");
		normalize("dc.publisher.date");
		add("dc.format.mimetype", "application/pdf");
		target.replaceByRegex("dc.identifier.uri", "^http://hdl\\.handle\\.net",
				"http://www.ir.juit.ac.in:8080/jspui/handle");
		
		// LRT mapping
		String type = target.getSingleValue("dc.type");
		if(StringUtils.isNotBlank(type)) {
			int idx = type.indexOf(' ');
			String first = idx != -1 ? type.substring(0, idx) : null;
			if(degree.containsKey(first)) {
				target.add("dc.type.degree", degree.get(first));
				target.add("lrmi.learningResourceType", degree.get(type.substring(idx + 1)));
			} else {
				target.add("lrmi.learningResourceType", degree.get(type));
			}
		}
		if(isUnassigned("lrmi.learningResourceType")) {
			// still not assigned
			transformFieldByPartialMatch("lrmi.learningResourceType", "dc.title", titleLRT);
		}
		
		target.updateSingleValue("dc.type", "text");
		
		// publisher
		target.updateSingleValue("dc.publisher", "Jaypee University of Information Technology");
		target.add("dc.publisher.place", "Solan; H.P.");
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/AIP_TEST/Jaypee_University_8Nov_2018";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/AIP_test/logs";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/AIP_TEST/out";
		String name = "juit.v1";
		
		String degreeFile = "/home/dspace/debasis/NDL/generated_xml_data/AIP_TEST/degree.csv";
		String lrtFile = "/home/dspace/debasis/NDL/generated_xml_data/AIP_TEST/lrt.csv";
		String titleLRTFile = "/home/dspace/debasis/NDL/generated_xml_data/AIP_TEST/title.lrt.csv";
		
		JUITCorrectionV1 t = new JUITCorrectionV1(input, logLocation, outputLocation, name);
		t.turnOnAccessHierarchyFlag();
		t.degree = NDLDataUtils.loadKeyValue(degreeFile);
		t.lrt = NDLDataUtils.loadKeyValue(lrtFile);
		t.titleLRT = NDLDataUtils.loadKeyValue(titleLRTFile);
		t.correctData();
		
		System.out.println("Done.");
	}
}