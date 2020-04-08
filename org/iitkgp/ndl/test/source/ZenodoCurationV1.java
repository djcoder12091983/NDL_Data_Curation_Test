package org.iitkgp.ndl.test.source;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.json.NDLJSONParser;
import org.iitkgp.ndl.util.NDLDataUtils;

public class ZenodoCurationV1 extends NDLSIPCorrectionContainer {

	public ZenodoCurationV1(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		String webpage = target.getSingleValue("dc.identifier.other:alternateWebpageUri");
		String uri = target.getSingleValue("dc.identifier.uri");
		if(!StringUtils.equalsIgnoreCase(webpage, uri)) {
			// delete case
			return false;
		}
		// stay case
		if(isMultiValued("dc.relation.ispartofseries")) {
			delete("dc.relation.ispartofseries");
		}
		target.deleteDuplicateFieldValues("dc.identifier.uri", "ndl.sourceMeta.additionalInfo:relatedContentUrl");
		deleteIfContainsByRegex("dc.identifier.other:alternateContentUri", ".*api.*");
		Map<String, String> sponsorship = NDLDataUtils.mapFromJson(target.getSingleValue("dc.description.sponsorship"));
		if(sponsorship != null) {
			target.add("dc.identifier.other:journal", sponsorship.get("title"));
			target.add("dc.identifier.other:issue", sponsorship.get("issue"));
			target.add("dc.identifier.other:volume", sponsorship.get("volume"));
			if(!target.exists("dc.publisher.date")) {
				add("dc.publisher.date", sponsorship.get("year"));
			}
			target.add("dc.format.extent:pages", sponsorship.get("pages"));
			// delete this
			delete("dc.description.sponsorship");
		}
		
		List<String> tvalues = target.getValue("dc.coverage.temporal");
		for(String tvalue : tvalues) {
			NDLJSONParser t = new NDLJSONParser(tvalue);
			target.add("dc.description.sponsorship", t.getText("funder.name"));
		}
		target.replaceByRegex("dc.identifier.other:doi", "^https://doi\\.org/", "");
		
		return true;
	}

	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/zenodo/in/2019.May.14.11.18.22.Zenedo.data.tar.gz"; // flat SIP location or compressed SIP location
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/zenodo/logs"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/zenodo/out";
		String name = "zenodo.v1";
		
		ZenodoCurationV1 p = new ZenodoCurationV1(input, logLocation, outputLocation, name);
		p.turnOffControlFieldsValidationFlag();
		p.turnOffLoadHierarchyFlag();
		p.correctData();
		
		System.out.println("Done.");
	}
}