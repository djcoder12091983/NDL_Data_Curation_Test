package org.iitkgp.ndl.test.source;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class SSCSIPCorrection extends NDLSIPCorrectionContainer {
	
	Set<String> auRemoveSet;
	Set<String> deleteTokens; 
	Set<String> descSet;
	Set<String> organizationSet;
	
	// construction
	public SSCSIPCorrection(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	void loadConfiguration(String auRemoveFile, String deleteTokensFile, String descFile, String organizationFile)
			throws Exception {
		auRemoveSet = NDLDataUtils.loadSet(auRemoveFile);
	    deleteTokens = NDLDataUtils.loadSet(deleteTokensFile);
	    descSet = NDLDataUtils.loadSet(descFile);
	    organizationSet = NDLDataUtils.loadSet(organizationFile);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		// author normalization
		List<NDLDataNode> names = target.getNodes("dc.contributor.author");
		for (NDLDataNode name : names) {
			String value = StringEscapeUtils.unescapeHtml4(name.getTextContent());
			String original = value;
			String tokens[] = value.split(", *");
			if(tokens.length == 2 && tokens[0].trim().equals(tokens[1].trim())) {
				// remove duplicate
				value = tokens[0];
			}
			for(String delete : deleteTokens) {
				value = value.replace(delete, "");
			}
		    value = value.replaceAll("\\(|\\)", "").replaceAll(",,", ",");
		    if(NDLDataUtils.partiallyBelongsTo(organizationSet, value)) {
		        name.remove();
		        target.add("dc.contributor.other:organization", value);
		        log("author", new String[]{original, value, "organization"});
		    } else if(NDLDataUtils.partiallyBelongsTo(descSet, value)) {
		        name.remove();
		        target.add("dc.description", value);
		        log("author", new String[]{original, value, "description"});
		    } else {
		        // author normalization
		        value = value.replaceFirst("(^(,|&|\\.|_|-))|((,|&|_|-)$)", "");
		        value = NDLDataUtils.normalizeSimpleNameByWrongNames(value, auRemoveSet);
		        if(StringUtils.isNotBlank(value)) {
		            name.setTextContent(value);
		            log("author", new String[]{original, value, "author"});
		        } else {
		            name.remove();
		            log("author", new String[]{original, value, "delete"});
		        }
		    }
		}
		
		// organization correction
		transformFieldByExactMatch("dc.contributor.other:organization", "org");
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/generated_xml_data/SSC/ScienceSupercourse-SIP-30.10.2018.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/SSC/logs";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/SSC/out";
		String name = "SSC.V2";
		
		String auRemoveFile = "/home/dspace/debasis/NDL/generated_xml_data/SSC/conf/authRemove.data";
		String authPreFile = "/home/dspace/debasis/NDL/generated_xml_data/SSC/conf/delete.tokens.data";
		String descFile = "/home/dspace/debasis/NDL/generated_xml_data/SSC/conf/desc.data";
		String organizationFile = "/home/dspace/debasis/NDL/generated_xml_data/SSC/conf/organization.data";
		String orgFile = "/home/dspace/debasis/NDL/generated_xml_data/SSC/conf/org.csv";
		
		SSCSIPCorrection p = new SSCSIPCorrection(input, logLocation, outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		p.loadConfiguration(auRemoveFile, authPreFile, descFile, organizationFile);
		p.addCSVLogger("author", new String[]{"Old", "New", "Field"});
		p.addMappingResource(orgFile, "Old", "org");
		p.correctData();
		
		System.out.println("Done.");
		
	}
}