package org.iitkgp.ndl.test.source;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class OAAuthorCurationV2 extends NDLSIPCorrectionContainer {
	
	Map<String, String> corrections1;
	Map<String, String> corrections2;

	public OAAuthorCurationV2(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		String id = NDLDataUtils.getHandleSuffixID(target.getId());
		if(corrections2.containsKey(id)) {
			delete("dc.contributor.author");
			target.add("dc.contributor.author", corrections2.get(id).split(";"));
		}
		
		deleteByMappingKey("dc.contributor.author", "delete", true);
		transformFieldByExactMatch("dc.contributor.author", corrections1);
		
		target.moveByRegex("dc.contributor.author", "dc.contributor.other:organization", "^.*Program.*$");
		target.replaceByRegex("dc.contributor.author", "(^&\\. *)|( *Ph D\\.$)|( *M\\. D\\. P\\. H\\.$)|( *ABSTRACT$)|(^(-+ *)+)", "");
		
		List<String> authors = target.getValue("dc.contributor.author");
		int l = authors.size() - 1;
		boolean f = false;
		for(int i = 0; i < l;) {
			String v1 = authors.get(i);
			String v2 = authors.get(i + 1);
			boolean f1 = v1.replaceAll("\\.| ", "").length() <= 2 || v2.replaceAll("\\.| ", "").length() <= 2;
			if(f1) {
				log("merge.case", id + " => <" + v1 + "|" + v2 + ">");
				// merge case
				f = true;
				authors.set(i, v1 + " " + v2);
				authors.set(i + 1, "__DELETE__");
				i += 2;
			} else {
				i++;
			}
		}
		if(f) {
			// merge case happens
			delete("dc.contributor.author");
			for(String a : authors) {
				if(!StringUtils.equals(a, "__DELETE__")) {
					target.add("dc.contributor.author", a);
				}
			}
		}
		
		List<NDLDataNode> nodes = target.getNodes("dc.contributor.author");
		for(NDLDataNode node : nodes) {
			String v = node.getTextContent();
			// invalid token
			if(v.length() <= 2) {
				log("delete.tokens.case", id + " => " + v);
				node.remove();
			}
		}
		
		//log("full.test", id + " => " + NDLDataUtils.join(target.getValue("dc.contributor.author"), '|'));
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/OA/input/2019.Jul.23.14.02.55.OpenAccess.V8.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/OA/logs";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/OA/out";
		String name = "oa.V9";
		
		String deleteFile = "/home/dspace/debasis/NDL/NDL_sources/OA/conf/delete1.csv";
		String correctionFile1 = "/home/dspace/debasis/NDL/NDL_sources/OA/conf/Author.correction1.csv";
		String correctionFile2 = "/home/dspace/debasis/NDL/NDL_sources/OA/conf/Author.correction2.csv";
		
		OAAuthorCurationV2 p = new OAAuthorCurationV2(input, logLocation, outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		p.addMappingResource(deleteFile, "delete", true);
		p.corrections1 = NDLDataUtils.loadKeyValue(correctionFile1);
		p.corrections2 = NDLDataUtils.loadKeyValue(correctionFile2);
		p.addTextLogger("delete.tokens.case");
		p.addTextLogger("merge.case");
		//p.addTextLogger("full.test");
		p.correctData();
		
		System.out.println("Done.");
	}
	
}