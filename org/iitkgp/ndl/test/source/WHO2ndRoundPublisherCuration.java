package org.iitkgp.ndl.test.source;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.ConfigurationData;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.data.normalizer.NDLDataNormalizer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class WHO2ndRoundPublisherCuration extends NDLSIPCorrectionContainer {
	
	Map<String, List<String>> configuration = new HashMap<String, List<String>>();
	Map<String, Map<String, String>> confLRT = new HashMap<String, Map<String, String>>();
	
	// constructor
	public WHO2ndRoundPublisherCuration(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	void load(String field, String file) throws Exception {
		configuration.put(field, IOUtils.readLines(new FileInputStream(file), "UTF-8"));
	}
	
	void loadLRT(String key, String file) throws Exception {
		confLRT.put(key, NDLDataUtils.loadKeyValue(file));
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		// delete logic
		String handle = target.getId();
		String id = handle.substring(handle.indexOf('/') + 1);
		if(containsMappingKey("delete." + id)) {
			// delete it
			log("publisher.logger", handle + " deleted.");
			return false;
		}
		
		apply(target, "dc.publisher", "dc.publisher");
		apply(target, "dc.publisher.institution", "dc.publisher.institution");
		
		deleteByMappingKey("dc.publisher", "pubdelete");
		transformFieldByExactMatch("dc.publisher.place", "pubplace11");
		
		// LRT mapping
		transformFieldByPartialMatch("lrmi.learningResourceType", "dc.relation.ispartofseries", confLRT.get("series"));
		transformFieldByPartialMatch("lrmi.learningResourceType", "dc.title", confLRT.get("title"));
		transformFieldByPartialMatch("lrmi.learningResourceType", "dc.subject", confLRT.get("subject"));
		
		if(target.contains("lrmi.learningResourceType", "technicalReport")) {
			deleteIfContains("lrmi.learningResourceType", "report");
		}
		
		// author correction
		normalize("dc.contributor.author");
		
		target.removeDuplicate("dc.publisher");
		target.removeDuplicate("dc.publisher.place");
		target.removeDuplicate("dc.publisher.department");
		target.removeDuplicate("dc.publisher.institution");
		
		return true;
	}
	
	void apply(SIPDataItem target, String targetField, String ... excludeFields) throws Exception {
		// exclude fields
		Set<String> excludes = new HashSet<String>(2);
		for(String field : excludeFields) {
			excludes.add(field);
		}
		
		List<NDLDataNode> nodes = target.getNodes(targetField);
		for(NDLDataNode node : nodes) {
			String value = node.getTextContent();
			
			String tokens[] = value.split(" *\\. *");
			StringBuilder rem = new StringBuilder();
			for(String token : tokens) {
				
				boolean flag = false;
				if(containsMappingKey("places." + ConfigurationData.escapeDot(token))) {
					// place
					log("publisher.logger", token + " moved to dc.publisher.place");
					target.add("dc.publisher.place", token);
					flag = true;
				} else {
					for(String field : configuration.keySet()) {
						// exclude fields
						if(excludes.contains(field)) {
							continue;
						}
						// each field
						for(String fvalue : configuration.get(field)) {
							if(StringUtils.contains(token, fvalue)) {
								// match
								target.add(field, token);
								log("publisher.logger", token + " moved to " + field);
								flag = true;
								break;
							}
						}
						if(flag) {
							// no more matching need
							break;
						}
					}
				}
				
				if(!flag) {
					// if not used
					rem.append(token).append(".");
				}
			}
			
			int s = rem.length();
			if(s == 0) {
				// empty
				node.remove();
			} else {
				// update with remaining
				rem.deleteCharAt(s - 1); // last do remove
				node.setTextContent(rem.toString());
			}
		}
	}
	
	// test
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/SIP/18.Jul.2018.WHO.misc_v3.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/SIP/logs";
		String outputLocation = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/SIP/2nd";
		String name = "WHO.Misc";
		
		String deptFile = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/conf/publisher_conf/token.publisher.dept.list";
		String sgFile = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/conf/publisher_conf/token.publisher.sg.list";
		String orgFile = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/conf/publisher_conf/token.publisher.org.list";
		String instFile = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/conf/publisher_conf/token.publisher.inst.list";
		String placeFile = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/conf/publisher_conf/token.publisher.place.csv";
		String publisherFile = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/conf/publisher_conf/token.publisher.list";
		String publisherDeleteFile = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/conf/publisher_conf/token.publisher.delete.csv";
		String publisherPlace11File = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/conf/publisher_conf/publisher.place.11.csv";
		String deleteFile = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/conf/publisher_conf/delete.handle.list.csv";
		
		String lrtFile1 = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/conf/publisher_conf/series2lrt.map.csv";
		String lrtFile2 = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/conf/publisher_conf/title2lrt.map.csv";
		String lrtFile3 = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/conf/publisher_conf/subject2.lrt.map.csv";
		
		WHO2ndRoundPublisherCuration p = new WHO2ndRoundPublisherCuration(input, logLocation, outputLocation, name);
		p.addTextLogger("publisher.logger");
		p.addMappingResource(deleteFile, "delete");
		p.load("dc.publisher.department", deptFile);
		p.load("dc.publisher.institution", instFile);
		p.load("dc.contributor.other:organization", orgFile);
		p.load("dc.contributor.other:studyGroup", sgFile);
		p.load("dc.publisher", publisherFile);
		p.loadLRT("series", lrtFile1);
		p.loadLRT("title", lrtFile2);
		p.loadLRT("subject", lrtFile3);
		p.addMappingResource(placeFile, "places");
		p.addMappingResource(publisherDeleteFile, "pubdelete");
		p.addMappingResource(publisherPlace11File, "Old", "pubplace11");
		p.addNormalizer("dc.contributor.author", new NDLDataNormalizer() {
			// author normalization
			@Override
			public Set<String> transform(String input) {
				String tokens[] = input.split(" +");
				StringBuilder modified = new StringBuilder();
				for(String token : tokens) {
					if(token.length() == 1) {
						// dot after single letter
						token = token + ".";
					}
					modified.append(token).append(" ");
				}
				modified.deleteCharAt(modified.length() - 1);
				return NDLDataUtils.createSet(modified.toString());
			}
		});
		
		p.correctData();
		
		System.out.println("Done.");
	}
}