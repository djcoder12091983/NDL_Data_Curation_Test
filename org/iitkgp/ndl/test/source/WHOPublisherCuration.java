package org.iitkgp.ndl.test.source;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.ConfigurationData;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;

public class WHOPublisherCuration extends NDLSIPCorrectionContainer {
	
	//Pattern pattern = Pattern.compile("(.+)\\((.+)\\)$");
	Map<String, String> moves = new HashMap<String, String>(8);
	
	// constructor
	public WHOPublisherCuration(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
		
		moves.put("departments", "dc.publisher.department");
		moves.put("institutes", "dc.publisher.institution");
		moves.put("studeygroups", "dc.contributor.other:studyGroup");
		moves.put("authors", "dc.contributor.author");
		moves.put("places", "dc.publisher.place");
		moves.put("organizations", "dc.contributor.other:organization");
		//moves.put("translators", "dc.contributor.other:translator");
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		//String id = target.getId();
		List<NDLDataNode> nodes = target.getNodes("dc.publisher");
		for(NDLDataNode node : nodes) {
			String value = node.getTextContent();
			/*if(value.startsWith(",") || value.startsWith("The ,")) {
				log("publisher.logger", "Original value: " + value);
				value = value.replaceFirst("^(,|The ,) *", "");
				// move
				log("publisher.logger", value + " moved to dc.publisher.department");
				node.getParentNode().removeChild(node);
				add("dc.publisher.department", value);
				continue;
			}*/
			if(containsMappingKey("deletes." + ConfigurationData.escapeDot(value))) {
				log("publisher.logger", value + " deleted");
				// delete candidate
				node.remove();
			} else {
				// move
				//boolean found = false;
				for(String key : moves.keySet()) {
					if(containsMappingKey(key + "." + ConfigurationData.escapeDot(value))) {
						// move to field
						String field = moves.get(key);
						log("publisher.logger", value + " moved to " + field);
						node.remove();
						target.add(field, value);
						//found = true;
						break;
					}
				}
				
				/*if(!found) {
					String tkey = "translators." + ConfigurationData.escapeDot(value);
					if(containsMappingKey(tkey)) {
						String tvalue = getMappingKey(tkey);
						log("publisher.logger", value + " moved to dc.contributor.other:translator as " + tvalue);
						node.getParentNode().removeChild(node);
						target.add("dc.contributor.other:translator", tvalue);
					} else if(containsMappingKey("11map." +  ConfigurationData.escapeDot(value))) {
						node.getParentNode().removeChild(node);
						// institute and department
						String institute = "منظمة الصحة العالمي";
						String dept = "المكتب الإقليمي لشرق المتوسط";
						log("publisher.logger", value + " transforms institute: " + institute + " department: " + dept);
						target.add("dc.publisher.institution", institute);
						target.add("dc.publisher.department", dept);
					} else {
						// place logic
						Matcher matcher = pattern.matcher(value);
						if(matcher.find()) {
							// found
							String place = matcher.group(2).trim();
							String tokens[] = place.split(":");
							if(tokens.length > 1) {
								// last is place
								place = tokens[tokens.length - 1].trim();
							} else {
								place = tokens[0].trim(); // single token
							}
							if(!containsMappingKey("wrongplace." + ConfigurationData.escapeDot(place))) {
								// valid place
								String publisher = matcher.group(1).trim();
								log("publisher.logger", value + " transforms publisher: " + publisher + " place: " + place);
								node.setTextContent(publisher);
								target.add("dc.publisher.place", place);
							}
						}
					}
				}*/
			}
		}
		
		// success
		return true;
	}
	
	/*public static void main(String[] args) {
		
		System.out.println(",  debasis jana".replaceFirst("^(,|The ,) *", ""));
		System.out.println("The ,  debasis jana".replaceFirst("^(,|The ,) *", ""));
		
		// WHO Expert Committee on the Selection and Use of Essential Medicines (15th : 2007 : Geneva, Switzerland)
		Pattern pattern = Pattern.compile("(.+)\\((.+)\\)$");
		Matcher matcher = pattern.matcher(
				"WHO Centre for Health Development (Kobe, Japan)");
		if(matcher.find()) {
			// match found
			int c = matcher.groupCount();
			for(int i = 0; i <= c; i++) {
				System.out.println(matcher.group(i));
			}
		}
	}*/
	
	// test
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/SIP/18.Jul.2018.WHO.misc_v2.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/SIP/logs";
		String outputLocation = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/SIP/2nd";
		String name = "WHO.Misc";
		
		String departmentsFile = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/conf/department.list.csv";
		String institutesFile = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/conf/institute.list.csv";
		String studeygroupsFile = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/conf/studygroup.list.csv";
		String deletesFile = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/conf/delete.publisher.list.csv";
		String authorsFile = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/conf/author.list.csv";
		//String translatorsFile ="/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/conf/translator.list.csv";
		String placesFile = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/conf/place.list.csv";
		String organizationsFile = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/conf/organization.list.csv";
		//String staticPublisherMapFile = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/conf/11mappublisher.map.csv";
		//String wrongPlaceFile = "/home/dspace/debasis/NDL/WHO/cluster.data/out_selected/others/conf/wrong.place.list.csv";
		
		WHOPublisherCuration p = new WHOPublisherCuration(input, logLocation, outputLocation, name);
		p.addTextLogger("publisher.logger");
		p.addMappingResource(departmentsFile, "departments");
		p.addMappingResource(institutesFile, "institutes");
		p.addMappingResource(studeygroupsFile, "studeygroups");
		p.addMappingResource(deletesFile, "deletes");
		p.addMappingResource(authorsFile, "authors");
		//p.addMappingResource(translatorsFile, "translators");
		p.addMappingResource(placesFile, "places");
		p.addMappingResource(organizationsFile, "organizations");
		//p.addMappingResource(wrongPlaceFile, "wrongplace");
		//p.addMappingResource(staticPublisherMapFile, "11map");
		
		p.correctData();
		
		System.out.println("Done.");
	}
	
}