package org.iitkgp.ndl.test.source;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.ConfigurationData;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class ZenodoCurationV6 extends NDLSIPCorrectionContainer {
	
	Set<String> wrongAuthors;
	int aoc = 0;
	Set<String> titles = new HashSet<String>();
	
	public ZenodoCurationV6(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		String title = target.getSingleValue("dc.title");
		if(titles.contains(title)) {
			// duplicate
			// remove item
			return false;
		}
		titles.add(title); //track title
		
		target.deleteDuplicateFieldValues("dc.description.abstract", "dc.subject");
		target.deleteDuplicateFieldValues("dc.source", "dc.subject");
		target.deleteDuplicateFieldValues("dc.title", "dc.subject");
		
		deleteIfContainsByRegex("dc.identifier.other:issue", ".*Special.*");
		
		// TEST
		String id = NDLDataUtils.getHandleSuffixID(target.getId());
		if(id.equals("1312882")) {
			delete("dc.contributor.author");
			target.add("dc.contributor.other:organization", "Bioquímica Oral, Grupo De Investigação Em Biologia E.|De Medicina Dentária Da Universidade De Lisboa, Faculdade");
		}
		/*if(id.equals("1312882")) {
			System.out.println("A: " + NDLDataUtils.join(target.getValue("dc.contributor.author"), '|'));
			//System.out.println("O: " + "authors_mapping." + ConfigurationData.escapeDot(v) + ".Org");
		}*/
		
		List<NDLDataNode> authors = target.getNodes("dc.contributor.author");
		List<String> nauthors = new LinkedList<String>();
		for(NDLDataNode author : authors) {
			String v = author.getTextContent();
			int l = v.length();
			if(l <= 2) {
				log("a.delete", v);
				// remove case
				author.remove();
			} else {
				// modification case
				if(wrongAuthors.contains(v)) {
					log("a.delete", v);
					// remove case
					author.remove();
				} else {
					// modification if any
					String akey = "authors_mapping." + ConfigurationData.escapeDot(v) + ".Author";
					String okey = "authors_mapping." + ConfigurationData.escapeDot(v) + ".Org";
					// TEST
					/*if(id.equals("851939")) {
						System.out.println("C: " + containsMappingKey(okey));
						String o = getMappingKey(okey);
						System.out.println("ORG: " + target.getId() + " => " + o);
					}*/
					
					boolean f = false;
					if(containsMappingKey(akey)) {
						String a = getMappingKey(akey);
						//System.out.println("AUTHORS: " + target.getId() + " => " + a);
						author.remove();
						if(StringUtils.isNotBlank(a)) {
							// modify case
							String tokens[] = a.split(" *; *");
							for(String t : tokens) {
								t = t.trim();
								nauthors.add(t);
							}
						}
						f = true;
					}
					if(containsMappingKey(okey)) {
						String o = getMappingKey(okey);
						if(StringUtils.equalsIgnoreCase(o, "organisation")) {
							// modify and update
							String tokens[] = v.split(", ");
							target.add("dc.contributor.other:organization", tokens[1] + " " + tokens[0]);
						} else if(StringUtils.equalsIgnoreCase(o, "delete")) {
							// delete
							log("a.delete", v);
							if(!f) {
								author.remove();
							}
						} else {
							//System.out.println("ORG: " + target.getId() + " => " + o);
							target.add("dc.contributor.other:organization", o);
						}
						f = true;
					}
					
					if(f) {
						log("a.modify", v);
					}
				}
			}
		}
		
		if(nauthors.size() > 0) {
			// modify
			target.add("dc.contributor.author", nauthors);
		}
		
		authors = target.getNodes("dc.contributor.author");
		boolean delete = false;
		for(NDLDataNode author : authors) {
			String v = author.getTextContent();
			int l = v.length();
			if(l > 50 || l < 5) {
				log("a.delete", v);
				// remove case
				// item delete
				delete = true;
				break;
			}
			if(StringUtils.equalsIgnoreCase(v, "sciences")) {
				log("a.delete", v);
				author.remove();
			}
			if(StringUtils.endsWithIgnoreCase(v, " of")) {
				/*log("a.delete", v);
				author.remove();*/
				// modify and update
				String tokens[] = v.split(", ");
				author.setTextContent(tokens[1] + " " + tokens[0]);
				System.err.println("WARN: ID(" + id + ") => " + v);
			}
		}
		
		return !delete;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/zenodo/out/2019.Jun.26.14.44.48.zenodo.v5/2019.Jun.26.14.44.48.zenodo.v5.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/zenodo/logs"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/zenodo/out";
		String name = "zenodo.v6";
		
		String mappingfile = "/home/dspace/debasis/NDL/NDL_sources/zenodo/conf/author.org.mapping.csv";
		String wrongafile = "/home/dspace/debasis/NDL/NDL_sources/zenodo/conf/authors.delete";
		
		ZenodoCurationV6 p = new ZenodoCurationV6(input, logLocation, outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		p.wrongAuthors = NDLDataUtils.loadSet(wrongafile);
		p.addMappingResource(mappingfile, "Value", "authors_mapping");
		//p.addCSVLogger("a.modify", new String[]{"ID", ""});
		p.addTextLogger("a.modify");
		p.addTextLogger("a.delete");
		p.correctData();
		
		System.out.println("Done.");
	}

}