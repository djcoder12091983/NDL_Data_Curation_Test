package org.iitkgp.ndl.test.source;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.json.NDLJSONParser;
import org.iitkgp.ndl.json.exception.InvalidJSONExpressionException;
import org.iitkgp.ndl.util.NDLDataUtils;

// author curation
// with no render
public class ZenodoCurationV5 extends NDLSIPCorrectionContainer {
	
	//Pattern NAME_PATTERN = Pattern.compile(" *Dr +");
	
	static Set<String> WRONG_NAME_TOKENS;
	
	static {
		WRONG_NAME_TOKENS = new HashSet<String>(2);
		WRONG_NAME_TOKENS.add("dr");
		WRONG_NAME_TOKENS.add("prof");
		WRONG_NAME_TOKENS.add("ms");
		WRONG_NAME_TOKENS.add("mr");
		WRONG_NAME_TOKENS.add("professor");
		WRONG_NAME_TOKENS.add("doctor");
	}
	
	Set<String> organizations;
	Set<String> organizations1;
	Set<String> orgtokens;
	Set<String> commaNames;
	Set<String> wrongnames;
	Map<String, String> namemapping;
	Set<String> crosschecks;
	Set<String> titles = new HashSet<String>();
	
	public ZenodoCurationV5(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		String title = target.getSingleValue("dc.title");
		/*if(target.getId().equals("zenodo/1013427")) {
			// author print
			System.out.println("Author: " + target.getValue("dc.contributor.author"));
		}*/
		
		List<String> names = readAuthors(target);
		
		String id = NDLDataUtils.getHandleSuffixID(target.getId());
		if (crosschecks.contains(id)) {
			log("cross.check", "1. ID(" + id + "): " + NDLDataUtils.join(names, '#'));
			//System.out.println("1. ID(" + id + "): " + NDLDataUtils.join(names, '#'));
		}
		
		// title matches author then delete
		// organization
		List<String> newnames = new LinkedList<String>();
		boolean f = false; //  TODO this flag is unnecessary
		for(String name : names) {
			name = name.trim();
			name = name.replaceAll("\\*", "");
			if(StringUtils.containsIgnoreCase(title, name)) {
				/*if(StringUtils.equals(id, "zenodo/1208423")) {
					System.out.println("111");
				}*/
				f = true;
			} else if(StringUtils.equalsIgnoreCase(name, "iti")) {
				// special case
				f = true;
				target.add("dc.publisher.institution", "Instituto Tecnológico de Informática");
			} else if(organizations1.contains(name)) {
				/*if(StringUtils.equals(id, "zenodo/1208423")) {
					System.out.println("222");
				}*/
				f = true;
				target.add("dc.contributor.other:organization", name);
			} else if(name.matches("^and.+$") || name.matches(".+and$")) {
				/*if(StringUtils.equals(id, "zenodo/1208423")) {
					System.out.println("333");
				}*/
				f = true;
				name = name.replaceAll("(^and)|(and$)", "");
				newnames.addAll(splitByAnd(name));
			} /*else if(name.contains("(") && name.contains(")")) {
				if(StringUtils.equals(id, "zenodo/1208423")) {
					System.out.println("444");
				}
				f = true;
				name = name.replaceAll("\\(.+\\)", "");
				newnames.addAll(splitByAnd(name));
			}*/ else if(name.contains(",") && name.length() > 50) {
				/*if(StringUtils.equals(id, "zenodo/1208423")) {
					System.out.println("666");
				}*/
				// multiple names
				f = true;
				newnames.addAll(splitByAnd(name));
			} else if(name.contains(" and ") || name.contains(" & ") || name.contains(" And ")) {
				/*if(StringUtils.equals(id, "zenodo/1208423")) {
					System.out.println("555");
				}*/
				// contains and
				f = true;
				newnames.addAll(splitByAnd(name));
			} else {
				/*if(StringUtils.equals(id, "zenodo/1208423")) {
					System.out.println("777");
				}*/
				f = true;
				name = name.replaceAll("([0-9]+(-[0-9]+)?)|(\\*)", "");
				newnames.add(NDLDataUtils.normalizeSimpleNameByWrongNameTokens(name, WRONG_NAME_TOKENS));
			}
		}
		
		if (crosschecks.contains(id)) {
			log("cross.check", "2. ID(" + id + "): " + NDLDataUtils.join(newnames, '#'));
			//System.out.println("2. ID(" + id + "): " + NDLDataUtils.join(newnames, '#'));
		}
		
		if(f) {
			// names to modified
			delete("dc.contributor.author");
			target.add("dc.contributor.author", newnames);
			log("modified.names", new String[]{id, NDLDataUtils.join(newnames, '|')});
		}
		
		// normal delete by tokens
		deleteIfContains("dc.contributor.author", wrongnames);
		
		// organization
		List<String> newauthors = new LinkedList<String>();
		List<NDLDataNode> authors = target.getNodes("dc.contributor.author");
		boolean removeall = false;
		for(NDLDataNode author : authors) {
			String a = author.getTextContent();
			if(organizations.contains(a) || containsOrgToken(a)) {
				// organization
				String org;
				if(a.contains(",")) {
					String tokens[] = a.split(" *, *");
					org = tokens[1] + " " + tokens[0];
				} else {
					// without comma
					org = a;
				}
				target.add("dc.contributor.other:organization", org);
				author.remove(); // remove the node
			} else if(a.contains("|") || a.length() == 2 || a.startsWith("[")) {
				// gets from author-info and update it
				removeall = true;
				// read from author info
				author.remove();
				readFromAuthorInfo(target, newauthors);
				
				// nothing to do
				break;
			} else if(commaNames.contains(a)) {
				// comma names
				author.remove();
				String tokens[] = a.split(" *, *");
				for(String t : tokens) {
					newauthors.add(t);
				}
			}
		}
		
		/*if(target.getId().equals("zenodo/1013427")) {
			System.out.println(removeall + " " + newauthors);
		}*/
		
		if(removeall) {
			// copy names without rendering
			target.delete("dc.contributor.author");
		}
		// new authors
		target.add("dc.contributor.author", newauthors);
		
		// dr token remove
		/*authors = target.getNodes("dc.contributor.author");
		for(NDLDataNode author : authors) {
			String a = author.getTextContent();
			a = a.replaceFirst(" +((Dr\\.?)|(Prof\\.?)) +", " ");
			a = a.replaceFirst("^Dr\\.? +", ""); // in case of first
			author.setTextContent(a); // set modified one
		}*/
		
		// next modification
		transformFieldByExactMatch("dc.contributor.author", namemapping);
		
		if (crosschecks.contains(id)) {
			log("cross.check", "3. ID(" + id + "): " + NDLDataUtils.join(target.getValue("dc.contributor.author"), '#'));
			//System.out.println("3. ID(" + id + "): " + NDLDataUtils.join(target.getValue("dc.contributor.author"), '#'));
		}
	
		return true;
	}
	
	boolean containsOrgToken(String author) {
		String tokens[] = author.split("( +)|,|\\.");
		for(String token : tokens) {
			if(orgtokens.contains(token.toLowerCase())) {
				// case insensitive match
				return true;
			}
		}
		return false;
	}
	
	List<String> splitByAnd(String name) {
		name = name.replaceAll("([0-9]+(-[0-9]+)?)|(\\*)", ""); // remove digits
		if(name.length() < 50) {
			// single name, dnt split with comma
			List<String> names = new LinkedList<String>();
			String tokens[] = name.split(" +((and)|&|(And)) +");
			for(String t : tokens) {
				t.replaceFirst("^Dr\\.", "");
				t= t.replaceAll("\\(.+\\)", ""); // remove first bracket
				names.add(NDLDataUtils.normalizeSimpleNameByWrongNameTokens(t, WRONG_NAME_TOKENS));
			}
			return names;
		}
		if(name.contains(" and ") || name.contains(",") || name.contains(" & ") || name.contains(" And ")) {
			List<String> names = new LinkedList<String>();
			name = name.replaceAll("\\*", "");
			String tokens[] = name.split(",|( +((and)|&|(And)) +)");
			for(String t : tokens) {
				t.replaceFirst("^Dr\\.", "");
				t = t.replaceAll("\\(.+\\)", ""); // remove first bracket
				names.add(NDLDataUtils.normalizeSimpleNameByWrongNameTokens(t, WRONG_NAME_TOKENS));
			}
			return names;
		} else {
			return new LinkedList<String>();
		}
	}
	
	// reads authors from authorinfo
	List<String> readAuthors(SIPDataItem target) {
		List<NDLDataNode> inodes = target.getNodes("ndl.sourceMeta.additionalInfo");
		List<String> names = new LinkedList<String>();
		for(NDLDataNode inode : inodes) {
			String atxt = inode.getTextContent();
			NDLJSONParser parser = new NDLJSONParser(atxt);
			try {
				String name = parser.getText("authorInfo.name");
				if(StringUtils.isNotBlank(name)) {
					names.add(name);
				}
			} catch(InvalidJSONExpressionException ex) {
				// expression not found
				//System.err.println("Author info(" + atxt + ") not found: " + target.getId());
			}
		}
		return names;
	}
	
	// read from author info
	void readFromAuthorInfo(SIPDataItem target, List<String> newauthors) {
		List<NDLDataNode> inodes = target.getNodes("ndl.sourceMeta.additionalInfo");
		for(NDLDataNode inode : inodes) {
			String atxt = inode.getTextContent();
			NDLJSONParser parser = new NDLJSONParser(atxt);
			try {
				String name = parser.getText("authorInfo.name");
				if(StringUtils.isNotBlank(name)) {
					String tokens[] = name.split("\\|");
					// multiple names with pipe
					for(String t : tokens) {
						newauthors.add(t);
					}
				}
			} catch(InvalidJSONExpressionException ex) {
				// expression not found
				System.err.println("Author info(" + atxt + ") not found: " + target.getId());
				return;
			}
		}
	}

	public static void main(String[] args) throws Exception {
		
		// flat SIP location or compressed SIP location
		String input = "/home/dspace/debasis/NDL/NDL_sources/zenodo/in/2019.Jun.19.14.20.07.Zenodo.V4.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/zenodo/logs"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/zenodo/out";
		String name = "zenodo.v5";
		
		String orgfile = "/home/dspace/debasis/NDL/NDL_sources/zenodo/conf/org.data";
		String orgfile1 = "/home/dspace/debasis/NDL/NDL_sources/zenodo/conf/org1.data";
		String crosscheckfile = "/home/dspace/debasis/NDL/NDL_sources/zenodo/conf/crosscheckfile";
		String orgtokenfile = "/home/dspace/debasis/NDL/NDL_sources/zenodo/conf/orgtokens.data";
		String commanamefile = "/home/dspace/debasis/NDL/NDL_sources/zenodo/conf/comma.name.data";
		String wrongnamefile = "/home/dspace/debasis/NDL/NDL_sources/zenodo/conf/wrong.names";
		String namemapfile = "/home/dspace/debasis/NDL/NDL_sources/zenodo/conf/name.mapping.csv";
		
		ZenodoCurationV5 p = new ZenodoCurationV5(input, logLocation, outputLocation, name);
		p.organizations = NDLDataUtils.loadSet(orgfile);
		p.organizations1 = NDLDataUtils.loadSet(orgfile1);
		p.crosschecks = NDLDataUtils.loadSet(crosscheckfile);
		p.orgtokens = NDLDataUtils.loadSet(orgtokenfile);
		p.commaNames = NDLDataUtils.loadSet(commanamefile);
		p.wrongnames = NDLDataUtils.loadSet(wrongnamefile);
		p.namemapping = NDLDataUtils.loadKeyValue(namemapfile);
		p.addCSVLogger("modified.names", new String[]{"Handle_ID", "New_Authors"});
		p.addTextLogger("cross.check");
		p.turnOffLoadHierarchyFlag();
		p.correctData();
		
		System.out.println("Done.");
	}
}