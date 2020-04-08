package org.iitkgp.ndl.test.source;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.data.normalizer.NDLDataNormalizer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class ARXIVAuthorCorrection extends NDLSIPCorrectionContainer {
	
	//Set<String> filter = null;
	Set<String> wrongAuthors = null;
	Set<String> wrongTokens = new HashSet<String>();
	Map<String, String> author121 = null;
	Set<String> org = null;
	
	boolean isWrongToken(String token) {
		return NDLDataUtils.isWrongNameToken(token) || StringUtils.contains(token, "‡");
	}
	
	NDLDataNormalizer orgNormalizer = new NDLDataNormalizer() {
		
		@Override
		public Collection<String> transform(String input) {
			String value = null;
			input = input.replace("\"", "");
			String tokens[] = input.split(" *, *");
			if(tokens.length == 2) {
				value = tokens[1] + " " + tokens[0];
			} else {
				value = input;
			}
			char ch = value.charAt(0);
			if(CharUtils.isAsciiAlphaLower(ch)) {
				// init cap
				value = String.valueOf(ch).toUpperCase() + value.substring(1);
			}
			return NDLDataUtils.createNewList(value);
		}
	};
	
	/*NDLDataNormalizer normalizer = new NDLDataNormalizer() {
		
		@Override
		public Collection<String> transform(String input) {
			List<String> names = new LinkedList<String>();
			if(StringUtils.contains(input, " and ")) {
				String tokens[] = input.split(" +and +");
				names.add(normalizeName(tokens[0]));
				names.add(normalizeName(tokens[1]));
			} else if(StringUtils.contains(input, ",")) {
				String tokens[] = input.split(",");
				if(tokens.length > 2) {
					// expected 3
					int p = input.lastIndexOf(',');
					names.add(normalizeName(input.substring(0, p)));
					names.add(normalizeName(input.substring(p + 1)));
				} else {
					names.add(normalizeName(input));
				}
			}
			
			return names;
		}
	};*/
	
	List<String> splitAndNormlaize(String input) {
		List<String> names = new LinkedList<String>();
		if(StringUtils.contains(input, " and ")) {
			String tokens[] = input.split(" +and +");
			for(String t : tokens) {
				names.add(normalizeName(t));
			}
		} else if(StringUtils.contains(input, ",")) {
			String tokens[] = input.split(",");
			if(tokens.length > 2) {
				// expected 3
				int p = input.lastIndexOf(',');
				String second = input.substring(p + 1);
				if(!NDLDataUtils.isInitialLetter(second)) {
					// two names
					names.add(normalizeName(input.substring(0, p)));
					names.add(normalizeName(second));
				} else {
					// single name
					names.add(normalizeName(input.replace(",", "")));
				}
			} else {
				names.add(normalizeName(input));
			}
		} else {
			names.add(normalizeName(input));
		}
		return names;
	}
	
	String getModifiedName(String name) {
		if(NumberUtils.isDigits(name)) {
			// invalid
			return StringUtils.EMPTY;
		}
		name = name.replaceAll("\"|\\*|[0-9]+", "");
		int p = name.indexOf(',');
		if(p != -1) {
			String t = name.substring(0, p);
			// System.out.println(t + " => " + NDLDataUtils.isWrongNameToken(t));
			if(isWrongToken(t)) {
				name = name.replaceFirst("^.+,", "").trim();
			}
		} else if(isWrongToken(name.substring(0, 1))) {
			// first letter check
			name = name.substring(1).trim();
		}
		name = name.replaceFirst("^(and|al),", "").replaceFirst("et.?$", "")
				.replaceFirst("\\(.+\\)?$", "").replaceFirst("\\(.+\\)", "").trim();
		return name;
	}
	
	String normalizeName(String name) {
		//System.out.println(name);
		String modified;
		if(!name.contains(",")) {
			// need to normalize
			modified = NDLDataUtils.normalizeSimpleNameByWrongNameTokens(name, wrongTokens);
		} else {
			// place dot in case of single letter
			modified = NDLDataUtils.normalizeSimpleNameByWrongNameTokens(name, wrongTokens, false);
		}
		if(StringUtils.isBlank(modified)) {
			// cross check
			System.err.println("[" + getCurrentTargetItem().getId() + "]" + name + " => " + modified);
		}
		return modified;
	}
	
	// constructor
	public ARXIVAuthorCorrection(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
		wrongTokens.add("et");
		wrongTokens.add("al");
		wrongTokens.add("Mrs");
		wrongTokens.add("Dr");
		wrongTokens.add("Prof");
		wrongTokens.add("Mr");
		//wrongTokens.add("Jr");
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		// delete wrong authors
		moveIfContains("dc.contributor.author", "dc.contributor.other:organization", org);
		normalize("dc.contributor.other:organization");
		deleteIfContains("dc.contributor.author", wrongAuthors);
		transformAndNormalizeFieldByExactMatch("dc.contributor.author", author121, '|');
		// author normalization and merging
		List<NDLDataNode> nodes = target.getNodes("dc.contributor.author");
		/*if(target.getId().equals("arxiv/oai_arxiv_org_1711_05621")) {
			System.out.println(NDLDataUtils.join(target.getValue("dc.contributor.author"), '|'));
		}*/
		NDLDataNode prevnode = null;
		List<String> modifiedNames = new LinkedList<String>();
		for(NDLDataNode node : nodes) {
			String name = node.getTextContent();
			name = getModifiedName(name);
			/*if(target.getId().equals("arxiv/oai_arxiv_org_1711_05621")) {
				System.out.println(name);
			}*/
			node.setTextContent(name);
			/*if(StringUtils.isBlank(name)) {
				// delete node
				//node.remove();
			} else */
			if(NDLDataUtils.isInitialLetter(name)) {
				// merge
				if(prevnode != null) {
					name = prevnode.getTextContent() + ", " + name.replace(",", "");
					List<String> names = splitAndNormlaize(name);
					//prevnode.setTextContent(names.get(0));
					modifiedNames.add(names.get(0));
					if(names.size() > 1) {
						modifiedNames.addAll(names.subList(1, names.size()));
					}
					//prevnode.setTextContent(name);
					//node.remove();
					prevnode = null; // reset
				} else {
					List<String> names = splitAndNormlaize(name);
					//node.setTextContent(names.get(0));
					modifiedNames.add(names.get(0));
					if(names.size() > 1) {
						modifiedNames.addAll(names.subList(1, names.size()));
					}
					//node.setTextContent(name);
					prevnode = null; // reset
				}
			} else {
				if(prevnode != null) {
					// last node's normalization
					List<String> names = splitAndNormlaize(prevnode.getTextContent());
					//prevnode.setTextContent(names.get(0));
					modifiedNames.add(names.get(0));
					if(names.size() > 1) {
						modifiedNames.addAll(names.subList(1, names.size()));
					}
				}
				prevnode = node; // save last one
			}
		}
		if(prevnode != null) {
			// in case of last node left for normalization
			List<String> names = splitAndNormlaize(prevnode.getTextContent());
			// prevnode.setTextContent(names.get(0));
			modifiedNames.add(names.get(0));
			if(names.size() > 1) {
				modifiedNames.addAll(names.subList(1, names.size()));
			}
		}
		
		// delete and add names
		target.delete("dc.contributor.author");
		target.add("dc.contributor.author", modifiedNames);
		
		//normalize("dc.contributor.author");
		
		return true;
		//return filter.contains(target.getId());
	}
	
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/generated_xml_data/ARXIV/ARXIV-04.01.2019.tar.gz";
		String logLocation = "<not required>";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/ARXIV/out";
		String name = "arxiv.author.v2";
		
		String wrongAuthorsFile = "/home/dspace/debasis/NDL/generated_xml_data/ARXIV/conf/wrong.authors";
		String author121MappingFile = "/home/dspace/debasis/NDL/generated_xml_data/ARXIV/conf/author.121.csv";
		String orgFile = "/home/dspace/debasis/NDL/generated_xml_data/ARXIV/conf/org.data";
		//String filterFile = "/home/dspace/debasis/NDL/generated_xml_data/ARXIV/conf/data.filter";
		
		ARXIVAuthorCorrection p = new ARXIVAuthorCorrection(input, logLocation, outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		//p.addNormalizer("dc.contributor.author", p.normalizer);
		//p.filter = NDLDataUtils.loadSet(filterFile);
		p.wrongAuthors = NDLDataUtils.loadSet(wrongAuthorsFile);
		p.author121 = NDLDataUtils.loadKeyValue(author121MappingFile);
		p.addMappingResource(author121MappingFile, "Old", "author121");
		//p.addMappingResource(orgFile, "org");
		p.org = NDLDataUtils.loadSet(orgFile);
		p.addNormalizer("dc.contributor.other:organization", p.orgNormalizer);
		p.correctData();
		
		/*String names[] = { "NASA/GSFC), R. H. D. Corbet", "(CfA), J. Galache", "Univ), V. A. McBride (Southampton",
				"Univ), L. J. Townsend (Southampton", "Obs), A. Udalski (Warsaw" };
		for(String n : names) {
			System.out.println(n + " ======> " + p.getModifiedName(n));
		}*/
		// System.out.println(NDLDataUtils.normalizeSimpleName("Amaryan, 18 M. J."));
		//System.out.println((int)'‡');
		//System.out.println(p.getModifiedName("De Rydt, Jan"));
		//System.out.println(p.isSingleLetter("Dunlop"));
		//System.out.println(p.isSingleLetter("J."));
		// System.out.println("Gottl\"ober, S.".contains("\""));
		
		System.out.println("Done.");
	}
}