package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.converter.NDLSIP2CSVConverter;
import org.iitkgp.ndl.data.Filter;
import org.iitkgp.ndl.data.SIPDataItem;

// test of NDLSIP2CSVConverter
public class SIP2CSVConverterTest {
	
	// TEST
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/RRB-GD/2020.Feb.04.14.44.59.RRB_GDV2.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/RRB-GD";
		
		/*String idFile = "/home/dspace/debasis/NDL/NDL_sources/KH/conf/check.data";
		Set<String> ids = NDLDataUtils.loadSet(idFile);*/

		/*String handleFile = "/home/dspace/debasis/NDL/generated_xml_data/ARXIV/conf/data.filter";
		Set<String> handles = NDLDataUtils.loadSet(handleFile);*/
		
		//Set<String> educations = new HashSet<String>();
		/*String pfile = "/home/dspace/debasis/NDL/NDL_sources/RAJ/conf/parent.data";
		Set<String> parents = NDLDataUtils.loadSet(pfile);*/
		
		//NDLConfigurationContext.addConfiguration("compressed.data.process.buffer.size", "2");
		
		//NDLConfigurationContext.addConfiguration("sip.bypass.unknown.xml.file", "true");
		
		NDLSIP2CSVConverter converter = new NDLSIP2CSVConverter(input, logLocation, "RRB-GD.stitch", false);
		converter.setCsvThresholdLimit(50000);
		converter.setMultivalueSeparator('|');
		/*converter.addColumnSelector("ndl.sourceMeta.additionalInfo", "Author", new Transformer<String, String>() {
			
			@Override
			public Collection<String> transform(String input) {
				NDLJSONParser parser = new NDLJSONParser(input);
				try {
					String name = parser.getText("authorInfo.name");
					if(StringUtils.isNotBlank(name)) {
						List<String> names = new LinkedList<>();
						names.add(name);
						return names;
					}
				} catch(InvalidJSONExpressionException ex) {
					// expression not found
					return new LinkedList<String>();// empty list
				}
				return new LinkedList<String>();// empty list
			}
		});*/
		
		converter.addColumnSelector("dc.title.alternative", "Title.alternative");
		converter.addColumnSelector("ndl.learningResourceType", "LRT");
		converter.addColumnSelector("ndl.relation.isPartOf", "Parent1");
		converter.addColumnSelector("ndl.relation.solutionOf", "Parent2");
		converter.addColumnSelector("ndl.examination.sequence", "O1");
		converter.addColumnSelector("ndl.questionPaperPart.sequence", "O2");
		converter.addColumnSelector("ndl.question.sequence", "O3");
		converter.addColumnSelector("ndl.solution.type", "O4");
		
		/*converter.addExcludeColumn("dc.type", "dc.description.searchVisibility", "dc.rights.accessRights",
				"dc.identifier.uri", "dc.source", "dc.source.uri", "dc.language.iso");*/
		/*converter.addColumnSelector("dc.title", "Title");
		converter.addColumnSelector("dc.title.alternative", "Title.alternative");
		converter.addColumnSelector("dc.language.iso", "Language");
		converter.addColumnSelector("dc.identifier.other", "Identifier.other");
		converter.addColumnSelector("dc.format.extent", "Format.extent");
		converter.addColumnSelector("lrmi.learningResourceType", "LRT");
		converter.addColumnSelector("dc.description", "Description");
		converter.addColumnSelector("dc.contributor.author", "Author");
		converter.addColumnSelector("dc.contributor.editor", "Editor");
		converter.addColumnSelector("dc.contributor.other", "Contributor.other");
		converter.addColumnSelector("dc.date.other", "Date.other");
		converter.addColumnSelector("dc.publisher.date", "Publisher.date");
		converter.addColumnSelector("dc.coverage.temporal", "Temporal");
		converter.addColumnSelector("dc.subject", "Subjects");
		converter.addColumnSelector("ndl.sourceMeta.additionalInfo", "Additional.info");*/
		/*converter.addColumnSelector("dc.identifier.other:journal", "Journal");*/
		//converter.addColumnSelector("dc.identifier.other:pmId", "PMID");
		/*converter.addColumnSelector("dc.description", "Description");
		converter.addColumnSelector("dc.coverage.spatial", "Spatial");
		converter.addColumnSelector("dc.subject", "keywords");*/
		/*converter.addColumnSelector("dc.publisher.place", "Place");
		converter.addColumnSelector("dc.publisher.institution", "Institute");
		converter.addColumnSelector("dc.publisher", "Publisher");
		converter.addColumnSelector("dc.description", "Description");*/
		/*converter.addColumnSelector("dc.contributor.author", "Author_Wrong", new Transformer<String, String>() {
			
			@Override
			public Collection<String> transform(String input) {
				List<String> o = new LinkedList<String>();
				int l = input.length();
				if(l < 5 || l > 50) {
					o.add(input);
				}
				return o;
			}
		});*/
		//converter.addColumnSelector("dc.contributor.other:organization", "Organization");
		/*converter.addColumnSelector("dc.format.extent:startingPage", "Starting-Page");
		converter.addColumnSelector("dc.format.extent:endingPage", "Ending-Page");
		converter.addColumnSelector("dc.format.extent:pageCount", "Page-Count");
		converter.addColumnSelector("dc.format.extent:size_in_Bytes", "Size");
		converter.addColumnSelector("lrmi.learningResourceType", "LRT");
		converter.addColumnSelector("dc.description", "Description");
		converter.addColumnSelector("dc.language.iso", "Language");
		converter.addColumnSelector("dc.type", "Type");
		converter.addColumnSelector("dc.subject", "keywords");
		converter.addColumnSelector("dc.creator.researcher", "Researcher");
		converter.addColumnSelector("dc.rights.license", "License");*/
		
		/*converter.addColumnSelector("dc.identifier.other:journal", "Journal");
		converter.addColumnSelector("dc.identifier.other:volume", "Volume");
		converter.addColumnSelector("dc.identifier.other:issue", "Issue");
		converter.addColumnSelector("dc.identifier.other:doi", "DOI");
		converter.addColumnSelector("dc.description.sponsorship", "Sponsorship");*/
		//converter.addExcludeColumn("dc.relation.haspart", "dc.relation.ispartof");
		//converter.addColumnSelector("ndl.sourceMeta.additionalInfo:note", "note");
		/*converter.addColumnSelector("dc.date.copyright", "copyright");
		converter.addColumnSelector("dc.description.searchVisibility", "visibility");
		converter.addColumnSelector("dc.subject.ddc", "DDC");
		converter.addColumnSelector("dc.subject", "Keywords");
		converter.addColumnSelector("dc.identifier.other:doi", "DOI");
		converter.addColumnSelector("dc.rights.accessRights", "Rights");
		converter.addColumnSelector("dc.identifier.other:journal", "Journal");
		converter.addColumnSelector("dc.identifier.other:volume", "Volume");
		converter.addColumnSelector("dc.identifier.other:issue", "Issue");
		converter.addColumnSelector("dc.identifier.other:itemId", "ItemID");
		converter.addColumnSelector("dc.contributor.advisor", "Advisor");
		converter.addColumnSelector("ndl.sourceMeta.uniqueInfo", "UniqueInfo");
		converter.addColumnSelector("dc.publisher.institution", "Institute");
		converter.addColumnSelector("dc.relation.haspart", "Children");
		converter.addColumnSelector("dc.relation.ispartof", "Parent");*/
		
		//converter.setCsvThresholdLimit(50000);
		// converter.turnOnAllowAllBlankFields();
		//converter.addExcludeColumn("dc.relation.haspart", "dc.relation.ispartof");
		
		/*converter.addDataFilter(new Filter<SIPDataItem>() {
			
			@Override
			public boolean filter(SIPDataItem data) {
				Map<String, Collection<String>> values = data.getAllValues();
				for(String k : values.keySet()) {
					Collection<String> v = values.get(k);
					for(String d : v) {
						// condition
						if(condition) {
							return true;
						}
					}
				}
				return false;
				return data.exists("dc.relation.haspart");
				//!data.containsByRegex("dc.title", "^Videos:.*")
				boolean f = data.contains("dc.type", "video", "simulation", "audio")
						&& !parents.contains(NDLDataUtils.getHandleSuffixID(data.getId()));
				if(f) {
					educations.add(data.getSingleValue(""));
				}
				return f;
				if(data.exists("dc.description.abstract")) {
					int l = data.getSingleValue("dc.description.abstract").length();
					return l < 100 || l > 5000; // limit violation
				} else {
					 return false;
				}
			}
		});*/
		
		/*converter.addDataFilter(new Filter<SIPDataItem>() {
			
			@Override
			public boolean filter(SIPDataItem data) {
				String j = data.getSingleValue("dc.identifier.other:journal");
				String v = data.getSingleValue("dc.identifier.other:volume");
				String i = data.getSingleValue("dc.identifier.other:issue");
				
				if(data.getId().equals("omics/_jurnalul_de_chirurgie_1584_9341")) {
					System.err.println(j);
					System.err.println(v);
					System.err.println(i);
					// System.err.println(StringUtils.isBlank(j));
				}
				
				return !data.contains("lrmi.learningResourceType", "journal")
						&& (StringUtils.isBlank(j) || StringUtils.isBlank(v) || StringUtils.isBlank(i));
			}
		});*/

		/*converter.addColumnSelector("dc.title", "Title");
		converter.addColumnSelector("dc.identifier.other:journal", "Journal");
		converter.addColumnSelector("dc.identifier.other:volume", "Volume");
		converter.addColumnSelector("dc.identifier.other:issue", "Issue");*/
		/*converter.addColumnSelector("dc.contributor.author", "Author");
		converter.addColumnSelector("dc.contributor.other:organization", "Organization");*/
		//converter.addColumnSelector("dc.identifier.other:part", "Part");
		// converter.addColumnSelector("dc.identifier.citation", "Citation");
		/*converter.addColumnSelector("dc.title", "Title");
		converter.addColumnSelector("dc.title.alternative", "Title Alternative");
		converter.addColumnSelector("dc.description", "Description");
		converter.addColumnSelector("dc.description.abstract", "Description Abstract");
		converter.addColumnSelector("dc.publisher", "Publisher");
		converter.addColumnSelector("dc.publisher.place", "Publisher Place");
		converter.addColumnSelector("dc.subject", "Subject");
		converter.addColumnSelector("dc.contributor.author", "Author");
		converter.addColumnSelector("dc.contributor.editor", "Editor");
		converter.addColumnSelector("dc.date.awarded", "Date Awarded");
		converter.addColumnSelector("dc.publisher.date", "Publisher Date");
		converter.addColumnSelector("dc.date.created", "Created");
		converter.addColumnSelector("dc.identifier.issn", "ISSN");
		converter.addColumnSelector("dc.identifier.isbn", "ISBN");
		converter.addColumnSelector("dc.identifier.other:eisbn", "EISBN");
		converter.addColumnSelector("dc.identifier.other:doi", "DOI");
		converter.addColumnSelector("ndl.sourceMeta.translation", "Translation");
		converter.addColumnSelector("dc.description.searchVisibility", "Visibility");*/
		//converter.addColumnSelector("lrmi.educationalUse", "Education");
		/*converter.addColumnSelector("dc.relation.references", "References");
		converter.addColumnSelector("dc.description.abstract", "Abstract");
		converter.addColumnSelector("lrmi.educationalAlignment.educationalFramework", "Educations");*/
		
		/*converter.addColumnSelector("dc.description.uri", "Description", new Transformer<String, String>() {
			
			@Override
			public Collection<String> transform(String input) {
				Map<String, String> map = NDLDataUtils.mapFromJson(input);
				return NDLDataUtils.createList(map.get("description"));
			}
		});*/
		
		//converter.addColumnSelector("dc.contributor.advisor", "Advisor");
		/*converter.addColumnSelector("dc.rights.accessRights", "Rights");
		converter.addColumnSelector("dc.language.iso", "Lang");
		converter.addColumnSelector("lrmi.learningResourceType", "LRT");
		converter.addColumnSelector("dc.date.awaconverter.addColumnSelector("dc.contributor.author", "Author");rded", "Awarded");
		converter.addColumnSelector("dc.date.created", "Date");*/
		//converter.addColumnSelector("dc.description", "Desc");
		//converter.addColumnSelector("dc.subject.ddc", "DDC");
		/*converter.addColumnSelector("dc.rights.license", "License");
		converter.addColumnSelector("dc.rights.uri", "Rights");*/
		//converter.addColumnSelector("lrmi.learningResourceType", "LRT");
		/*converter.addColumnSelector("dc.title", "Title");
		converter.addColumnSelector("dc.description.abstract", "Abstract");*/
		/*converter.addColumnSelector("dc.description.sponsorship", "Sponsorship", new Transformer<String, String>() {
			
			@Override
			public Collection<String> transform(String input) {
				if(input.contains("@")) {
					return NDLDataUtils.createEmtyList();
				} else {
					return NDLDataUtils.createNewList(input);
				}
				return NDLDataUtils.createNewList(input);
			}
		});*/
		//converter.addColumnSelector("ndl.sourceMeta.additionalInfo:RightsStatement", "Rights");
		
		/*converter.addColumnSelector("dc.contributor.author", "Author");
		converter.addColumnSelector("dc.contributor.other:organization", "Organization");*/
		
		/*converter.addColumnSelector("dc.identifier.citation", "Pagination", new Transformer<String, String>() {
			
			@Override
			public Collection<String> transform(String input) {
				StringTokenizer tokens = new StringTokenizer(input);
				String pages = null;
				while(tokens.hasMoreTokens()) {
					String t = tokens.nextToken();
					if(t.matches("^[0-9]+-[0-9]+\\.$")) {
						// latest
						pages = t;
					}
				}
				return NDLDataUtils.createNewList(pages);
			}
		});*/

		/*converter.addColumnSelector("dc.description", "Description");
		converter.addColumnSelector("dc.language.iso", "Language");
		converter.addColumnSelector("ndl.sourceMeta.translation", "Translation");*/
		
		converter.addDataFilter(new Filter<SIPDataItem>() {
			
			@Override
			public boolean filter(SIPDataItem data) {
				/*String v = data.getSingleValue("dc.description.sponsorship");
				if(StringUtils.isNotBlank(v)) {
					boolean f = !v.contains("-") && !v.contains("p.");
					if(f) {
						// more filtering
						String tokens[] = v.split(" +");
						return tokens.length > 3;
					} else {
						return f;
					}
				} else {
					return false;
				}
				List<String> names = data.getValue("dc.contributor.author");
				for(String name : names) {
					int l = name.length();
					if(l < 5 || l > 50) {
						return true;
					}
				}
				return false;*/
				//return ids.contains(NDLDataUtils.getHandleSuffixID(data.getId()));
				//return true; //all pass
				/*List<NDLDataNode> inodes = data.getNodes("ndl.sourceMeta.additionalInfo");
				for(NDLDataNode inode : inodes) {
					String atxt = inode.getTextContent();
					NDLJSONParser parser = new NDLJSONParser(atxt);
					try {
						String name = parser.getText("authorInfo.name");
						if(StringUtils.isNotBlank(name)) {
							String tokens[] = name.split(" +");
							for(String t : tokens) {
								if(StringUtils.equalsIgnoreCase(t, "and")) {
									// contains and token
									return true;
								}
							}
						}
					} catch(InvalidJSONExpressionException ex) {
						// suppress error
					}
				}
				return false;*/
				
				/*return data.containsByRegex("dc.title", "^.*\\$.*$")
						|| data.contains("dc.description", "^.*\\$.*$")
								|| data.contains("dc.coverage.spatial", "^.*\\$.*$")
						|| data.contains("dc.subject", "^.*\\$.*$");*/
				//return !data.exists("lrmi.learningResourceType");
				return true;
			}
		});
		
		converter.convert();
		
		System.out.println("Done.");
	}
}