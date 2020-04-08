package org.iitkgp.ndl.test.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.core.Group;
import org.iitkgp.ndl.data.DataType;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.stitch.AbstractNDLSIPStitchingContainer;
import org.iitkgp.ndl.data.correction.stitch.NDLStitchHierarchy;
import org.iitkgp.ndl.data.correction.stitch.NDLStitchHierarchyNode;
import org.iitkgp.ndl.util.NDLDataUtils;

// NPTEL SIP stitching
public class NPTELSIPStitching extends AbstractNDLSIPStitchingContainer {
	
	Map<Group, Set<String>> leafTitles = new HashMap<>();
	Map<String, String> ddc1 = null;
	Map<String, String> ddc2 = null;
	Map<String, String> assignmentsorder = null;
	
	Map<String, Map<String, Integer>> l2map = new HashMap<>();
	Set<String> l2singlet = null;
	Set<String> delete = null;
	Set<String> authors = null;
	
	Pattern TITLE_REGX1 = Pattern.compile("^.*_co_([0-9]+)_exercises$");
	Map<String, String> moreTitles = null;
	int mtmc = 0;
	
	//static Pattern LECT_PATTERN = Pattern.compile(".*_lec(t_)?[0-9]+_.*");

	public NPTELSIPStitching(String input, String logLocation, String outputLocation, String logicalName)
			throws Exception {
		super(input, logLocation, outputLocation, logicalName);
	}
	
	@Override
	protected String orphanNodesCauseDetailingTrack(SIPDataItem item) {
		String l1 = item.getSingleValue("dc.coverage.temporal");
		String l2 = item.getSingleValue("dc.title.alternative");
		String l3 = item.getSingleValue("dc.description.tableofcontents");
		List<String> cause = new ArrayList<>();
		if(StringUtils.isBlank(l1)) {
			cause.add("temporal is blank");
		}
		if(StringUtils.isBlank(l2)) {
			cause.add("title.alternative is blank");
		}
		if(StringUtils.isBlank(l3)) {
			cause.add("tableofcontents is blank");
		}
		return NDLDataUtils.join(cause, ',');
	}
	
	@Override
	protected boolean preStitchCorrection(SIPDataItem item) throws Exception {
		
		if(delete.contains(item.getSingleValue("dc.identifier.uri"))) {
			// delete
			return false;
		}
		
		item.move("dc.identifier.isbn", "dc.identifier.other:uniqueId");
		item.delete("dc.subject.prerequisitetopic");

		if(item.contains("lrmi.learningResourceType", "webCourse")) {
			item.delete("dc.language.iso");
		}

		if(item.containsByEndsWith("dc.identifier.uri", ".HTM")) {
			item.updateSingleValue("dc.format.mimetype", "text/html");
			item.updateSingleValue("dc.type", "text");
		}

		if(item.containsByRegex("dc.identifier.uri", ".*\\.htm(l)?\\#?.*")) {
			item.updateSingleValue("dc.format.mimetype", "text/html");
			item.updateSingleValue("dc.type", "text");
		}

		if(item.containsByEndsWith("dc.identifier.uri", ".swf")) {
			item.updateSingleValue("dc.type", "animation");
			item.updateSingleValue("dc.format.mimetype", "application/x-shockwave-flash");
		}
		
		if(authors.contains(NDLDataUtils.getHandleSuffixID(item.getId()))) {
			item.delete("dc.contributor.author");
			item.add("dc.contributor.author", "Pan, Chandra Subhas");
		}
		
		// remove invalid characters from title
		item.updateSingleValue("dc.title", NDLDataUtils.removeInvalidCharacters(item.getSingleValue("dc.title"), 65533));
		
		// more title modifications
		String id = NDLDataUtils.getHandleSuffixID(item.getId());
		Matcher m = TITLE_REGX1.matcher(id);
		if(m.find()) {
			item.updateSingleValue("dc.title", item.getSingleValue("dc.title") + " - " + m.group(1));
			mtmc++;
			log("more.title.modifications.log", id + " => " + item.getSingleValue("dc.title"));
		} else if(moreTitles.containsKey(id)) {
			// 1 to 1 mapping
			item.updateSingleValue("dc.title", moreTitles.get(id));
			mtmc++;
			log("more.title.modifications.log", id + " => " + item.getSingleValue("dc.title"));
		}
		
		return true;
	}
	
	@Override
	protected void postStitchCorrection(SIPDataItem item) throws Exception {
		
		// dc.coverage.temporal
		item.move("dc.coverage.temporal", "dc.subject");
		
		if(StringUtils.equals(item.getSingleValue("dc.description.tableofcontents"),
				item.getSingleValue("dc.title.alternative"))) {
			item.move("dc.title.alternative", "dc.subject");
		}
		
		if(item.contains("lrmi.learningResourceType", "webCourse") && item.exists("dc.subject")) {
			item.move("dc.title.alternative", "dc.subject");
		}
		
		item.delete("dc.title.alternative");
		item.delete("dc.description.tableofcontents");
		item.delete("dc.coverage.spatial");
		item.delete("lrmi.educationalAlignment.educationalFramework");
	}
	
	String findlect(String text) {
		int p = text.lastIndexOf("lec");
		if(p != -1) {
			int l = text.length();
			boolean numeric = false;
			StringBuilder lectn = new StringBuilder();
			for(int i = p + 3; i < l; i++) {
				char ch = text.charAt(i);
				if(CharUtils.isAsciiNumeric(ch)) {
					lectn.append(ch);
					numeric = true;
				} else if(numeric) {
					break;
				}
			}
			return lectn.toString();
		}
		return "";

	}
	
	String[] get2ndLevelTitleAndID(String t, String isbn) {
		String l2id = t;
		String l2title = t;
		if(StringUtils.isNotBlank(isbn)) {
			// f = true;
			l2id = t + '-' + isbn;
			Map<String, Integer> l2smap = l2map.get(t);
			if(l2smap == null) {
				l2smap = new HashMap<>(2);
				l2map.put(t, l2smap);
			}
			Integer i = l2smap.get(isbn);
			if(i == null) {
				i = l2smap.size() + 1;
				l2smap.put(isbn, i);
			}
			l2title = t + (l2singlet.contains(t) ? "" : (" - " + i)); // append index
		}
		return new String[]{l2id, l2title};
	}
	
	@Override
	protected NDLStitchHierarchy hierarchy(SIPDataItem item) throws Exception {
		
		// preserve old values
		String l1 = NDLDataUtils.removeInvalidCharacters(item.getSingleValue("dc.coverage.temporal"), 65533);
		String l2 = NDLDataUtils.removeInvalidCharacters(item.getSingleValue("dc.title.alternative"), 65533);
		String l3;
		String toc = NDLDataUtils.removeInvalidCharacters(item.getSingleValue("dc.description.tableofcontents"), 65533);
		
		// part of stitching
		if(NDLDataUtils.allNotBlank(l1, l2)) {
			
			boolean assignemnts = false; // assignments or web course
			boolean web = false;
			
			if(item.contains("lrmi.learningResourceType", "webCourse")) {
				// web course
				l3 = "Web-Course";
				web = true;
			} else if(StringUtils.isBlank(toc)) {
				// assignments
				l3 = "Assignments";
				assignemnts = true;
				// assumed assignments
				// title modification
				String t = item.getSingleValue("dc.title");
				int l = t.length();
				if(!CharUtils.isAsciiNumeric(t.charAt(l - 1))) {
					StringBuilder n = new StringBuilder();
					String id = item.getId();
					l = id.length();;
					for(int i =  l - 1; i >= 0; i--) {
						char ch = id.charAt(i);
						if(!CharUtils.isAsciiNumeric(ch)) {
							break;
						}
						n.append(ch);
					}
					if(n.length() > 0) {
						// title update
						item.updateSingleValue("dc.title", t + " - " + n.reverse());
					}
				}
			} else {
				// normal case
				l3 = toc;
			}
			
			boolean aw = assignemnts || web;
			
			// ready to stitch
			NDLStitchHierarchy h = new NDLStitchHierarchy();
			NDLStitchHierarchyNode root = new NDLStitchHierarchyNode("NPTEL", "NPTEL", true);
			root.addAdditionalData("sv", "false"); // search visibility
			h.add(root);
			
			NDLStitchHierarchyNode l1node = new NDLStitchHierarchyNode(l1, l1);
			l1node.addAdditionalData("sv", "true"); // search visibility
			String ddc1v = null;
			if(ddc1.containsKey(l1)) {
				ddc1v = ddc1.get(l1);
				l1node.addAdditionalData("ddc", ddc1v);
			} else {
				// cross check
				System.err.println("First level: " + l1 + " DDC missing.");
			}
			h.add(l1node);
			
			String isbn = item.getSingleValue("dc.identifier.other:uniqueId");
			
			boolean merge = false;
			String ddc2v = ddc2.get(l2); // 2nd level DDC
			if(StringUtils.isBlank(ddc2v)) {
				// 2nd level DDC inherit from 1st level
				ddc2v = ddc1v;
			}
			if(!aw && StringUtils.equalsIgnoreCase(l2, l3)) {
				// merge case
				merge = true;
			} else {
				// node creation
				String l2d[] = get2ndLevelTitleAndID(l2, isbn);
				String l2id = l2d[0];
				String l2title = l2d[1];
				
				/*if(StringUtils.equalsAny(item.getId(), "nptel/courses_106_105_106105078_web_lec1", "nptel/courses_106_105_106105077_lec1")) {
					System.err.println("1. " + item.getId() + " => " + l2 + " ISBN: " + isbn);
					System.err.println("1. " + l2id + " => " + l2title);
				}*/
				
				NDLStitchHierarchyNode l2node = new NDLStitchHierarchyNode(l2id, l2title);
				
				l2node.setOrder(String.valueOf((int)l2title.charAt(0))); // first letter sorting
				l2node.addAdditionalData("sv", "true"); // search visibility
				if(StringUtils.isNotBlank(ddc2v)) {
					l2node.addAdditionalData("ddc", ddc2v);
				}
				
				h.add(l2node);
			}
			
			String l3id;
			String l3title;
			if(merge) {
				String l3d[] = get2ndLevelTitleAndID(l3, isbn);
				l3id = l3d[0];
				l3title = l3d[1];
				/*if(StringUtils.equalsAny(item.getId(), "nptel/courses_106_105_106105078_web_lec1", "nptel/courses_106_105_106105077_lec1")) {
					System.err.println("2. " + item.getId() + " => " + l3 + " ISBN: " + isbn);
					System.err.println("2. " + l3id + " => " + l3title);
				}*/
			} else {
				l3id = l3;
				l3title = l3;
			}
			NDLStitchHierarchyNode l3node = new NDLStitchHierarchyNode(l3id, l3title);
			
			// 3rd level ordering
			if(web) {
				// web course
				l3node.setOrder(String.valueOf(Integer.MAX_VALUE));
			} else if(!merge) {
				// actual ordering info
				l3node.setOrder(NDLDataUtils.NVL(item.getSingleValue("dc.coverage.spatial"),
						String.valueOf(Integer.MAX_VALUE)));
			} else {
				// drop actual ordering instead alphabetic ordering
				l3node.setOrder(String.valueOf((int)l3.charAt(0))); // first letter sorting
			}
			if(aw) {
				// Assignments
				l3node.addAdditionalData("sv", "false"); // search visibility
			} else {
				l3node.addAdditionalData("sv", String.valueOf(merge)); // search visibility
			}
			if(StringUtils.isNotBlank(ddc2v)) {
				// 3rd level DDC inherit from 2nd level
				l3node.addAdditionalData("ddc", ddc2v);
			}
			h.add(l3node);
			
			// web course extra node
			if(web && StringUtils.isNotBlank(toc)) {
				String l4 = toc;
				NDLStitchHierarchyNode l4node = new NDLStitchHierarchyNode(l4, l4);
				l4node.addAdditionalData("sv", String.valueOf(!web)); // search visibility true for non-WEB
				if(StringUtils.isNotBlank(ddc2v)) {
					// 4th level DDC inherit from 2nd level
					l4node.addAdditionalData("ddc", ddc2v);
				}
				
				// 4th level ordering
				l4node.setOrder(NDLDataUtils.NVL(item.getSingleValue("dc.coverage.spatial"),
						String.valueOf(Integer.MAX_VALUE)));
				
				h.add(l4node);
			}
			
			// group by key
			Group g = new Group();
			g.add(l1);
			g.add(l2);
			g.add(l3);
			
			// duplicate leaf title track
			Set<String> leaves = leafTitles.get(g);
			if(leaves == null) {
				leaves = new HashSet<>(2);
				leafTitles.put(g, leaves);
			}
			String t = item.getSingleValue("dc.title");
			if(!leaves.add(t) || StringUtils.equalsIgnoreCase(l3, t)) {
				// duplicate title for leaves
				// append lec. information to leaf node
				
				String itemid = item.getId();
				String lectn = findlect(itemid);
				if(StringUtils.isNotBlank(lectn)) {
					String mt = t + " - Lec" + lectn;
					item.updateSingleValue("dc.title", mt);
				}
			}
			
			// leaf level DDC
			if(StringUtils.isNotBlank(ddc2v)) {
				addNodeValue(item, "dc.subject.ddc", ddc2v.split("\\|"));
			}
			
			return h;
		} else {
			// orphan nodes
			return null;
		}			
	}
	
	@Override
	protected String itemOrder(SIPDataItem item) {
		String id = NDLDataUtils.getHandleSuffixID(item.getId());
		if(assignmentsorder.containsKey(id)) {
			// assignments order
			return assignmentsorder.get(id);
		}
		return NDLDataUtils.NVL(item.getSingleValue("lrmi.educationalAlignment.educationalFramework"),
				String.valueOf(Integer.MAX_VALUE));
	}
	
	@Override
	protected void addIntermediateNodeMetadata(SIPDataItem item, Map<String, String> additionalData) throws Exception {
		// add meta-data for intermediate node
		if(additionalData.containsKey("sv")) {
			String sv = additionalData.get("sv");
			if(sv.equals("true")) {
				item.addIfNotContains("dc.type", "video", "text");
			}
			item.add("dc.description.searchVisibility", sv);
		} else {
			// cross check
			System.err.println("SV missing: " + item.getSingleValue("dc.title"));
		}
		if(additionalData.containsKey("ddc")) {
			// DDC available
			addNodeValue(item, "dc.subject.ddc", additionalData.get("ddc").split("\\|"));
			/*log("ddc.logger", "[" + item.getId() + "]" + item.getSingleValue("dc.title") + " => "
					+ item.getValue("dc.subject.ddc"));*/
		} else {
			// cross check
			System.err.println("DDC missing: " + item.getSingleValue("dc.title"));
		}
		
		// applicable for virtual node
		if(item.contains("dc.description.searchVisibility", "true")) {
			item.addIfNotContains("dc.type", "video", "text");
		}
	}
	
	/*public static void main1(String[] args) {
		String txt = "106106092_assignments_courses_106106092_downloads_co_12_exercises";
		Matcher m = TITLE_REGX1.matcher(txt);
		if(m.find()) {
			System.out.println(m.group(1));
		}
	}*/
	
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/NPTEL/in/2019.Nov.07.13.54.50.NPTL.full.merged.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/NPTEL/out";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/NPTEL/out";
		String logicalName = "nptel.combined.stitich";
		
		String fddc1 = "/home/dspace/debasis/NDL/NDL_sources/NPTEL/conf/1st.level.ddc.csv";
		String fddc2 = "/home/dspace/debasis/NDL/NDL_sources/NPTEL/conf/2nd.level.ddc.csv";
		String assignmentsOrderFile = "/home/dspace/debasis/NDL/NDL_sources/NPTEL/conf/assignmentOut.csv";
		String singletaf = "/home/dspace/debasis/NDL/NDL_sources/NPTEL/conf/single.ta";
		String deletef = "/home/dspace/debasis/NDL/NDL_sources/NPTEL/conf/NPTEL-web-delete-item";
		String authorsf = "/home/dspace/debasis/NDL/NDL_sources/NPTEL/conf/authors";
		String moreTitlesFile = "/home/dspace/debasis/NDL/NDL_sources/NPTEL/conf/modified.titles.csv";
		
		NPTELSIPStitching nptel = new NPTELSIPStitching(input, logLocation, outputLocation, logicalName);
		
		//nptel.addTextLogger("ddc.logger");
		
		nptel.ddc1 = NDLDataUtils.loadKeyValue(fddc1);
		nptel.ddc2 = NDLDataUtils.loadKeyValue(fddc2);
		nptel.assignmentsorder = NDLDataUtils.loadKeyValue(assignmentsOrderFile);
		nptel.l2singlet = NDLDataUtils.loadSet(singletaf);
		nptel.delete = NDLDataUtils.loadSet(deletef);
		nptel.authors = NDLDataUtils.loadSet(authorsf);
		nptel.moreTitles = NDLDataUtils.loadKeyValue(moreTitlesFile);
		
		nptel.turnOnLogRelationDetails();
		nptel.setLeafIsPartLogging(-1);
		nptel.turnOnOrphanNodesLogging();
		nptel.turnOnDuplicateHandlesChecking();
		//nptel.addLevelOrder(3, DataType.INTEGER);
		nptel.addLevelOrder(4, DataType.INTEGER);
		nptel.addLevelOrder(5, DataType.INTEGER);
		nptel.addLevelOrder(6, DataType.INTEGER);
		
		nptel.addTextLogger("more.title.modifications.log");
		
		nptel.addGlobalMetadata("dc.rights.accessRights", "open");
		//nptel.addGlobalMetadata("dc.description.searchVisibility", "false");
		nptel.addGlobalMetadata("lrmi.typicalAgeRange", "18-22", "22+");
		nptel.addGlobalMetadata("dc.language.iso", "eng");
		nptel.addGlobalMetadata("lrmi.educationalAlignment.educationalLevel", "ug_pg");
		nptel.addGlobalMetadata("lrmi.educationalUse", "research", "selfLearning", "assignment");
		
		// abbreviated handle id generation strategy
		nptel.setDefaultAbbreviatedHandleIDGenerationStrategy(2, 3, 4, 5);
		// stitch starts
		nptel.stitch();
		
		/*BufferedWriter w = new BufferedWriter(new FileWriter(new File(logLocation, "single.ta")));
		for(String t : nptel.l2map.keySet()) {
			if(nptel.l2map.get(t).size() == 1) {
				w.write(t);
				w.newLine();
			}
		}
		w.close();*/
		
		System.out.println("More title modifications: " + nptel.mtmc);
		
		System.out.println("Done.");
	}
}