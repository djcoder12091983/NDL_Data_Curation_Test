package org.iitkgp.ndl.test.source;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.ConfigurationData;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.relation.HasPart;
import org.iitkgp.ndl.relation.IsPartOf;
import org.iitkgp.ndl.util.NDLDataUtils;

// JCB curation
public class EuclidCuration extends NDLSIPCorrectionContainer {
	
	static final Pattern FILE_SIZE_REGEX = Pattern.compile("([0-9]+) (kb|mb)");
	static final Pattern INSTITUTE_PATTERN_REGX = Pattern.compile(".*(\\(.+\\)).*");
	static final Pattern YEAR_PATTERN_REGX = Pattern.compile(".*([0-9]{4}).*");
	static final String HANDLE_PREFIX = "projecteuclid/"; 
	
	Map<String, String> subjectsDDCMapping = null;
	//Map<String, String> booksTitleMapping = null;
	Set<String> pubids = null;

	public EuclidCuration(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	// journals extract
	Map<String, String> jextract(String text) {
		Map<String, String> map = new HashMap<String, String>(4);
		String tokens[] = text.split(" *, *");
		for(String token : tokens) {
			token = token.replace(".", "");
			String tokens1[] = token.split(" +");
			if(StringUtils.startsWithIgnoreCase(tokens1[0], "vol")) {
				// volume
				if(NumberUtils.isDigits(tokens1[1])) {
					map.put("vol", tokens1[1]);
				}
			} else if(StringUtils.containsIgnoreCase(tokens1[0], "number")
					|| StringUtils.containsIgnoreCase(tokens1[0], "issue")) {
				// issue
				if(NumberUtils.isDigits(tokens1[1])) {
					map.put("no", tokens1[1]);
				}
			} else if(StringUtils.containsIgnoreCase(token, "pages") || StringUtils.containsIgnoreCase(token, "pp")
					|| token.matches("[0-9A-Z]+(-[0-9A-Z]+)?")) {
				// pagination
				map.put("page", token);
			} else if(StringUtils.containsIgnoreCase(tokens1[0], "article")) {
				// article ID
				map.put("article-id", tokens1[2]);
			}
			
			if(tokens1.length == 3 && tokens1[2].matches("\\([0-9]{4}\\)")) {
				// year
				String y = tokens1[2].replaceAll("\\(|\\)", "");
				if(NumberUtils.isDigits(y)) {
					map.put("year", y);
				}
			}
		}
		return map;
	}
	
	// add pagination
	static void addPages(SIPDataItem target, String pages) {
		String t[] = pages.split(" +");
		if(t.length == 2) {
			// page count
			target.add("dc.format.extent:pageCount", String.valueOf(t[0]));
		} else {
			String tokens[] = pages.split("-");
			if(tokens.length == 2) {
				long pc = -1;
				if(NumberUtils.isDigits(tokens[0]) && NumberUtils.isDigits(tokens[1])) {
					pc = Long.parseLong(tokens[1]) - Long.parseLong(tokens[0]);
				} else if(NDLDataUtils.isRoman(tokens[0]) && NDLDataUtils.isRoman(tokens[1])) {
					pc = NDLDataUtils.romanToDecimal(tokens[1]) - NDLDataUtils.romanToDecimal(tokens[0]);
				}
				//System.out.println(pc);
				if(pc < 0) {
					// cross check
					System.err.println("[WARN] " + pages);
				} else {
					target.add("dc.format.extent:startingPage", tokens[0]);
					target.add("dc.format.extent:endingPage", tokens[1]);
					target.add("dc.format.extent:pageCount", String.valueOf(pc + 1));
				}
			} /*else if(NumberUtils.isDigits(tokens[0])){
				// page count
				target.add("dc.format.extent:pageCount", tokens[0]);
			}*/
		}
	}
	
	// extracts subjects/keywords
	static List<String> kextract(String subject) {
		List<String> subjects = new ArrayList<String>(4);
		String tokens[] = subject.split(":| +");
		boolean flag = false;
		boolean flag1 = false;
		StringBuilder t = new StringBuilder();
		int l = tokens.length;
		String code = null;
		for(int i = 0; i < l; i++) {
			String token = tokens[i];
			if ((i == 0 && (StringUtils.equalsIgnoreCase(token, "primary")
					|| StringUtils.equalsIgnoreCase(token, "secondary"))) || StringUtils.isBlank(token)) {
				// wrong tokens
				continue;
			}
			if(!flag && token.matches("[0-9]{2}[A-Z][0-9]{2}")) {
				// code
				if(i + 1 < l) {
					String next = tokens[i + 1];
					if(next.matches("[0-9]{2}[A-Z][0-9]{2}")) {
						// next token is also a code then store previous code as subject
						subjects.add(token);
					}
				}
				flag1 = true;
			} else if(token.startsWith("[") || token.startsWith("{")) {
				// wrong token starts
				flag = true;
				flag1 = true;
			} else if(token.endsWith("]") || token.endsWith("}")) {
				// reset flag
				flag = false;
			} else if(!flag1 && !flag) {
				t.append(token).append(" ");
			}
			if(flag1) {
				// reset
				flag1 = false;
				if(StringUtils.isNotBlank(code)) {
					if(t.length() > 0) {
						t.append('(').append(code).append(')');
					} else {
						t.append(code);
					}
				}
				if(t.length() > 0) {
					subjects.add(t.toString().trim());
					// reset
					t = new StringBuilder();
				}
				
				if(token.length() > 1 && NumberUtils.isDigits(token.substring(0, 2))) {
					code = token; // code
				} else {
					// reset code
					code = null;
				}
			}
		}
		// last token
		if(StringUtils.isNotBlank(code)) {
			if(t.length() > 0) {
				t.append('(').append(code).append(')');
			} else {
				t.append(code);
			}
		}
		if(t.length() > 0) {
			subjects.add(t.toString().trim());
		}
		return subjects;
	}
	
	// proceeding pagination extract
	static String[] pextract(String value) {
		int p = value.lastIndexOf(',');
		if(p != -1) {
			String t = value.substring(p + 1).toLowerCase();
			boolean pc = false;
			if(t.contains("p")) {
				pc = true;
			}
			String last = t.replaceAll("( +)|\\.|(p+)", "");
			//System.out.println(last);
			if(last.matches("[0-9]+(-[0-9]+)?") || last.matches("[ixvIXV]+(-[ixvIXV]+)?")) {
				return new String[]{value.substring(0, p), last + (pc ? " pp" : "")};
			} else {
				// can't extract
				return null;
			}
		} else {
			// can't extract
			return null;
		}
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		String id = NDLDataUtils.getHandleSuffixID(target.getId()); // id
		
		add("dc.type", "text");
		normalize("dc.date.copyright");
		target.add("dc.description.searchVisibility", "true");
		add("dc.subject.ddc", "510");
		target.replaceByRegex("dc.identifier.other:doi", "(?i)^(doi: *)*[^0-9]*", "");
		
		if(id.equals("1504144835") || id.equals("1504922733")) {
			target.delete("dc.identifier.other:doi");
		}
		
		target.moveByRegex("dc.rights.holder", "dc.rights.license", ".*Creative Commons.*");
		
		// DOI modification
		deleteIfContainsByRegex("dc.identifier.other:doi", "^0");
		
		// access rights
		if(target.contains("dc.rights.uri", "icon-access-closed") || target.contains("dc.rights.uri", "icon-access-open")) {
			if(!target.exists("dc.rights.accessRights")) {
				// unassigned
				target.updateSingleValue("dc.rights.accessRights", "subscribed");
			} else {
				String rights = target.getSingleValue("dc.rights.accessRights");
				if(StringUtils.containsIgnoreCase(rights, "pdf")) {
					target.updateSingleValue("dc.format.mimetype", "application/pdf");
				}
				Matcher m = FILE_SIZE_REGEX.matcher(rights);
				if(m.find()) {
					String size = m.group(1);
					String type = m.group(2);
					if(StringUtils.equalsIgnoreCase(type, "kb")) {
						target.add("dc.format.extent:size_in_Bytes",
								String.valueOf(Long.parseLong(size) * 1024)); 
					} else if(StringUtils.equalsIgnoreCase(type, "mb")) {
						target.add("dc.format.extent:size_in_Bytes",
								String.valueOf(Long.parseLong(size) * 1024 * 1024));
					}
				}
				if(StringUtils.containsIgnoreCase(rights, "Access denied")
						|| StringUtils.containsIgnoreCase(rights, "JSTOR subscribers")
						|| StringUtils.containsIgnoreCase(rights, "not supplied")
						|| StringUtils.containsIgnoreCase(rights, "KHARAGPUR")) {
					// subscribed
					target.updateSingleValue("dc.rights.accessRights", "subscribed");
				} else if(StringUtils.containsIgnoreCase(rights, "Open access")) {
					target.updateSingleValue("dc.rights.accessRights", "open");
				} else {
					target.updateSingleValue("dc.rights.accessRights", "open");
				}
			}
		}
		
		// wrong publisher delete
		deleteIfContainsByRegex("dc.publisher", "^(v|V)ol ");
		
		// keywords
		List<String> subject = target.getValue("dc.subject.other:callNumberDDC");
		Set<String> subjects = new HashSet<String>(4);
		for(String sub : subject) {
			// map from digits
			for(String s : kextract(sub)) {
				String tokens[] = s.split(" +");
				boolean f = true;
				Set<String> digits = new HashSet<String>(2);
				for(String t : tokens) {
					t = t.replaceFirst("\\(|\\)", "");
					String digit = t.length() > 1 ? t.substring(0, 2) : "";
					if(NumberUtils.isDigits(digit)) {
						digits.add(digit);
					} else {
						f = false;
						break;
					}
				}
				if(f) {
					for(String d : digits) {
						if(subjectsDDCMapping.containsKey(d)) {
							subjects.add(subjectsDDCMapping.get(d));
						} else { 
							subjects.add(d);
						}
					}
				} else {
					subjects.add(s);
				}
			}
		}
		target.add("dc.subject", subjects);
		
		// journal detail
		if(target.contains("lrmi.learningResourceType", "Journal article")) {
			List<String> publisher = target.getValue("dc.publisher");
			String detail;
			if(!target.exists("dc.identifier.other:journal")) {
				target.add("dc.identifier.other:journal", publisher.get(0));
				detail = publisher.get(1);
			} else {
				detail = publisher.get(0);
			}
			Map<String, String> map = jextract(detail);
			if(!target.exists("dc.publisher.date")) {
				add("dc.publisher.date", map.get("year"));
			}
			target.add("dc.identifier.other:volume", map.get("vol"));
			target.add("dc.identifier.other:issue", map.get("no"));
			target.add("dc.identifier.other:accessionNo", map.get("article-id"));
			String pages = map.get("page");
			if(StringUtils.isNotBlank(pages)) {
				addPages(target, pages);
			}
			
			// delete
			delete("dc.publisher", "dc.publisher.institution");
		}
		
		// proceedings pagination
		if(target.contains("lrmi.learningResourceType", "Proceeding")) {
			// delete journal
			delete("dc.identifier.other:journal");
			List<String> publisher = target.getValue("dc.publisher");
			if(publisher.size() == 2) {
				String pub = publisher.get(1);
				String values[] = pextract(pub);
				if(values != null) {
					target.updateNodeValue("dc.publisher", values[0], 2);
					addPages(target, values[1]);
				}
			}  else {
				String pub = target.getSingleValue("dc.publisher.institution");
				String values[] = pextract(pub);
				if(values != null) {
					target.updateSingleValue("dc.publisher.institution", values[0]);
					addPages(target, values[1]);
				}
			}
			
			updateInstitute(target); // handle institute
		}
		
		// delete publisher/institution
		String institue = target.getSingleValue("dc.publisher.institution");
		List<String> publisher = target.getValue("dc.publisher");
		if(!publisher.isEmpty()) {
			for(String p : publisher) {
				if(checkduplicate(p, institue)) {
					// duplicate values then delete institution
					delete("dc.publisher.institution");
					break;
				}
			}
		}
		
		// RightsStatement publication date
		List<String> rights = target.getValue("ndl.sourceMeta.additionalInfo:RightsStatement");
		for(String r : rights) {
			if(StringUtils.startsWithIgnoreCase(r, "Publication date:")) {
				if(!target.exists("dc.publisher.date")) {
					add("dc.publisher.date", r.split(":")[1]);
				}
			}
		}
		// note
		/*String note = target.getSingleValue("ndl.sourceMeta.additionalInfo:note");
		if(StringUtils.containsIgnoreCase(note, "pp")) {
			// pagination
			target.add("dc.format.extent:pageCount", note.split(" +")[0]);
			delete("ndl.sourceMeta.additionalInfo:note");
		}*/
		
		delete("dc.identifier.isbn");
		List<NDLDataNode> tnodes = target.getNodes("dc.coverage.temporal");
		for(NDLDataNode node : tnodes) {
			String value = node.getTextContent().replace("-", "");
			int l = value.length();
			if(l == 10 || l == 13) {
				// move to ISBN
				if(!target.exists("dc.identifier.isbn")) {
					target.add("dc.identifier.isbn", value);
				}
			}
			
			node.remove(); // delete
		}
		
		// delete fields
		delete("dc.rights.uri",
				"dc.contributor.illustrator",
				"ndl.sourceMeta.additionalInfo:RightsStatement",
				"dc.subject.other:callNumberDDC",
				"dc.contributor.other:bookCoordinator",
				"dc.contributor.other:owner",
				"dc.description.uri",
				"ndl.sourceMeta.additionalInfo:note",
				"ndl.sourceMeta.additionalInfo:thumbnail");
		
		// issn
		String issn = target.getSingleValue("dc.identifier.issn");
		if(StringUtils.isNotBlank(issn)) {
			if(!StringUtils.equals(issn, "0") && !StringUtils.equalsIgnoreCase(issn, "number")) {
				target.add("ndl.sourceMeta.uniqueInfo", NDLDataUtils
						.getUniqueInfoJSON("mathematical_reviews_number_mathscinet", "MR" + issn.replaceAll(" +", "")));
			}
			delete("dc.identifier.issn");
		}
		String itemid = target.getSingleValue("dc.identifier.other:itemId");
		if(StringUtils.isNotBlank(itemid)) {
			target.add("ndl.sourceMeta.uniqueInfo",
					NDLDataUtils.getUniqueInfoJSON("zentralblatt_math_identifier", itemid));
			delete("dc.identifier.other:itemId");
		}
		List<NDLDataNode> accessions = target.getNodes("dc.identifier.other:accessionNo");
		for(NDLDataNode accession : accessions) {
			String acc = NDLDataUtils.getValueByJsonKey(accession.getTextContent(), "accessionNo");
			if(StringUtils.startsWithIgnoreCase(acc, "MR")) {
				if(!StringUtils.equals(acc, "0")) {
					target.add("ndl.sourceMeta.uniqueInfo",
							NDLDataUtils.getUniqueInfoJSON("mathematical_reviews_number_mathscinet", acc));
				}
			} else {
				target.add("dc.identifier.other:itemId", acc);
			}
			// delete
			accession.remove();
		}
		move("dc.subject.other:jel", "dc.description");
		
		// books case
		boolean book = target.contains("lrmi.learningResourceType", "Book");
		Set<String> deleteAuthors = null;
		if(book) {
			move("dc.contributor.other:commentator", "dc.contributor.editor");
			// stitching
			List<String> chapters = target.getValue("lrmi.educationalRole");
			List<HasPart> hparts = new ArrayList<HasPart>(chapters.size());
			deleteAuthors = new HashSet<String>(2);
			for(String chapter : chapters) {
				int idx = chapter.lastIndexOf('/');
				String cid = chapter.substring(idx + 1);
				String key = "books_child." + ConfigurationData.escapeDot(cid);
				if(!containsMappingKey(key)) {
					// cross check
					//throw new IllegalStateException("book chpater(" + target.getId() + ") title is missing: " + cid);
					System.err.println("book chpater(" + target.getId() + ") title is missing: " + cid);
				} else {
					String ctitle = getMappingKey(key + ".Title");
					hparts.add(new HasPart(ctitle, HANDLE_PREFIX + cid, false, true));
					// delete authors for book
					deleteAuthors.addAll(normalizedNames(Arrays.asList(getMappingKey(key + ".Authors").split("\\|"))));
				}
			}
			// hasparts
			target.add("dc.relation.haspart", NDLDataUtils.serializeHasPart(hparts));
			delete("lrmi.educationalRole"); //delete it
			
			handlebooks(target); // extract publisher data
			updateInstitute(target); // handle institute
		} else if(target.contains("lrmi.learningResourceType", "Book chapter")) {
			// book chapter
			String k = "books_tree." + ConfigurationData.escapeDot(id);
			if(containsMappingKey(k)) {
				// book chapter
				String pid = getMappingKey(k + ".Parent");
				String ptitle = getMappingKey(k + ".PTitle");
				IsPartOf ipart = new IsPartOf(HANDLE_PREFIX + pid, ptitle);
				// ispart
				target.add("dc.relation.ispartof", NDLDataUtils.serializeIsPartOf(ipart));
				// duplicate publisher delete
				List<NDLDataNode> nodes = target.getNodes("dc.publisher");
				String mptitle = ptitle.replaceFirst(" *\\([0-9]+\\)$", "");
				for(NDLDataNode node : nodes) {
					String pub = node.getTextContent();
					if(StringUtils.equalsIgnoreCase(mptitle, pub)) {
						// delete
						node.remove();
					} else if(pub.matches("(.+, +)?[0-9]+, +[a-z0-9A-Z]+-[a-z0-9A-Z]+")) {
						node.remove();
						// pages
						String tokens[] = pub.substring(pub.lastIndexOf(',') + 1).trim().split("-");
						target.add("dc.format.extent:startingPage", tokens[0]);
						target.add("dc.format.extent:endingPage", tokens[1]);
					}
				}
				
				// further extract publisher data
				handlebooks(target);
				updateInstitute(target); // handle institute
			} else {
				// cross check
				throw new IllegalStateException("book chpater detail is missing: " + id);
			}
		}
		
		// name normalization
		normalizeNames(target, "dc.contributor.author");
		normalizeNames(target, "dc.contributor.advisor");
		normalizeNames(target, "dc.contributor.editor");
		
		if(deleteAuthors != null) {
			// delete unnecessary authors
			int c = deleteIfContains("dc.contributor.author", deleteAuthors);
			if(c > 0) {
				System.err.println("Deleted authors(" + target.getId() + "): " + c);
			}
		}
		
		if(book) {
			target.deleteDuplicateFieldValues("dc.contributor.editor", "dc.contributor.author");
		} else {
			target.deleteDuplicateFieldValues("dc.contributor.author", "dc.contributor.editor");
		}
		
		/*target.removeDuplicate("dc.contributor.author");
		target.removeDuplicate("dc.contributor.editor");
		target.removeDuplicate("dc.contributor.advisor");*/
		
		// LRT update
		target.replace("lrmi.learningResourceType", "Journal article", "article");
		target.replace("lrmi.learningResourceType", "Book chapter", "book");
		target.replace("lrmi.learningResourceType", "Book", "book");
		target.replace("lrmi.learningResourceType", "Proceeding", "proceeding");
		
		// publisher correction
		if(pubids.contains(id)) {
			// TODO need to discuss
			target.moveByRegex("dc.description", "ndl.sourceMeta.additionalInfo:note", ".+ .+");
			move("dc.description", "dc.subject");
			move("dc.publisher", "dc.description", 1); // second node move
		}
		
		// dc.subject dc.description none remove
		deleteIfContains("dc.subject", true, "none");
		deleteIfContains("dc.description", true, "none");
		
		// remove multiple spaces
		removeMultipleSpaces("dc.title", "dc.description", "dc.description.abstract");
		removeMultipleLines("dc.title", "dc.description", "dc.description.abstract");
		
		// handle missing publisher date
		if(!target.exists("dc.publisher.date")) {
			List<String> desc = target.getValue("dc.description");
			for(String d : desc) {
				Matcher m = YEAR_PATTERN_REGX.matcher(d);
				if(m.find()) {
					add("dc.publisher.date", m.group(1));
					break;
				}
			}
		}
		
		return true;
	}
	
	void updateInstitute(SIPDataItem target) {
		// updates institute
		//System.out.println("LRT: " + target.getValue("lrmi.learningResourceType"));
		//boolean book = target.contains("lrmi.learningResourceType", "Book");
		String pub = target.getSingleValue("dc.publisher.institution");
		if(StringUtils.isNotBlank(pub)) {
			/*if(book) {
				System.out.println("PUB: " + pub);
			}*/
			Matcher m = INSTITUTE_PATTERN_REGX.matcher(pub);
			if(m.find()) {
				target.updateSingleValue("dc.publisher.institution", m.group(1).replaceAll("^\\(|((, *[0-9]+)?\\)$)", ""));
			}
		}
	}
	
	void handlebooks(SIPDataItem target) {
		// publisher data extract
		List<NDLDataNode> pubdata = target.getNodes("dc.publisher");
		for(NDLDataNode node : pubdata) {
			String pub = node.getTextContent();
			if(StringUtils.startsWithIgnoreCase(pub, "volume") || StringUtils.startsWithIgnoreCase(pub, "number")
					|| CharUtils.isAsciiNumeric(pub.charAt(0))) {
				String tokens[] = pub.split("( +)|,");
				int l = tokens.length;
				for(int i = 0; i < l; i++) {
					String t = tokens[i];
					if(t.matches("[0-9]+")) {
						// pub year
						if(!target.exists("dc.publisher.date")) {
							add("dc.publisher.date", t);
						}
					} else if(StringUtils.startsWithIgnoreCase(t, "volume")) {
						// volume
						target.add("dc.identifier.other:volume", tokens[i + 1]);
					} else if(t.matches("[0-9]+-[0-9]+")) {
						// pagination
						addPages(target, t);
					}
				}
				// delete
				node.remove();
			}
		}
	}
	
	// check duplicate
	boolean checkduplicate(String pulisher, String institute) {
		if(StringUtils.isBlank(institute)) {
			// handle null case
			return false;
		}
		String regx = "\\.|( +)|\\)|\\(|,";
		String tokens1[] = pulisher.split(regx);
		String tokens2[] = institute.split(regx);
		int l1 = tokens1.length, l2 = tokens2.length;
		if(l2 < l1) {
			return false;
		}
		for(int i = 0; i < l1; i++) {
			String t1 = tokens1[i];
			String t2 = tokens1[i];
			if(!StringUtils.equalsIgnoreCase(t1, t2)) {
				return true;
			}
		}
		return false;
	}
	
	// normalize names
	void normalizeNames(SIPDataItem target, String field) {
		List<String> names = target.getValue(field);
		// delete
		target.delete(field);
		// normalize
		Set<String> modifiednames = normalizedNames(names);
		// re-update
		target.add(field, modifiednames);
	}
	
	Set<String> normalizedNames(List<String> names) {
		Set<String> modifiednames = new HashSet<String>();
		// normalize
		for(String name : names) {
			/*if(StringUtils.equalsIgnoreCase(name, "jr")) {
				// exclude junior
				continue;
			}*/
			name = name.replaceFirst(",? +Jr\\.?", "XJr"); // handle Jr. case
			String tokens[] = name.replaceAll("\\(.*\\)", "").split("( +and +)|( *, *)");
			for(String t : tokens) {
				modifiednames.add(NDLDataUtils.normalizeSimpleName(t).replace("XJr", " Jr."));
			}
		}
		return modifiednames;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/euclid/2019.Apr.09.10.18.14.Euclid.V1.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/euclid/logs";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/euclid/out";
		String name = "euclid.v3";
		
		String subjectmapFile = "/home/dspace/debasis/NDL/NDL_sources/euclid/conf/subject.map.csv";
		String bookshierarchyFile = "/home/dspace/debasis/NDL/NDL_sources/euclid/conf/2019.Apr.16.15.26.12.euclid.books.heirarchy_1.csv";
		String bookschildFile = "/home/dspace/debasis/NDL/NDL_sources/euclid/conf/2019.Apr.16.15.26.12.euclid.child.books_1.csv";
		String idFile = "/home/dspace/debasis/NDL/NDL_sources/euclid/dc.publisher-curation.csv";
		
		EuclidCuration p = new EuclidCuration(input, logLocation, outputLocation, name);
		p.pubids = NDLDataUtils.loadSet(idFile);
		p.turnOffLoadHierarchyFlag();
		p.dontShowWarnings();
		p.subjectsDDCMapping = NDLDataUtils.loadKeyValue(subjectmapFile);
		//p.booksTitleMapping = NDLDataUtils.loadKeyValue(bookstitleFile);
		p.addMappingResource(bookshierarchyFile, "Child", "books_tree");
		p.addMappingResource(bookschildFile, "ID", "books_child");
		p.correctData();
		
		System.out.println("Done.");
	}
	
	public static void main1(String[] args) throws Exception {
		/*String text = "Primary: 05C80: Random graphs [See also 60B20]";
				//+ "|Secondary: 05C20: Directed graphs (digraphs), tournaments 05C38: Paths and cycles [See also 90B10] 60C05: Combinatorial probability";
		List<String> values = kextract(text);
		for(String value : values) {
			System.out.println(value);
		}*/
		
		/*String text = "G. Lame, Leçons sur les coordonnées curvilignes et leurs diverses applications "
				+ "(Paris: Mallet-Bachelier, 1859)";*/
		/*String text = "Jens E. Fenstad, General Recursion Theory: An Axiomatic  Approach "
				+ "(Berlin: Springer-Verlag, 1980), 43-61";
		Matcher m = INSTITUTE_PATTERN_REGX.matcher(text);
		if(m.find()) {
			System.out.println(m.group(1).replaceAll("^\\(|((, *[0-9]+)?\\)$)", ""));
		}*/
		
		/*String value = "my name is ö debasis jana";
		System.out.println(NDLDataUtils.getUniqueInfoJSON("name", value));
		
		String json = "{\"authorInfo\":\"[\\\"Department of Clinical Chemistry, "
				+ "University of Lund, Malm\\u00f6 General Hospital, Sweden.\\\"]\"}";
		//String json = "{\"name\":\"debasis jana\"}";
		String a[] = NDLDataUtils.getJSONKeyedValue(json, false);
		System.out.println(a[0]);
		System.out.println(a[1]);*/
		
		/*String json = "{\"name\":\"J Stenflo\",\"affiliation\":\"[\\\"Department of Clinical Chemistry, University "
				+ "of Lund, Malm\\u00f6 General Hospital, Sweden.\\\"]\"}";
		Map<String, String> map = NDLDataUtils.mapFromJson(json, false);
		System.out.println(NDLDataUtils.getJson("xxx", map.get("affiliation")));*/
		//System.out.println(map);
		
		/*System.out.println("Michael J. Jacobson, Jr.".replace(", Jr.", "#Jr."));
		System.out.println(NDLDataUtils.normalizeSimpleName("Michael J. Jacobson"));
		System.out.println(NDLDataUtils.normalizeSimpleName("Michael J. Jacobson, Jr.".replace(", Jr.", "XJr")).replace("XJr", " Jr."));*/
		//System.out.println("/10.16929/as/2016.883.81".replaceAll("(?i)^(((doi: *)+)|[^0-9])", ""));
		
		/*String text = "Proc. [First] Berkeley Symp. on Math. Statist. and Prob. "
				+ "(Univ. of Calif. Press, 1949), i-viii";
		String r[] = pextract(text);
		System.out.println(r[0]);
		System.out.println(r[1]);*/
		
		/*SIPDataItem sip = NDLDataUtils.createBlankSIP("1234567");
		addPages(sip, "v-vii");*/
		
		/*String desc = "Proc. Third Berkeley Symp. on Math. Statist. and Prob., Vol. 2 (Univ. of Calif. Press, 1956)";
		Pattern p = Pattern.compile(".*([0-9]{4}).*");
		Matcher m = p.matcher(desc);
		if(m.find()) {
			System.out.println(m.group(1));
		}*/
	}
}