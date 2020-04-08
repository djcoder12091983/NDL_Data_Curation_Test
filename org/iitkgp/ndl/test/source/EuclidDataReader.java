package org.iitkgp.ndl.test.source;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.DefaultNDLSIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class EuclidDataReader extends DefaultNDLSIPDataContainer {
	
	class ProceedingData {
		String id;
		String lrt;
		String proceeding;
		String publisher;
		String year;
		String pagination;
		
		public ProceedingData(String id, String lrt, String proceeding, String publisher, String year,
				String pagination) {
			this.id = id;
			this.lrt = lrt;
			this.proceeding = proceeding;
			this.publisher = publisher;
			this.year = year;
			this.pagination = pagination;
		}
	}
	
	long valid = 0;
	long invalid = 0;
	
	// proceedings data
	List<ProceedingData> proceedings = new ArrayList<ProceedingData>();

	public EuclidDataReader(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}
	
	@Override
	protected boolean readItem(SIPDataItem item) throws Exception {
		
		String id = item.getId();
		
		List<String> subject = item.getValue("dc.subject.other:callNumberDDC");
		List<String> subjects = new ArrayList<String>(4);
		for(String sub : subject) {
			subjects.addAll(extract(sub));
		}
		
		if(!subjects.isEmpty()) {
			log("subjects",
					new String[] {item.getId(), NDLDataUtils.join(subject, '|'), NDLDataUtils.join(subjects, '|')});
		}
		
		if(item.contains("lrmi.learningResourceType", "Proceeding")) {
			// proceeding data
			List<String> publisher = item.getValue("dc.publisher");
			String[] detail;
			String pub;
			String proceeding = publisher.get(0);
			if(publisher.size() == 2) {
				pub = publisher.get(1);
			} else {
				pub = item.getSingleValue("dc.publisher.institution");
			}
			detail = extract1(pub);
			proceedings.add(new ProceedingData(id, NDLDataUtils.join(item.getValue("lrmi.learningResourceType"), '|'),
					proceeding, pub, detail[0],
					detail[1]));
		} else if(item.contains("lrmi.learningResourceType", "Journal article")) {
			// journal details
			List<String> publisher = item.getValue("dc.publisher");
			String journal;
			String detail;
			if(item.exists("dc.identifier.other:journal")) {
				journal = item.getSingleValue("dc.identifier.other:journal");
				detail = publisher.get(0);
			} else {
				journal = publisher.get(0);
				detail = publisher.get(1);
			}
			Map<String, String> map = extract2(detail);
			String y = map.get("year");
			String v = map.get("vol");
			String i = map.get("no");
			String p = map.get("page");
			String f = null;
			if(NumberUtils.isDigits(y) && NumberUtils.isDigits(v) && StringUtils.isNotBlank(i)
					&& StringUtils.isNotBlank(p) && p.matches("[0-9]+-[0-9]+")) {
				// valid
				f = "journals.valid";
				valid++;
			} else {
				f = "journals.invalid";
				invalid++;
			}
			log(f, new String[] {id, journal, detail, y, v, i, p, map.get("article-id")});
		}
		
		// books child parent
		if(item.contains("lrmi.learningResourceType", "Book")) {
			List<String> chapters = item.getValue("lrmi.educationalRole");
			for(String chapter : chapters) {
				int idx = chapter.lastIndexOf('/');
				String child = chapter.substring(idx + 1);
				log("books.heirarchy", new String[] {child, NDLDataUtils.getHandleSuffixID(id),
						NDLDataUtils.removeMultipleSpaces(item.getSingleValue("dc.title"))});
			}
		} else if(item.contains("lrmi.learningResourceType", "Book chapter")) {
			log("child.books",
					new String[] {NDLDataUtils.getHandleSuffixID(id),
							NDLDataUtils.removeMultipleSpaces(item.getSingleValue("dc.title")),
							NDLDataUtils.join(item.getValue("dc.contributor.author"), '|')});
		}
		
		return true;
	}
	
	@Override
	public void postProcessData() throws Exception {
		super.postProcessData();
		// sort the data
		Collections.sort(proceedings, new Comparator<ProceedingData>() {
			@Override
			public int compare(ProceedingData first, ProceedingData second) {
				String p1 = first.proceeding;
				String p2 = second.proceeding;
				int c = p1.compareTo(p2);
				if(c == 0) {
					String year1 = first.year;
					String year2 = second.year;
					String pagination1 = first.pagination;
					String pagination2 = second.pagination;
					// year
					if(NumberUtils.isDigits(year1) && NumberUtils.isDigits(year2)) {
						c = Integer.valueOf(year1).compareTo(Integer.valueOf(year2));
					} else {
						c = year1.compareTo(year2);
					}
					if(c == 0) {
						// page
						String fpage1 = pagination1.split("-")[0].trim();
						String fpage2 = pagination2.split("-")[0].trim();
						if(NumberUtils.isDigits(fpage1) && NumberUtils.isDigits(fpage2)) {
							return Integer.valueOf(fpage1).compareTo(Integer.valueOf(fpage2));
						} else {
							// text based comparison
							return fpage1.compareTo(fpage2);
						}
					} else {
						return c;
					}
				} else {
					return c;
				}
			}
		});
		
		for(ProceedingData d : proceedings) {
			log("proceedings",
					new String[] {d.id, d.lrt, d.proceeding, d.publisher, d.year, d.pagination});
		}
	}
	
	static Map<String, String> extract2(String text) {
		Map<String, String> map = new HashMap<String, String>(4);
		String tokens[] = text.split(" *, *");
		for(String token : tokens) {
			token = token.replace(".", "");
			String tokens1[] = token.split(" +");
			if(StringUtils.startsWithIgnoreCase(tokens1[0], "vol")) {
				// volume
				map.put("vol", tokens1[1]);
			} else if(StringUtils.containsIgnoreCase(tokens1[0], "number")
					|| StringUtils.containsIgnoreCase(tokens1[0], "issue")) {
				// issue
				map.put("no", tokens1[1]);
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
				map.put("year", tokens1[2].replaceAll("\\(|\\)", ""));
			}
		}
		
		return map;
	}
	
	String[] extract1(String text) {
		String tokens[] = text.split(",");
		int l = tokens.length;
		// second last is year
		String year = tokens[l -2].replace(")", "");
		// last is pagination
		//String fpage = tokens[l - 1].split("-")[0].trim();
		String page = tokens[l - 1];
		return new String[]{year.trim(), page.trim()};
	}
	
	// extracts subjects
	static List<String> extract(String subject) {
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
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/euclid/2019.Apr.09.10.18.14.Euclid.V1.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/euclid/extraction";
		String name = "euclid";
		
		EuclidDataReader p = new EuclidDataReader(input, logLocation, name);
		p.addCSVLogger("subjects", new String[]{"ID", "Text", "Subjects"});
		p.addCSVLogger("journals.valid",
				new String[] {"ID", "Journal", "Detail", "Year", "Volume", "Issue", "Pagination", "Article-ID"});
		p.addCSVLogger("journals.invalid",
				new String[] {"ID", "Journal", "Detail", "Year", "Volume", "Issue", "Pagination", "Article-ID"});
		p.addCSVLogger("proceedings",
				new String[] {"ID", "LRT", "Proceeding", "Publisher", "Year", "Pagination"});
		p.addCSVLogger("books.heirarchy", new String[]{"Child", "Parent", "PTitle"});
		p.addCSVLogger("child.books", new String[]{"ID", "Title", "Authors"});
		p.processData();
		
		System.out.println("Valid journals: " + p.valid + " Invalid journals: " + p.invalid);
		
		System.out.println("Done.");
	}
	
	public static void main1(String[] args) {
		/*String text = "Secondary: 35B40: Asymptotic behavior of solutions 35R60: Partial differential "
				+ "equations with randomness, stochastic partial differential equations [See also 60H15] "
				+ "60J65: Brownian motion [See also 58J65] 76M35: Stochastic analysis 76S05: Flows in porous "
				+ "media; filtration; seepage";*/
		String text = "Primary: 54X10 58Y30";
		List<String> values = extract(text);
		for(String value : values) {
			System.out.println(value);
		}
		
		// System.out.println("47-86".matches("[0-9A-Z]+(-[0-9A-Z]+)?"));
		
		//System.out.println(extract2("Volume 4, Number 1 (2010), 47-86."));
	}

}