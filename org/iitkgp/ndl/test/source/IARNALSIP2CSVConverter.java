package org.iitkgp.ndl.test.source;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.converter.NDLSIP2CSVConverter;
import org.iitkgp.ndl.data.Transformer;
import org.iitkgp.ndl.util.NDLDataUtils;

// test of NAL CSV conversion with filter data and column
public class IARNALSIP2CSVConverter {
	
	static Set<String> wrongNameTokens = new HashSet<String>();
	static Set<String> wrongNames = null;
	
	static {
		wrongNameTokens.add("major-general");
		wrongNameTokens.add("mr");
		wrongNameTokens.add("mrs");
		wrongNameTokens.add("professor");
		wrongNameTokens.add("prof");
		wrongNameTokens.add("associate");
		wrongNameTokens.add("dr");
		wrongNameTokens.add("jr");
	}
	
	static String normalizeName(String input) {
		//System.out.println("input: " + input);
		if(wrongNames.contains(input)) {
			// invalid name, empty string
			return StringUtils.EMPTY;
		}
		
		StringBuilder firstName = new StringBuilder();
		StringBuilder lastName = new StringBuilder();
		
		String tokens[] = input.split(",");
		if(tokens.length == 1) {
			List<String> list = getTokens(tokens[0]);
			int l = list.size();
			lastName.append(list.get(l - 1));
			for(int i = 0; i < l - 1; i++) {
				firstName.append(list.get(i)).append(" ");
			}
		} else {
			List<String> list1 = getTokens(tokens[0]);
			List<String> list2 = getTokens(tokens[1]);
			int l = list1.size();
			for(int i = 0; i < l; i++) {
				lastName.append(list1.get(i)).append(" ");
			}
			l = list2.size();
			for(int i = 0; i < l; i++) {
				firstName.append(list2.get(i)).append(" ");
			}
		}
		
		if(lastName.length() == 2) {
			// invalid last name, all are initials
			return StringUtils.EMPTY;
		}
		
		String lastNameText = lastName.toString().trim();
		String firstNameText = firstName.toString().trim();
		
		return lastNameText + (!firstNameText.isEmpty() ? ((!lastNameText.isEmpty() ? ", " : "") + firstNameText) : "");
	}
	
	// gets name tokens by text
	static List<String> getTokens(String text) {
		String tokens[] = text.split(" +");
		int l = tokens.length;
		List<String> list = new ArrayList<String>(2);
		for(int i = 0; i < l; i++) {
			String token = tokens[i];
			token = token.replace(".", "");
			if(wrongNameTokens.contains(token.toLowerCase())) {
				// wrong tokens
				continue;
			}
			if(StringUtils.isAllUpperCase(token)) {
				// all upper case then make initial cap
				list.add(NDLDataUtils.initCap(token) + (token.length() == 1 ? "." : ""));
			} else {
				// normal case, leave as it is
				list.add(token);
			}
		}
		return list;
	}
	
	// TEST
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/IAR/raw_data/research_article/out/2018.Aug.28.17.12.47.JSTOR.V1/2018.Aug.28.17.12.47.JSTOR.V1.Corrected.tar.gz";
		String logLocation  = "/home/dspace/debasis/NDL/IAR/raw_data/research_article/csv_data";
		String wrongNameFile = "/home/dspace/debasis/NDL/IAR/raw_data/research_article/conf/wrong.authors.txt";
		
		wrongNames = NDLDataUtils.loadSet(wrongNameFile);
		Set<String> names = new HashSet<String>(); 
		
		//Set<String> titles = new HashSet<String>();
		
		// List<String> authors = new LinkedList<String>();
		
		NDLSIP2CSVConverter converter = new NDLSIP2CSVConverter(input, logLocation, "JSTOR.author");
		converter.setMultivalueSeparator('|');
		converter.setCsvThresholdLimit(50000);
		/*converter.addDataFilter(new Filter<SIPDataItem>() {
			// filter
			@Override
			public boolean filter(SIPDataItem data) {
				List<String> values = data.getValue("dc.contributor.author");
				for(String value : values) {
					if(!StringUtils.contains(value, ",")) {
						authors.add(value);
					}
				}
				return true;
			}
		});*/
		// converter.addColumnSelector("dc.identifier.other:itemId", "ID");
		/*converter.addColumnSelector("dc.identifier.uri", "URL");*/
		/*converter.addColumnSelector("dc.subject.ddc", "DDC");
		converter.addColumnSelector("dc.subject", "Keywords");
		converter.addColumnSelector("dc.language.iso", "Language");*/
		//converter.addColumnSelector("dc.identifier.other:uniqueId", "Unique ID");
		//converter.addColumnSelector("dc.identifier.other:itemId", "Item ID");
		/*converter.addColumnSelector("dc.contributor.author", "Author");
		converter.addColumnSelector("dc.contributor.other:inventor", "Inventor");
		converter.addColumnSelector("dc.contributor.other:compiler", "Compiler");
		converter.addColumnSelector("dc.contributor.other:investigator", "Investigator");
		converter.addColumnSelector("dc.contributor.editor", "Editor");*/
		/*converter.addColumnSelector("dc.rights.holder", "Rights Holder");*/
		/* converter.addColumnSelector("dc.relation.ispartofseries", "Series");
		converter.addColumnSelector("lrmi.learningResourceType", "LRT");
		converter.addColumnSelector("ndl.sourceMeta.uniqueInfo", "Unqiue Info");*/
		
		Transformer<String, String> t = new Transformer<String, String>() {
			// ransformer logic
			@Override
			public Collection<String> transform(String name) {
				//name = normalizeName(name);
				if(StringUtils.isNotBlank(name)) {
					names.add("\"" + name + "\"");
				}
				return NDLDataUtils.createNewList(name);
			}
		};
		/*converter.addColumnSelector("dc.title", "Title");
		converter.addColumnSelector("dc.publisher.date", "Date");*/
		/*converter.addColumnSelector("dc.contributor.other:inventor", "Inventor");
		converter.addColumnSelector("dc.contributor.other:compiler", "Compiler");
		converter.addColumnSelector("dc.contributor.other:investigator", "Investigator");
		converter.addColumnSelector("dc.contributor.editor", "Editor");*/
		converter.addColumnSelector("dc.contributor.author", "Author", t);
		converter.convert(); // convert
		
		IOUtils.writeLines(names, NDLDataUtils.NEW_LINE,
				new FileOutputStream(new File(logLocation, "unique.authors.txt")), "UTF-8");
		/*for (String author : IOUtils.readLines(
				new FileInputStream("/home/dspace/debasis/NDL/IAR/raw_data/research_article/conf/author.test.list"),
				"utf-8")) {
			System.out.println(author + " => " + normalizeName(author));
		}*/
		
		//System.out.println(normalizeName("L. C. W."));
		
		System.out.println("Done.");
	}
}