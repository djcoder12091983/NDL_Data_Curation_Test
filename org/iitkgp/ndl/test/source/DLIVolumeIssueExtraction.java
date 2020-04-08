package org.iitkgp.ndl.test.source;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.NDLSIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

import com.opencsv.CSVWriter;

public class DLIVolumeIssueExtraction extends NDLSIPDataContainer {

	public DLIVolumeIssueExtraction(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}
	
	@Override
	protected boolean readItem(SIPDataItem item) throws Exception {
		
		String pi = item.getSingleValue("dc.publisher.institution");
		if(StringUtils.isNotBlank(pi)) {
			// extraction logic
			try {
				pi = pi.replace("\\", ""); // remove slash
				Map<String, String> map = extract(pi);
				if(!map.isEmpty()) {
					// data found
					String part = map.get("part");
					String sec = map.get("sec");
					if(StringUtils.isNotBlank(part) && StringUtils.isNotBlank(sec)) {
						part += " (Sec." + sec + ")";
					}
					log("volume.issue.log", new String[] { NDLDataUtils.getHandleSuffixID(item.getId()), pi,
							map.get("vol"), map.get("no"), part, sec });
				}
			} catch(Exception ex) {
				// cross check
				System.err.println(pi);
				throw ex;
			}
		}
		
		return true;
	}
	
	// extraction logic
	static Map<String, String> extract(String text) {
		
		Map<String, String> map = new HashMap<>(4);
		
		String tokens[] = text.split(" *(,|\\.|-| |_|(amp)|\\\\) *");
		List<String> ntokens = new ArrayList<>(tokens.length);
		for(String t : tokens) {
			if(StringUtils.isNotBlank(t)) {
				ntokens.add(t);
			}
		}
		// modified array
		tokens = new String[ntokens.size()];
		ntokens.toArray(tokens);
		
		int l = tokens.length;
		for(int i = 0 ;i < l;){
			String token = tokens[i];
				// volume
			if(StringUtils.startsWithIgnoreCase(token, "volume")) {
				// volume
				i = extract(token, tokens, "vol", 6, i, map);
			} else if(StringUtils.startsWithIgnoreCase(token, "vol")) {
				// volume
				i = extract(token, tokens, "vol", 3, i, map);
			} else if(StringUtils.startsWithIgnoreCase(token, "no") && !NDLDataUtils.isMonth(token)) {
				// issue
				i = extract(token, tokens, "no", 2, i, map);
			} else if(StringUtils.startsWithIgnoreCase(token, "section")) {
				// section
				i = extract(token, tokens, "sec", 7, i, map);
			} else if(StringUtils.startsWithIgnoreCase(token, "sec")) {
				// section
				i = extract(token, tokens, "sec", 3, i, map);
			} else if(StringUtils.startsWithIgnoreCase(token, "part")) {
				// part
				i = extract(token, tokens, "part", 4, i, map);
			} else {
				i++;
			}
		}
		
		return map;
	}
	
	// key wise extraction
	static int extract(String token, String tokens[], String key, int p, int i, Map<String, String> map) {
		int l = tokens.length;
		if (token.length() > p) {
			String v = token.substring(p);
			if(i + 1 < l) {
				String t1 = tokens[i + 1];
				if(t1.length() <= 2 && (NumberUtils.isDigits(t1) || NDLDataUtils.isRoman(t1))) {
					v += "-" + t1;
					i += 2;
				} else if(NDLDataUtils.isDecimalWords(t1)) {
					v += ' ' + t1;
					v = String.valueOf(NDLDataUtils.wordsToDecimal(v));
					i += 2;
				} else {
					i++;
				}
			} else {
				i++;
			}
			
			if(NDLDataUtils.isDecimalWords(v)) {
				v = String.valueOf(NDLDataUtils.wordsToDecimal(v));
			}
			
			map.put(key, v);
		} else {
			if(i + 1 < l) {
				String v = tokens[i + 1];
				if(i + 2 < l) {
					String t1 = tokens[i + 2];
					if(t1.length() <= 2 && (NumberUtils.isDigits(t1) || NDLDataUtils.isRoman(t1))) {
						v += "-" + t1;
						i += 3;
					} else if(NDLDataUtils.isDecimalWords(t1)) {
						v += ' ' + t1;
						v = String.valueOf(NDLDataUtils.wordsToDecimal(v));
						i += 3;
					} else {
						i += 2;
					}
				} else {
					i += 2;
				}
				
				if(NDLDataUtils.isDecimalWords(v)) {
					v = String.valueOf(NDLDataUtils.wordsToDecimal(v));
				}
				
				map.put(key, v);
			} else {
				i++;
			}
		}
		
		return i;
	}
	
	public static void main2(String[] args) {
		Map<String, String> map = extract("Vol XXXVIII Sec A Part IIIampIV");
		System.out.println(map);
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println("start.");
		
		String f1 = "/home/dspace/debasis/NDL/NDL_sources/DLI/data/pub.ins.volume.issue.part.sec.data";
		String f2 = "/home/dspace/debasis/NDL/NDL_sources/DLI/data/pub.ins.volume.issue.part.sec.sample.data.csv";
		BufferedReader reader = new BufferedReader(new FileReader(f1));
		CSVWriter writer = new CSVWriter(new FileWriter(f2));
		writer.writeNext(new String[]{"Text", "Volume", "Issue", "Part" , "Section"});
		
		String line;
		while((line = reader.readLine()) != null) {
			if(StringUtils.isNotBlank(line)) {
				line = line.replace("\\", "");
				Map<String, String> map = extract(line);
				writer.writeNext(new String[] { line, map.get("vol"), map.get("no"), map.get("part"), map.get("sec") });
			}
		}
		
		reader.close();
		writer.close();
		
		System.out.println("done.");
	}
	
	public static void main1(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/DLI/in/2019.Nov.19.12.58.30.DLI.filter.v1.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/DLI/logs";
		String name = "DLI.v1";
		
		DLIVolumeIssueExtraction t = new DLIVolumeIssueExtraction(input, logLocation, name);
		t.addCSVLogger("volume.issue.log", new String[]{"ID", "Text", "Volume", "Issue", "Part" , "Section"});
		t.processData();
		
		System.out.println("Done.");
	}
}