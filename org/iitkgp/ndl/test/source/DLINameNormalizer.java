package org.iitkgp.ndl.test.source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.util.NDLDataUtils;

// DLI name normalization
public class DLINameNormalizer {
	
	Set<String> delete;
	Map<String, String> mapping;
	Set<String> degree;
	String removeregx;
	Set<String> removetokens;
	
	static Pattern LITT_PATTERN = Pattern.compile(" +[a-zA-Z]\\.?,?$");
	static Pattern SINGLE_TOKEN_PATTERN = Pattern.compile("^[a-zA-Z]\\.?,?$");
	
	public DLINameNormalizer(String deletef, String degreef, String removetokensf, String mappingf) throws Exception {
		delete = NDLDataUtils.loadSet(deletef);
		degree = NDLDataUtils.loadSet(degreef);
		removetokens = NDLDataUtils.loadSet(removetokensf);
		mapping = NDLDataUtils.loadKeyValue(mappingf);
		
		removeregx = buildregx(removetokens);
	}
	
	// build remove reg-x
	String buildregx(Collection<String> tokens) {
		StringBuilder regx = new StringBuilder("(?i)");
		for(String t : tokens) {
			if(StringUtils.isNotBlank(t)) {
				regx.append('(').append(t).append(')').append('|');
			}
		}
		return regx.deleteCharAt(regx.length() - 1).toString();
	}
	
	List<String> correctName(String name) throws Exception {
		// mapping cases
		if(delete.contains(name)) {
			return NDLDataUtils.createNewList(""); // delete case
		}
		if(mapping.containsKey(name)) {
			return NDLDataUtils.createNewList(mapping.get(name).split(";"));
		}
		// logic based
		
		name = name.replaceFirst("(i?)Svaamii", "Swami").replaceAll("'|¯|/", "");
		
		// TODO multiple name split
		
		// sr. jr. for etc. remove from name
		if(!StringUtils.startsWithIgnoreCase(name, "pandit")) {
			name = name.replaceAll(removeregx, "");
		}
		
		// degree remove
		for(String deg : degree) {
			int p = name.lastIndexOf(deg);
			if(p != -1) {
				String t = name.substring(0, p);
				if(t.split(" +").length > 1) {
					name = t;
					break;
				}
			}
		}
		
		// handle mohammmad and lady|king
		String tokens[] = removeBlanks(name.split(",|( +)"));
		if(tokens.length == 2) {
			if(StringUtils.containsIgnoreCase(tokens[1], "Mohammad")
					|| StringUtils.containsIgnoreCase(tokens[1], "Mohd")
					|| StringUtils.containsIgnoreCase(tokens[1], "Muhammad")
					|| StringUtils.containsIgnoreCase(tokens[1], "Lord")
					|| StringUtils.containsIgnoreCase(tokens[1], "swami")
					|| StringUtils.containsIgnoreCase(tokens[1], "Maharaj")) {
				name = tokens[1] + ' ' + tokens[0];
			} else if(StringUtils.containsIgnoreCase(tokens[0], "Mohammad")
					|| StringUtils.containsIgnoreCase(tokens[0], "Muhammad")
					|| StringUtils.containsIgnoreCase(tokens[0], "Mohd")
					|| StringUtils.containsIgnoreCase(tokens[0], "Lord")
					|| StringUtils.containsIgnoreCase(tokens[0], "swami")
					|| StringUtils.containsIgnoreCase(tokens[0], "Maharaj")) {
				name = tokens[0] + ' ' + tokens[1];
			} else if(StringUtils.containsIgnoreCase(tokens[0], "Lady")
					|| StringUtils.containsIgnoreCase(tokens[0], "King")) {
				name = tokens[1] + ' ' + tokens[0];
			}
		}
		
		// litt. case handle
		if(StringUtils.startsWithIgnoreCase(name, "Litt,")) {
			Matcher m = LITT_PATTERN.matcher(name);
			if(m.find()) {
				name = name.replaceFirst("(i?)^Litt,", "").replaceFirst("[a-z]\\.?,?$", "");
			}
		}
		
		// handle single token starts with
		if(name.length() > 2 && (name.charAt(1) == '.' || name.charAt(1) == ',' || name.charAt(2) == ',')) {
			name = name.replaceFirst(",", "");
		}
		
		// final normalize
		name = name.replaceAll("(,|-) *$", "");
		
		return NDLDataUtils.createList(morework(name)); // result
	}
	
	String[] removeBlanks(String[] tokens) {
		List<String> ntokens = new ArrayList<String>(tokens.length);
		for(String t : tokens) {
			if(StringUtils.isBlank(t) || StringUtils.equals(t, ".")) {
				continue;
			}
			ntokens.add(t);
		}
		String ntokensa[] = new String[ntokens.size()];
		return ntokens.toArray(ntokensa);
	}
	
	String morework(String name) throws Exception {
		String tokens[] = name.split(" +");
		boolean f = false;
		StringBuilder mn = new StringBuilder();
		for(String t : tokens) {
			Matcher m = SINGLE_TOKEN_PATTERN.matcher(t);
			if(!m.matches()) {
				// no change
				f = true;
			}
			if(t.endsWith(".") && t.length() > 2) {
				mn.append(t.replaceAll("\\.$", "")).append(" ");
			} else {
				mn.append(t.replaceAll("^\\.+", "").trim()).append(" ");
			}
		}
		
		if(!f) {
			return ""; // remove all single tokens
		} else {
			return NDLDataUtils.removeMultipleSpaces(mn.toString().replaceAll("(i?)D\\. +Litt", "").trim());
		}
	}
}