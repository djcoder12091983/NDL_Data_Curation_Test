package org.iitkgp.ndl.test.source;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.NDLLanguageDetail;
import org.iitkgp.ndl.data.NDLLanguageTranslate;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.iitkgp.ndl.util.NDLServiceUtils;

public class HALCorrectionV4 extends NDLSIPCorrectionContainer {
	
	int limit = 0;
	
	public HALCorrectionV4(String input, String logLocation, String outputLocation, String name) {
		// validation off
		super(input, logLocation, outputLocation, name, false);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		// title alternative
		List<NDLDataNode> tnodes = target.getNodes("dc.title.alternative");
		boolean en = false;
		Set<NDLLanguageDetail> langs = new LinkedHashSet<>();
		List<String> talts = new LinkedList<String>();
		for(NDLDataNode tnode : tnodes) {
			Map<String, String> map = NDLDataUtils.mapFromJson(tnode.getTextContent());
			String lang = NDLServiceUtils.normalilzeLanguage(map.get("language"));
			if(lang == null) {
				// could not be converted
				// report error
				String msg = target.getId() + " Lang: " + map.get("language");
				System.err.println(msg);
				//log("lang.error", msg);
				// remove node
				tnode.remove();
				continue;
			}
			String value = map.get("text");
			if(lang.equals("eng")) {
				// english
				if(!en) {
					// title to be updated
					en = true;
					target.updateSingleValue("dc.title", value);
					langs.add(new NDLLanguageDetail(lang, value));
				} else {
					// title alternative
					talts.add(value);
				}
			} else {
				// other language
				langs.add(new NDLLanguageDetail(lang, value));
			}
			
			// remove node
			tnode.remove();
		}
		
		// adds alternatives
		if(!talts.isEmpty()) {
			log("translate.log", target.getId()  +" => " + NDLDataUtils.join(talts, '|'));
			target.add("dc.title.alternative", talts);
		}
		
		if(!en) {
			// item delete
			return false;
		}
		
		// translate JSON
		if(langs.size() > 1) {
			String t = NDLDataUtils.serializeLanguageTranslation(new NDLLanguageTranslate("title", langs));
			if(++limit < 1000) {
				log("translate.log", target.getId() + " => " + t);
			}
			target.add("ndl.sourceMeta.translation", t);
		}
		
		// description translation
		tnodes = target.getNodes("dc.description");
		langs = new LinkedHashSet<>();
		en = false;
		boolean mergef = false;
		StringBuilder mergedesc = new StringBuilder();
		for(NDLDataNode tnode : tnodes) {
			Map<String, String> map = NDLDataUtils.mapFromJson(tnode.getTextContent());
			String value = map.get("text").trim();
			if (value.length() < 100) {
				// invalid data
				tnode.remove();
				continue;
			}
			String l = map.get("lang");
			String lang = NDLServiceUtils.normalilzeLanguage(l);
			if(lang == null) {
				// report error
				String msg = target.getId() + " Lang: " + l;
				System.err.println(msg);
				//log("lang.error", msg);
				if(NumberUtils.isDigits(l)) {
					// merge
					mergedesc.append(value).append(' ');
					mergef = true;
				}
				// remove node
				tnode.remove();
				continue;
			}
			if(lang.equals("eng")) {
				if(!en) {
					//target.add("dc.description.abstract", value);
					mergedesc.append(value).append(' ');
					en = true;
				}
			} else {
				langs.add(new NDLLanguageDetail(lang, value));
			}
			tnode.remove();
		}
		
		// merge description if exists
		if(mergedesc.length() > 0) {
			String desc = mergedesc.toString();
			if(mergef) {
				System.err.println("ID: " + target.getId() + " => (desc) " + desc);
			}
			target.add("dc.description.abstract", desc.trim());
		}
		
		// translate JSON
		if(!langs.isEmpty()) {
			String t = NDLDataUtils.serializeLanguageTranslation(new NDLLanguageTranslate("description", langs));
			if(++limit < 1000) {
				log("translate.log", target.getId() + " => " + t);
			}
			target.add("ndl.sourceMeta.translation", t);
		}
		
		// finally do this
		removeMultipleLinesAndSpaceForAllFields();
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		
		// NDLConfigurationContext.addConfiguration("ndl.service.base.url", "http://10.2.3.4:100");
		
		// flat SIP location or compressed SIP location
		String input = "/home/dspace/debasis/NDL/NDL_sources/HAL/in/2019.Sep.12.19.07.36.HAL.V3.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/HAL/out"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/HAL/out";
		String name = "hal.v4";
		
		HALCorrectionV4 p = new HALCorrectionV4(input, logLocation, outputLocation, name);
		//p.addTextLogger("lang.error");
		p.addTextLogger("translate.log");
		p.turnOffLoadHierarchyFlag();
		p.turnOffControlFieldsValidationFlag();
		//p.turnOnFieldWiseDetailValidation();
		p.correctData();
		
		System.out.println("Done.");
	}
	
	/*public static void main(String[] args) {
		String txt = "{\"role\":\"crp\",\"firstname\":\"Philippe\",\"surname\":\"Blanchard\",\"affiliation\":[\"struct-24348\"]}";
		NDLJSONParser p = new NDLJSONParser(txt);
		System.out.println(p.getText("role", ""));
	}*/
}