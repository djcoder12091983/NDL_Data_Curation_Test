package org.iitkgp.ndl.test.source;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.DefaultNDLSIPDataContainer;

public class SIPReaderTest extends DefaultNDLSIPDataContainer {
	
	//String trye[] = {"éd.", "ed.", "edition"};
	
	Map<String, Long> jfreq = new HashMap<String, Long>();
	
	public SIPReaderTest(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}
	
	@Override
	protected boolean readItem(SIPDataItem item) throws Exception {
		/*if(item.getId().equals("cern/45917")) {
			System.out.println("Desc:");
			List<String> values = item.getValue("dc.description");
			for(String value : values) {
				if(StringUtils.isBlank(value)) {
					System.err.println("Yahoo!!");
				}
			}
		}*/
		/*List<NDLDataNode> descNodes = item.getNodes("dc.description");
		List<String> texts = new LinkedList<String>();
		for(int i = 0; i < descNodes.size(); i++) {
			NDLDataNode descNode = descNodes.get(i);
			String desc = descNode.getTextContent();
			if (StringUtils.startsWithIgnoreCase(desc, "Collaboration with:")
					|| StringUtils.startsWithIgnoreCase(desc, "Presented at :")
					|| StringUtils.startsWithIgnoreCase(desc, "Published in :")) {
				// valid description
				continue;
			}
			texts.add(desc);
			descNode.remove();
		}
		if(item.getId().equals("cern/45957")) {
			System.err.println(item.getFolder());
			//System.out.println(item.getValue("dc.description"));
			System.err.println(texts);
		}*/
		/*List<String> editions = item.getValue("dc.identifier.other:edition");
		String edition = null;
		boolean f = false;
		for(String e : editions) {
			edition = edition(e);
			if(StringUtils.isNotBlank(edition)) {
				f = true;
				break;
			}
		}
		if(f) {
			log("editions", new String[]{item.getId(), edition});
		}*/
		/*if(item.getId().equals("cern/1511095")) {
			System.out.println(item.getValue("dc.contributor.author"));
		}*/
		/*if(item.contains("dc.contributor.author", StringUtils.EMPTY)) {
			System.err.println(item.getId());
			System.err.println(item.getValue("dc.contributor.author"));
		}*/
		/*if(item.contains("lrmi.learningResourceType", "journal")) {
			log("journals", new String[] { item.getSingleValue("dc.title"), item.getId(),
					item.getSingleValue("dc.identifier.issn") });
		}*/
		
		String j = item.getSingleValue("dc.identifier.other:journal");
		if(StringUtils.isNotBlank(j)) {
			Long c = jfreq.get(j);
			if(c == null) {
				jfreq.put(j, 1L);
			} else {
				jfreq.put(j, c + 1);
			}
		}
		
		return true;
	}
	
	@Override
	public void postProcessData() throws Exception {
		// super call
		super.postProcessData();
		// write stat
		long max = Long.MIN_VALUE;
		for(String j : jfreq.keySet()) {
			long c = jfreq.get(j);
			if(c > max) {
				max = c;
			}
			log("journals.stat", new String[]{j , String.valueOf(c)});
		}
		
		System.out.println("Max: " + max);
	}
	
	/*String edition(String input) {
		int p  = input.indexOf("éd.");
		return null;
	}*/
	
	// test
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/OMICS/2019.Jan.16.11.47.03.omicsV2.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/OMICS/logs";
		String name = "omics.journals";
		
		SIPReaderTest t = new SIPReaderTest(input, logLocation, name);
		//t.addCSVLogger("journals", new String[]{"Journal", "Handle", "ISSN"});
		t.addCSVLogger("journals.stat", new String[]{"Journal", "Count"});
		t.processData();
		
		System.out.println("Done.");
	}

}