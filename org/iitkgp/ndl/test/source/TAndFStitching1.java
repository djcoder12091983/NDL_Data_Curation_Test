package org.iitkgp.ndl.test.source;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.DefaultNDLSIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

import com.opencsv.CSVReader;

public class TAndFStitching1 extends DefaultNDLSIPDataContainer {
	
	class Item {
		String handle;
		String title;
		
		Item(String handle, String title) {
			this.handle = handle;
			this.title = title;
		}
	}
	
	Map<String, Item> uri2details = new HashMap<String, Item>();
	Set<String> urls = new HashSet<String>();
	Map<String, String> journals = new HashMap<String, String>();
	Map<String, String> jcollections = new HashMap<String, String>();
	String dictFile;

	public TAndFStitching1(String input, String logLocation, String dictFile, String name) {
		super(input, logLocation, name);
		this.dictFile = dictFile;
	}

	@Override
	protected boolean readItem(SIPDataItem item) throws Exception {
		/*if(item.containsByRegex("dc.identifier.other:doi", "10\\.1080/09853111\\.1993\\.11105246")) {
			System.err.println(item.getId());
		}*/
		String uri = item.getSingleValue("dc.identifier.uri");
		String title = item.getSingleValue("dc.title");
		String handle = item.getId();
		uri2details.put(uri, new Item(handle, title));
		
		String jinfo = item.getSingleValue("dc.relation.requires");
		if(StringUtils.isNotBlank(jinfo)) {
			String jcode = jinfo.split("/")[2];
			journals.put(jcode, item.getSingleValue("dc.identifier.other:journal"));
			
			String f = item.getFolder();
			String col = f.substring(0, f.lastIndexOf('/'));
			jcollections.put(jcode, col);
		}
		
		return true;
	}
	
	@Override
	public void postProcessData() throws Exception {
		super.postProcessData();
		// write update hierarchy data
		CSVReader reader = NDLDataUtils.readCSV(new File(dictFile), 1);
		String tokens[];
		while((tokens = reader.readNext()) != null) {
			String uri = tokens[3].replace("/full/", "/abs/");
			if(!urls.add(uri)) {
				// already exists, so skip it
				continue;
			}
			Item item = uri2details.get(uri);
			if(item == null) {
				System.err.println("URI mapping not found: " + uri);
				//log("missing.uri", uri);
			} else {
				String j = tokens[0];
				String v = tokens[1].substring(4);
				String i;
				tokens[2] = tokens[2].replaceFirst("\\..*$", "");
				if(StringUtils.startsWithIgnoreCase(tokens[2], "issue")) {
					// normal
					i = tokens[2].substring(6);
				} else {
					// suppl
					i = tokens[2];
				}
				String itext = j + "_" + v + "_" + i;
				String ihandle = "tandfonline/I_" + itext; // issue handle
				
				String t[] = v.split("_");
				log("Dir_Struct_v4.0", new String[] { itext, item.handle, item.title, ihandle,
						"Year: " + t[1] + " Volume: " + t[0] + " Issue: " + i });
			}
		}
		
		reader.close();
		
		for(String jcode : journals.keySet()) {
			log("journals", new String[]{jcode, journals.get(jcode)});
		}
		
		for(String jcode : jcollections.keySet()) {
			log("jcollections", new String[]{jcode, jcollections.get(jcode)});
		}
	}
	
	// test
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/Taylor and Francis/in/t&f.17012019.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/generated_xml_data/Taylor and Francis/logs";
		String name = "tandfTnF";
		
		String dictFile = "/home/dspace/debasis/NDL/generated_xml_data/Taylor and Francis/conf/TnF_Dir_Structure.v3.csv";
		
		TAndFStitching1 t = new TAndFStitching1(input, logLocation, dictFile, name);
		t.addCSVLogger("Dir_Struct_v4.0", new String[]{"issue", "item-handle", "item-title", "parent-handle", "parent-title"});
		t.addCSVLogger("journals", new String[]{"journal-code", "journal-name"});
		t.addCSVLogger("jcollections", new String[]{"journal-code", "journal-collection"});
		//t.addTextLogger("missing.uri");
		t.processData();
		
		// System.out.println("/toc/tgsi20/7/3".split("/")[2]);
		
		System.out.println("Done.");
	}
}