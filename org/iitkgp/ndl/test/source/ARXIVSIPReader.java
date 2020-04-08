package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.DefaultNDLSIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

// TODO
public class ARXIVSIPReader extends DefaultNDLSIPDataContainer {

	public ARXIVSIPReader(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}
	
	@Override
	protected boolean readItem(SIPDataItem item) throws Exception {
		
		/*String desc = item.getSingleValue("dc.identifier.other:alternateContentUri");
		String tokens[] = desc.split(";");
		String journal = null, vol = null, issue = null, pages = null;
		String issn = null, eissn = null, isbn = null, eisbn = null;
		for(String token : tokens) {
			String parts[] = token.split("( +|\\(|\\)|\\[|\\]|\\.|,|:)");
			int l = parts.length;
			boolean f = false;
			for(int i = 0 ; i < l; i++) {
				String part = parts[i];
				
			}
		}*/
		
		String id = item.getId();
		if(id.equals("arxiv/oai_arxiv_org_1711_05621") || id.equals("arxiv/oai_arxiv_org_hep_ex_9707021")) {
			//System.err.println("Author: " + item.getValue("dc.contributor.author"));
			System.err.println(NDLDataUtils.join(item.getValue("dc.contributor.author"), '|'));
		}
		
		return true;
	}
	
	void setValue(String val) {
		if(val == null) {
			
		}
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/generated_xml_data/ARXIV/out/2019.Jan.07.21.14.34.arxiv.author.v1/2019.Jan.07.21.14.34.arxiv.author.v1.Corrected.tar.gz";
		String logLocation = "<log_location>";
		String name = "arxiv";
		
		ARXIVSIPReader t = new ARXIVSIPReader(input, logLocation, name);
		//t.addCSVLogger("desc_csv");
		t.processData();
		
		System.out.println("Done.");
	}

}