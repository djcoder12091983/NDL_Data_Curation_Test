package org.iitkgp.ndl.test.source;

import java.util.List;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.NDLSIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class DLINameCurationV2 extends NDLSIPDataContainer {
	
	DLINameNormalizer normalizer;
	
	public DLINameCurationV2(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}
	
	void loadconf(String deletef, String degreef, String removetokensf, String mappingf) throws Exception {
		normalizer = new DLINameNormalizer(deletef, degreef, removetokensf, mappingf);
	}
	
	@Override
	protected boolean readItem(SIPDataItem item) throws Exception {
		
		String id = NDLDataUtils.getHandleSuffixID(item.getId());
		
		List<String> names = item.getValue("dc.contributor.author");
		for(String name : names) {
			log("names", new String[] { id, name, NDLDataUtils.join(normalizer.correctName(name), '|') });
		}
		
		return true;
	}
	
	public static void main1(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/DLI/in/2019.Nov.19.12.58.30.DLI.filter.v1.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/DLI/logs/author";
		String name = "dli.authors";
		
		String deletef = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/name.delete";
		String mappingf = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/name.amping.csv";
		String degreef = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/name.degree";
		String removetokensf = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/name.remove.tokens";
		
		DLINameCurationV2 t = new DLINameCurationV2(input, logLocation, name);
		t.loadconf(deletef, degreef, removetokensf, mappingf);
		t.addCSVLogger("names", new String[]{"ID", "Old", "New"});
		
		t.processData();
		
		System.out.println("Done.");
	}
	
	public static void main(String[] args) throws Exception {
		
		String deletef = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/name.delete";
		String mappingf = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/name.amping.csv";
		String degreef = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/name.degree";
		String removetokensf = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/name.remove.tokens";
		
		DLINameNormalizer normalizer = new DLINameNormalizer(deletef, degreef, removetokensf, mappingf);
		
		/*System.out.println(t.removeregx);
		System.out.println("Ali, Aruna Asaf For.".replaceAll(t.removeregx, ""));*/
		//System.out.println(t.correctName("Ali, Aruna Asaf For."));
		
		// System.out.println("Aanand, Lakshhmand-".replaceAll("(,|-) *$", ""));
		
		System.out.println(normalizer.correctName("Stephen, Leack"));
	}
}