package org.iitkgp.ndl.test.source;

import java.util.List;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.asset.AssetDetail;
import org.iitkgp.ndl.data.container.NDLSIPDataContainer;

public class SIPreaderTest1 extends NDLSIPDataContainer {
	
	//Set<String> lrts = new HashSet<>();

	public SIPreaderTest1(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}
	
	@Override
	protected boolean readItem(SIPDataItem item) throws Exception {
		
		/*if(item.getId()
				.equals("omicscp/probabilistic_costresponse_functions_for_evaluating_damage_cost_of_buildings_76139")) {
			System.err.println(item.getSingleValue("dc.contributor.author"));
		}*/
		
		/*if(item.exists("lrmi.learningResourceType")) {
			lrts.addAll(item.getValue("lrmi.learningResourceType"));
		}*/
		
		/*AssetDetail detail = item.readAsset(NDLAssetType.FULLTEXT);
		System.out.println(new String(detail.getContents()));*/
		
		/*if(NDLDataUtils.getHandleSuffixID(item.getId()).equals("judgments_handle_123456789_224468")) {
			System.out.println("SV: " + item.getSingleValue("dc.description.searchVisibility"));
		}*/
		
		/*if(!Boolean.valueOf(item.getSingleValue("dc.description.searchVisibility"))) {
			// track false node
			log("sv.false", item.getId());
		}*/
		
		/*if(item.getId().equals("nptel/courses_105_106_105106177_lec3")) {
			String t = item.getSingleValue("dc.title.alternative");
			System.err.println("T: " + t);
			int l = t.length();
			for(int i = 0; i < l; i++) {
				System.err.print(t.charAt(i) + "(" + i + ") ");
			}
			System.out.println();
			System.err.println(t.charAt(24));
			System.err.println((int)t.charAt(24));
			System.err.println(NDLDataUtils.removeInvalidCharacters(t, 65533));
		}*/
		
		/*List<String> authors = item.getValue("dc.contributor.author");
		for(String a : authors) {
			if(a.length() > 30) {
				log("large.authors", new String[]{NDLDataUtils.getHandleSuffixID(item.getId()), a, ""});
			}
		}
		
		String title = item.getSingleValue("dc.title");
		String tokens[] = title.split("\\|");
		if(tokens.length == 2) {
			tokens = tokens[1].split(" +");
			int c = 0;
			for(String t : tokens) {
				if(t.trim().charAt(t.length() - 1) == '.') {
					c++;
				}
			}
			
			if(c >= 2) {
				log("titles", new String[]{item.getId(), title});
			}
		}*/
		
		List<AssetDetail> assets = item.readAllAssets(false);
		for(AssetDetail asset : assets) {
			System.out.println(item.getId() + ": " + asset.getType().getType() + " => " + asset.getName());
		}
		
		return true;
	}
	
	// test
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/Samim/RRB-GD_23_01_2020.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/Samim/temp";
		String name = "SIP.asset.test";
		
		SIPreaderTest1 t = new SIPreaderTest1(input, logLocation, name);
		//t.addTextLogger("sv.false");
		/*t.addCSVLogger("large.authors", new String[]{"ID", "OLD", "NEW"});
		t.addCSVLogger("titles", new String[]{"ID", "Title"});*/
		t.processData();
		
		/*String text = "my 	debasis jana";
		//String text = "my name                  debasis jana";
		System.out.println(NDLDataUtils.removeMultipleSpaces(text));
		int l = text.length();
		for(int i = 0; i < l; i++) {
			System.out.println((int)text.charAt(i));
		}*/
		
		//System.out.println(t.lrts);
		
		System.out.println("Done.");
	}
}