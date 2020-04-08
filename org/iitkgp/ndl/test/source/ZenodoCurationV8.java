package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;

public class ZenodoCurationV8 extends NDLSIPCorrectionContainer {
	
	//Set<String> ids;
	
	public ZenodoCurationV8(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		//String id = NDLDataUtils.getHandleSuffixID(target.getId());
		
		// remove ^Dr.
		target.replaceByRegex("dc.contributor.author",
				"(^Dr\\. +)|(,M\\. S\\.$)|(,Ph D\\.$)", "");
		target.replaceByRegex("dc.contributor.author", "( Ph D\\.,)|( M\\. S\\.,)", ",");
		target.replaceByRegex("dc.contributor.author", "(,Ph D\\. )|(,M\\. S\\. )", ", ");
		target.replaceByRegex("dc.contributor.author", "( Ph D\\. )|( M\\. S\\. )", " ");
		deleteIfContains("dc.contributor.author", true, "editor", "other");
		
		/*List<NDLDataNode> authors = target.getNodes("dc.contributor.author");
		boolean delete = false;
		for(NDLDataNode author : authors) {
			String v = author.getTextContent();
			int l = v.length();
			if(l < 5) {
				log("a.delete", v);
				delete = true;
				break;
			}
		}*/
		
		/*if(!delete) {
			// duplicates delete
			target.deleteDuplicateFieldValues("dc.creator.researcher", "dc.contributor.author");
		}*/
		
		/*if(id.equals("1001711") || id.equals("1035981") || id.equals("1009565")) {
			System.out.println("ID(" + id + ") A: " + NDLDataUtils.join(target.getValue("dc.contributor.author"), '#'));
			System.out.println("ID(" + id + ") R: " + NDLDataUtils.join(target.getValue("dc.creator.researcher"), '#'));
		}*/
		
		// duplicates delete
		target.deleteDuplicateFieldValues("dc.creator.researcher", "dc.contributor.author");
		
		
		// return !delete;
		return true;
	}

	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/zenodo/out/2019.Jun.25.17.07.22.zenodo.v7/2019.Jun.25.17.07.22.zenodo.v7.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/zenodo/logs"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/zenodo/out";
		String name = "zenodo.v8";
		
		ZenodoCurationV8 p = new ZenodoCurationV8(input, logLocation, outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		//p.addTextLogger("a.delete");
		p.correctData();
		
		System.out.println("Done.");
		
	}
}