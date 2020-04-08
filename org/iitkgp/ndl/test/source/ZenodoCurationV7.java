package org.iitkgp.ndl.test.source;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.Transformer;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class ZenodoCurationV7 extends NDLSIPCorrectionContainer {
	
	Set<String> wlincenses;
	Set<String> asplit;
	
	public ZenodoCurationV7(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}

	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		deleteIfContains("dc.rights.license", wlincenses);
		deleteIfContainsByRegex("dc.publisher.place", "^.{1,2}$");
		split("dc.publisher.institution", '|');
		target.move("dc.publisher", "dc.description", new Transformer<String, String>() {
			
			@Override
			public Collection<String> transform(String input) {
				return NDLDataUtils.createList("Published in: " + input);
			}
		});
		
		// special case
		moveIfContains("dc.contributor.author", "dc.contributor.other:organization", "Statistics, Bureau Of");
		target.replace("dc.contributor.other:organization", "Statistics, Bureau Of", "Bureau Of Statistics");
		
		// remove ^Dr.
		target.replaceByRegex("dc.contributor.author",
				"(^Dr\\. +)|(,M\\. S\\.$)|(,Ph D\\.$)", "");
		target.replaceByRegex("dc.contributor.author", "( Ph D\\.,)|( M\\. S\\.,)", ",");
		target.replaceByRegex("dc.contributor.author", "(,Ph D\\. )|(,M\\. S\\. )", ", ");
		target.replaceByRegex("dc.contributor.author", "( Ph D\\. )|( M\\. S\\. )", " ");
		deleteIfContains("dc.contributor.author", true, "editor", "other");
		
		List<NDLDataNode> authors = target.getNodes("dc.contributor.author");
		boolean f = false;
		for(NDLDataNode author : authors) {
			String v = author.getTextContent();
			if(asplit.contains(v)) {
				f = true;
				// split case
				String tokens[] = v.split(",");
				author.setTextContent(tokens[0]);
				target.add("dc.contributor.author", tokens[1]);
			} else if(v.contains(",") && v.contains(" And ")) {
				f = true;
				String tokens[] = v.split(",|( +And +)");
				author.setTextContent(tokens[0]);
				int l = tokens.length;
				for(int i = 1; i< l; i++) {
					target.add("dc.contributor.author", tokens[i]);
				}
			}
		}
		if(f) {
			// TEST
			System.out.println("ID(" + target.getId() + ") A: "
					+ NDLDataUtils.join(target.getValue("dc.contributor.author"), '#'));
		}
		
		// duplicates delete
		target.deleteDuplicateFieldValues("dc.creator.researcher", "dc.contributor.author");
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/zenodo/out/2019.Jun.26.14.59.32.zenodo.v6/2019.Jun.26.14.59.32.zenodo.v6.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/zenodo/logs"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/zenodo/out";
		String name = "zenodo.v7";
		
		String wronglfile = "/home/dspace/debasis/NDL/NDL_sources/zenodo/conf/wrong.license";
		String ausplitfile = "/home/dspace/debasis/NDL/NDL_sources/zenodo/conf/author.split";
		
		ZenodoCurationV7 p = new ZenodoCurationV7(input, logLocation, outputLocation, name);
		p.turnOffLoadHierarchyFlag();
		p.wlincenses = NDLDataUtils.loadSet(wronglfile);
		p.asplit = NDLDataUtils.loadSet(ausplitfile);
		p.correctData();
		
		System.out.println("Done.");
	}
}
