package org.iitkgp.ndl.test.source;

import java.util.Map;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class IITKGPCuration1 extends NDLSIPCorrectionContainer {
	
	Map<String, String> accessions;

	public IITKGPCuration1(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		String id = target.getId();
		if(accessions.containsKey(id)) {
			// delete and update
			target.delete("dc.identifier.other:accessionNo");
			target.add("dc.identifier.other:accessionNo", accessions.get(id).split("\\|"));
		}
		
		if(id.equals("cliitkgp/TB23645")) {
			// cross check
			System.err.println("Updated Accession: " + target.getValue("dc.identifier.other:accessionNo"));
		}
		
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		
		// NDLDataUtils.addNameNormalizationConfiguration("max.name.tokens.length", "5");
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/iit-kgp/out/2019.Oct.22.22.25.09.iitkgp.tentative.final.v5/2019.Oct.22.22.25.09.iitkgp.tentative.final.v5.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/iit-kgp/tentative.final";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/iit-kgp/tentative.final";
		String name = "iitkgp.tentative.final.v5";
		
		String mergeFile = "/home/dspace/debasis/NDL/NDL_sources/iit-kgp/out/merged.accession.items.csv";

		IITKGPCuration1 p = new IITKGPCuration1(input, logLocation, outputLocation, name);
		p.accessions = NDLDataUtils.loadKeyValue(mergeFile);
		p.correctData();
		
		System.out.println("Done.");
	}
}