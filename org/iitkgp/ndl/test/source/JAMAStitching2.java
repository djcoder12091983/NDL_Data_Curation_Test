package org.iitkgp.ndl.test.source;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.data.hierarchy.NDLRelation;
import org.iitkgp.ndl.relation.HasPart;
import org.iitkgp.ndl.util.NDLDataUtils;

import com.opencsv.CSVReader;

// stitching
public class JAMAStitching2 extends NDLSIPCorrectionContainer {
	
	static final String HANDLE_PREFIX = "jamanetwork/";
	
	Map<String, List<NDLRelation>> relations = new HashMap<String, List<NDLRelation>>();
	String relationFile;
	int hpc = 0;

	public JAMAStitching2(String input, String logLocation, String outputLocation, String name, String relationFile) {
		super(input, logLocation, outputLocation, name);
		this.relationFile = relationFile;
	}
	
	@Override
	public void preProcessData() throws Exception {
		super.preProcessData(); // super call
		// preparing has-part
		System.out.println("Preparing has-part relations....");
		
		CSVReader relationr = NDLDataUtils.readCSV(new File(relationFile));
		String tokens[];
		String previd = null;
		List<NDLRelation> crelations = new ArrayList<NDLRelation>();
		while((tokens = relationr.readNext()) != null) {
			String pid = tokens[0];
			if(!StringUtils.equals(pid, previd)) {
				// new PID
				if(!crelations.isEmpty()) {
					relations.put(previd, crelations);
					crelations = new ArrayList<NDLRelation>(); // reset
				}
			}
			
			// child relation
			crelations.add(new NDLRelation(tokens[1], tokens[2]));
			
			previd = pid;
		}
		
		// last if available
		if(!crelations.isEmpty()) {
			relations.put(previd, crelations);
		}
		
		relationr.close();
		
		System.out.println("Preparing has-part relations Done.");
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		String id = NDLDataUtils.getHandleSuffixID(target.getId());
		
		if(relations.containsKey(id)) {
			List<NDLRelation> crelations = relations.get(id);
			List<HasPart> parts = new ArrayList<HasPart>(crelations.size());
			for(NDLRelation rel : crelations) {
				String h = rel.getHandle();
				boolean i = h.startsWith("V");
				parts.add(new HasPart(rel.getTitle(), HANDLE_PREFIX + h, i, !i));
			}
			
			String hpjson = NDLDataUtils.serializeHasPart(parts);
			log("hierarchy.haspart", id + " => " + hpjson);
			target.add("dc.relation.haspart", hpjson);
			
			hpc++;
		}
		
		return true;
	}
	
	@Override
	protected void intermediateProcessHandler() {
		super.intermediateProcessHandler(); // super call
		System.out.println("Has-part added to: " + hpc);
	}
	
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/JAMA/2019.May.21.15.32.09.JAMA.stitching.v1.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/JAMA/logs";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/JAMA/out";
		String name = "JAMA.stitching.v2";
		
		String relationFile = "/home/dspace/debasis/NDL/NDL_sources/JAMA/hierarchy.data.csv";
		
		JAMAStitching2 p = new JAMAStitching2(input, logLocation, outputLocation, name, relationFile);
		p.turnOffLoadHierarchyFlag();
		p.turnOffControlFieldsValidationFlag();
		p.addTextLogger("hierarchy.haspart");
		p.processData();
		
		System.out.println("Done.");
	}
}