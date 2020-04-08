package org.iitkgp.ndl.test.source;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.stitch.AbstractNDLSIPStitchingContainer;
import org.iitkgp.ndl.data.correction.stitch.NDLStitchHierarchy;
import org.iitkgp.ndl.data.correction.stitch.NDLStitchHierarchyNode;

public class ComPADREStitchingTest extends AbstractNDLSIPStitchingContainer {
	
	public ComPADREStitchingTest(String input, String logLocation, String outputLocation, String logicalName)
			throws Exception {
		super(input, logLocation, outputLocation, logicalName);
	}

	protected NDLStitchHierarchy hierarchy(SIPDataItem target) throws Exception {
		// TODO Auto-generated method stub

		String temporal = target.getSingleValue("dc.coverage.temporal");
		
		if (StringUtils.isBlank(temporal)) {
			return null;
		}
		NDLStitchHierarchy h = new NDLStitchHierarchy();
		NDLStitchHierarchyNode top = new NDLStitchHierarchyNode("ComPADRE", "ComPADRE");
		top.addMetadata("dc.description.searchVisibility", "false");
		h.add(top);

		if (!StringUtils.startsWith(temporal, "Physics to Go:")) {
			NDLStitchHierarchyNode l1 = new NDLStitchHierarchyNode(temporal, temporal);
			if (temporal.equals("PhysPort")) {
				l1.addMetadata("dc.description.searchVisibility", "true");
				l1.addMetadata("dc.identifier.uri", "https://www.physport.org/");
			} else if (temporal.equals("Physics Teacher Education Coalition (PhysTEC)")) {
				l1.addMetadata("dc.description.searchVisibility", "true");
				l1.addMetadata("dc.identifier.uri", "https://www.phystec.org/");
			} else if (temporal.equals("Physical Sciences Resource Center (PSRC)")) {
				l1.addMetadata("dc.description.searchVisibility", "true");
				l1.addMetadata("dc.identifier.uri", "https://psrc.aapt.org/");
			} else {
				l1.addMetadata("dc.description.searchVisibility", "false");
			}
			h.add(l1);
		} else {
			NDLStitchHierarchyNode l1 = new NDLStitchHierarchyNode("Physics to Go", "Physics to Go");
			if (temporal.equals("PhysPort")) {
				l1.addMetadata("dc.description.searchVisibility", "true");
				l1.addMetadata("dc.identifier.uri", "https://www.physport.org/");
			} else if (temporal.equals("Physics Teacher Education Coalition (PhysTEC)")) {
				l1.addMetadata("dc.description.searchVisibility", "true");
				l1.addMetadata("dc.identifier.uri", "https://www.phystec.org/");
			} else if (temporal.equals("Physical Sciences Resource Center (PSRC)")) {
				l1.addMetadata("dc.description.searchVisibility", "true");
				l1.addMetadata("dc.identifier.uri", "https://psrc.aapt.org/");
			} else {
				l1.addMetadata("dc.description.searchVisibility", "false");
			}
			h.add(l1);

			String l2Title = temporal.split("\\:")[1].trim();

			NDLStitchHierarchyNode l2 = new NDLStitchHierarchyNode(l2Title, l2Title);
			if (temporal.equals("PhysPort")) {
				l2.addMetadata("dc.description.searchVisibility", "true");
				l2.addMetadata("dc.identifier.uri", "https://www.physport.org/");
			} else if (temporal.equals("Physics Teacher Education Coalition (PhysTEC)")) {
				l2.addMetadata("dc.description.searchVisibility", "true");
				l2.addMetadata("dc.identifier.uri", "https://www.phystec.org/");
			} else if (temporal.equals("Physical Sciences Resource Center (PSRC)")) {
				l2.addMetadata("dc.description.searchVisibility", "true");
				l2.addMetadata("dc.identifier.uri", "https://psrc.aapt.org/");
			} else {
				l2.addMetadata("dc.description.searchVisibility", "false");
			}
			h.add(l2);
		}

		return h;
	}

	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/2019.Dec.26.11.42.07.Compadre_curation.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/out";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/out";
		String logicalName = "Compadre_Stitch";
		ComPADREStitchingTest t = new ComPADREStitchingTest(input, logLocation, outputLocation, logicalName);
		t.turnOnLogRelationDetails();
		t.turnOnOrphanNodesLogging();
		t.turnOnFullHandleIDConsideration();
		t.turnOnDuplicateHandlesChecking();
		t.setDefaultAbbreviatedHandleIDGenerationStrategy(1, 2,3);
		t.stitch();
		System.out.println("End..");
	}

}