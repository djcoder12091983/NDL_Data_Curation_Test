package org.iitkgp.ndl.test;

import java.util.Collection;
import java.util.Map;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.stitch.AbstractNDLSIPStitchingContainer;
import org.iitkgp.ndl.data.correction.stitch.NDLStitchHierarchy;
import org.iitkgp.ndl.data.correction.stitch.NDLStitchHierarchyNode;

public class ASMEStitchingTest extends AbstractNDLSIPStitchingContainer {

	public ASMEStitchingTest(String input, String logLocation, String outputLocation, String logicalName)
			throws Exception {
		super(input, logLocation, outputLocation, logicalName);
	}
	
	@Override
	protected boolean preStitchCorrection(SIPDataItem item) throws Exception {
		if(item.getId().equals("asme/64_gtp_3")) {
			// to check whether mixed access-rights come for a virtual node
			item.updateSingleValue("dc.rights.accessRights", "subscribed");
		}
		
		return true;
	}
	
	@Override
	protected NDLStitchHierarchy hierarchy(SIPDataItem item) throws Exception {
		
		String isPartofSeries = item.getSingleValue("dc.relation.ispartofseries");
		String lrmiEduUse = item.getSingleValue("lrmi.educationalUse");
		String titleAlt = item.getSingleValue("dc.title.alternative");
		String accessr = item.getSingleValue("dc.rights.accessRights");
		
		NDLStitchHierarchy h = new NDLStitchHierarchy();
		h.add(new NDLStitchHierarchyNode("ASME ROOT", "ASME ROOT"));
		
		NDLStitchHierarchyNode l2 = new NDLStitchHierarchyNode(isPartofSeries, isPartofSeries);
		l2.addAdditionalData("AR", accessr);
		h.add(l2);
		
		NDLStitchHierarchyNode l3 = new NDLStitchHierarchyNode(lrmiEduUse, lrmiEduUse);
		l3.addAdditionalData("AR", accessr);
		h.add(l3);
		
		NDLStitchHierarchyNode l4 = new NDLStitchHierarchyNode(titleAlt, titleAlt);
		l4.addAdditionalData("AR", accessr);
		h.add(l4);
		
		return h;
	}
	
	// prepare metadata for virtual node
	@Override
	protected void prepareIntermediateNodeMetadata(SIPDataItem item, Map<String, Collection<String>> additionalData)
			throws Exception {
		if(additionalData.containsKey("AR") && additionalData.get("AR").size() > 1) {
			// cross check Access-rights
			System.err.println("ITEM(" + item.getId() + "): " + item.getSingleValue("dc.title"));
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/2019.Dec.18.15.50.27.ASME.V2.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/out";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/out";
		String logicalName = "asme.stitich.test";
		
		ASMEStitchingTest p = new ASMEStitchingTest(input, logLocation, outputLocation, logicalName);
		p.setDefaultAbbreviatedHandleIDGenerationStrategy(1, 2, 3, 4);
		p.turnOnAdditionalMetadataMergingOnIntermediateNode();
		p.stitch();
		
		System.out.println("done.");
	}
}