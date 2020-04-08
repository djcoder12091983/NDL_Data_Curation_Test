package org.iitkgp.ndl.test.source;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.stitch.AbstractNDLSIPExistingNodeLinkingStitchingContainer;
import org.iitkgp.ndl.data.correction.stitch.SimpleStitchingInformation;
import org.iitkgp.ndl.util.NDLDataUtils;

// testing of AbstractNDLSIPExistingNodeLinkingStitchingContainer
public class RRB_GDStitching extends AbstractNDLSIPExistingNodeLinkingStitchingContainer {
	
	public RRB_GDStitching(String input, String logLocation, String outputLocation, String logicalName) {
		super(input, logLocation, outputLocation, logicalName);
	}
	
	// hard coded conditional order extraction logic
	String order(SIPDataItem item) {
		// try one by one and returns first not NULL
		String o1 = item.getSingleValue("ndl.examination.sequence");
		String o2 = item.getSingleValue("ndl.questionPaperPart.sequence");
		String o3 = item.getSingleValue("ndl.question.sequence");
		String o4 = item.getSingleValue("ndl.solution.type");
		if(StringUtils.isNotBlank(o1)) {
			return o1;
		} else if(StringUtils.isNotBlank(o2)) {
			return o2;
		} else if(StringUtils.isNotBlank(o3)) {
			return o3;
		} else {
			if(StringUtils.equalsIgnoreCase(o4, "primary")) {
				return "1";
			} else if(StringUtils.equalsIgnoreCase(o4, "secondary")) {
				return "2";
			} else {
				return null;
			}
		}
	}
	
	// stitching information
	@Override
	protected SimpleStitchingInformation hierarchy(SIPDataItem sip) {
		String title = sip.getSingleValue("dc.title.alternative"); // stitching title
		String parenth = NDLDataUtils.NVL(sip.getSingleValue("ndl.relation.isPartOf"),
				sip.getSingleValue("ndl.relation.solutionOf")); // parent handle ID
		String order = NDLDataUtils.NVL(order(sip), "-1"); // order information, default is -1
		if(StringUtils.isBlank(title)) {
			// orphan node
			return null;
		} else {
			// sufficient stitching information
			return new SimpleStitchingInformation(title, parenth, Integer.parseInt(order));
		}
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/RRB-GD/2020.Feb.04.14.44.59.RRB_GDV2.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/RRB-GD/out";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/RRB-GD/out";
		String logicalName = "RRB_GD.stitich.test";
		
		RRB_GDStitching p = new RRB_GDStitching(input, logLocation, outputLocation, logicalName);
		p.turnOnLogRelationDetails();
		p.turnOnOrphanNodesLogging();
		p.stitch();
		
		// ================ Done ================
	}
}