package org.iitkgp.ndl.test.source;

import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.iitkgp.ndl.data.DataOrder;
import org.iitkgp.ndl.data.DataType;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.stitch.AbstractNDLSIPStitchingContainer;
import org.iitkgp.ndl.data.correction.stitch.NDLStitchHierarchy;
import org.iitkgp.ndl.data.correction.stitch.NDLStitchHierarchyNode;

// KH sip stitching container
public class KHSIPStitchingContainer extends AbstractNDLSIPStitchingContainer {

	public KHSIPStitchingContainer(String input, String logLocation, String outputLocation, String logicalName)
			throws Exception {
		super(input, logLocation, outputLocation, logicalName);
	}
	
	// last ddc numeric code
	String ddc(SIPDataItem item) {
		List<String> ddc = item.getValue("dc.subject.ddc");
		if(ddc.isEmpty()) {
			return null;
		} else {
			String t = ddc.get(ddc.size() - 1);
			return t.substring(0, t.indexOf(':'));
		}
	}
	
	Locale locale = Locale.getDefault();
	
	// hierarchy tree
	@Override
	protected NDLStitchHierarchy hierarchy(SIPDataItem item) throws Exception {
		
		String subject = item.getSingleValue("dc.subject");
		String bench = item.getSingleValue("dc.identifier.other:lawCaseBench");
		String date = item.getSingleValue("dc.date.other:lawJudgmentDate");
		
		if(StringUtils.isNotBlank(subject) && StringUtils.isNotBlank(bench) && StringUtils.isNotBlank(date)) {
			
			String lrt = item.getSingleValue("lrmi.learningResourceType");
			String ddc = ddc(item);
			
			// full path exists
			// preparation of hierarchy
			NDLStitchHierarchy h = new NDLStitchHierarchy();
			h.add(new NDLStitchHierarchyNode("KH_ROOT_12345", "High Court of Karnataka", true));
			
			String tsubject = subject.replaceAll("( |\\(|\\))+", "_");
			NDLStitchHierarchyNode snode = new NDLStitchHierarchyNode(tsubject, subject);
			snode.addAdditionalData("lrt", lrt);
			snode.addAdditionalData("ddc", ddc);
			h.add(snode);
			
			NDLStitchHierarchyNode bnode = new NDLStitchHierarchyNode(bench, "Case Bench: " + bench);
			bnode.addAdditionalData("lrt", lrt);
			bnode.addAdditionalData("ddc", ddc);
			h.add(bnode);
			
			Calendar cal = Calendar.getInstance();
			cal.setTime(DateUtils.parseDate(date, "yyyy-MM-dd"));
			
			String year = String.valueOf(cal.get(Calendar.YEAR));
			NDLStitchHierarchyNode ynode = new NDLStitchHierarchyNode(year, "Law Judgment " + year);
			/*if(year=="")
			System.out.println("year====="+year);*/
			ynode.setOrder(year);
			ynode.addAdditionalData("lrt", lrt);
			ynode.addAdditionalData("ddc", ddc);
			h.add(ynode);
			
			String month = cal.getDisplayName(Calendar.MONTH, Calendar.LONG, locale);
			String monthc = String.valueOf(cal.get(Calendar.MONTH));
			/*if(monthc == "" || month == "")
			System.out.println(monthc + " => " + month);*/
			NDLStitchHierarchyNode mnode = new NDLStitchHierarchyNode(monthc, month);
			mnode.setOrder(monthc);
			mnode.addAdditionalData("lrt", lrt);
			mnode.addAdditionalData("ddc", ddc);
			h.add(mnode);
			
			return h;
		} else {
			// non-stitching
			return null;
		}
	}
	
	// intermediate node METADATA
	@Override
	protected void addIntermediateNodeMetadata(SIPDataItem item, Map<String, String> additionalData) throws Exception {
		item.add("lrmi.learningResourceType", additionalData.get("lrt"));
		String ddc = additionalData.get("ddc");
		if(StringUtils.isNotBlank(ddc)) {
			addNodeValue(item, "dc.subject.ddc", ddc);
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/KH/in/2019.Sep.26.17.59.17.KH.v10.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/KH/out";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/KH/out";
		String logicalName = "kh.final.stitich";
		
		KHSIPStitchingContainer kc = new KHSIPStitchingContainer(input, logLocation, outputLocation, logicalName);
		/*kc.turnOnLogRelationDetails();
		kc.setLeafIsPartLogging(-1); // all logging leaf is-part*/
		kc.turnOnOrphanNodesLogging();
		kc.turnOnDuplicateHandlesChecking();
		//kc.turnOffLogRelationDetails();
		kc.addLevelOrder(4, DataOrder.DESCENDING, DataType.INTEGER);
		kc.addLevelOrder(5, DataType.INTEGER);
		//kc.addTextLogger("orphan.nodes");
		
		//kc.addGlobalMetadata("dc.source", "High Court of Karnataka");
		kc.addGlobalMetadata("dc.type", "text");
		kc.addGlobalMetadata("dc.language.iso", "eng");
		kc.addGlobalMetadata("dc.description.searchVisibility", "false");
		kc.addGlobalMetadata("dc.rights.accessRights", "open");
		
		kc.stitch();
		
		System.out.println("Done.");
	}
}