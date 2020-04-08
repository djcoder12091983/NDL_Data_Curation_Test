package org.iitkgp.ndl.test.source;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.DataOrder;
import org.iitkgp.ndl.data.DataType;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.stitch.AbstractNDLSIPStitchingContainer;
import org.iitkgp.ndl.data.correction.stitch.NDLStitchHierarchy;
import org.iitkgp.ndl.data.correction.stitch.NDLStitchHierarchyNode;
import org.iitkgp.ndl.util.NDLDataUtils;

public class DataStitchingASCEV1 extends AbstractNDLSIPStitchingContainer {

	public DataStitchingASCEV1(String input, String logLocation, String outputLocation, String logicalName)
			throws Exception {
		super(input, logLocation, outputLocation, logicalName);
	}

	@Override
	protected NDLStitchHierarchy hierarchy(SIPDataItem target) throws Exception {

		String journal = target.getSingleValue("dc.identifier.other:journal");
		String volume = target.getSingleValue("dc.identifier.other:volume");
		String issue = target.getSingleValue("dc.identifier.other:issue");
		String year = target.getSingleValue("dc.publisher.date");
		String issn = target.getSingleValue("dc.identifier.issn");
		String eissn = target.getSingleValue("dc.identifier.other:eissn");

		NDLStitchHierarchy root = new NDLStitchHierarchy();

		NDLStitchHierarchyNode jnode = new NDLStitchHierarchyNode(
				NDLDataUtils.NVL(issn, NDLDataUtils.splitByInitialLetter(journal)), journal);
		jnode.addAdditionalData("access", "subscribed");
		jnode.addAdditionalData("issn", issn);
		jnode.addAdditionalData("eissn", eissn);
		jnode.addAdditionalData("visi", "true");
		jnode.addAdditionalData("edu", "ug_pg");
		jnode.addAdditionalData("eduUse", "research");
		jnode.addAdditionalData("lrt", "journal");
		jnode.addAdditionalData("ageR", "18-22|22+");
		jnode.addAdditionalData("lang", "eng");
		root.add(jnode);

		NDLStitchHierarchyNode vnode = new NDLStitchHierarchyNode(volume,
				"Year: " + year.substring(0, 4) + " Volume: " + volume);
		vnode.setOrder(volume);
		vnode.addAdditionalData("visi", "false");
		root.add(vnode);

		NDLStitchHierarchyNode inode = new NDLStitchHierarchyNode(issue,
				"Year: " + year.substring(0, 4) + " Volume: " + volume + " Issue: " + issue);
		inode.setOrder(issue);
		inode.addAdditionalData("visi", "false");
		root.add(inode);

		return root;
	}

	@Override
	protected String itemOrder(SIPDataItem target) {

		return NDLDataUtils.NVL(target.getSingleValue("lrmi.educationalRole"), String.valueOf(Integer.MAX_VALUE));
	}

	@Override
	protected void addIntermediateNodeMetadata(SIPDataItem target, Map<String, String> additionalData)
			throws Exception {
		target.add("lrmi.learningResourceType", additionalData.get("lrt"));
		target.add("dc.rights.accessRights", additionalData.get("access"));
		target.add("dc.description.searchVisibility", additionalData.get("visi"));
		target.add("lrmi.educationalAlignment.educationalLevel", additionalData.get("edu"));
		target.add("lrmi.educationalUse", additionalData.get("eduUse"));
		if (StringUtils.isNotBlank(additionalData.get("ageR"))) {
			target.add("lrmi.typicalAgeRange", additionalData.get("ageR").split("\\|"));
		}
		target.add("dc.identifier.issn", additionalData.get("issn"));
		target.add("dc.identifier.other:eissn", additionalData.get("eissn"));
		target.add("dc.language.iso", additionalData.get("lang"));
	}

	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/ASCE/in/2019.Oct.16.17.07.55.ASCE.V3.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/ASCE/out";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/ASCE/out";
		String logicalName = "AESC.Stitch.v1";

		DataStitchingASCEV1 t = new DataStitchingASCEV1(input, logLocation, outputLocation, logicalName);

		t.turnOnLogRelationDetails();
		t.turnOnOrphanNodesLogging();
		t.turnOnDuplicateHandlesChecking();

		t.addLevelOrder(2, DataOrder.DESCENDING, DataType.INTEGER);
		t.addLevelOrder(3, DataOrder.DESCENDING, DataType.INTEGER);
		t.addLevelOrder(4, DataOrder.ASCENDING, DataType.INTEGER);

		t.stitch();
		System.out.println("End..");
	}

}
