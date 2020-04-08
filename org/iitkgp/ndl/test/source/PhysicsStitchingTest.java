package org.iitkgp.ndl.test.source;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.stitch.AbstractNDLSIPStitchingContainer;
import org.iitkgp.ndl.data.correction.stitch.NDLStitchHierarchy;
import org.iitkgp.ndl.data.correction.stitch.NDLStitchHierarchyNode;
import org.iitkgp.ndl.data.correction.stitch.NDLStitchHierarchyTreeNode;
import org.iitkgp.ndl.data.correction.stitch.comparator.StandardNDLStitchComparator;
import org.iitkgp.ndl.util.NDLDataUtils;

public class PhysicsStitchingTest extends AbstractNDLSIPStitchingContainer {
	
	Set<String> type2 = new HashSet<>(4);
	Set<String> type3 = new HashSet<>(4);

	public PhysicsStitchingTest(String input, String logLocation, String outputLocation, String logicalName)
			throws Exception {
		super(input, logLocation, outputLocation, logicalName);
		
		// type2
		type2.add("Concept Builders");
		type2.add("Physics Interactives");
		type2.add("The Calculator Pad Problemset");
		type2.add("The Review Session");
		type2.add("Video Tutorial");
		
		// type3
		type3.add("Curriculum Corner");
		type3.add("Reasoning");
		type3.add("Teacher Toolkits");
		type3.add("NGSS Corner");
		type3.add("Multimedia Studios");
		type3.add("The Laboratory");
	}

	@Override
	protected String itemOrder(SIPDataItem item) {
		if(item.getSingleValue("dc.coverage.spatial").equals("Physics Tutorial")) {
			// special case
			return item.getSingleValue("ndl.sourceMeta.additionalInfo:authorInfo");
		}

		String h = NDLDataUtils.getHandleSuffixID(item.getId());
		int p = h.lastIndexOf('_');
		String d = h.substring(p + 1);
		if(NumberUtils.isDigits(d)) {
			return String.valueOf(d); // last digit
		}else {
			// default
			return String.valueOf(Integer.MAX_VALUE);
		}
	}

	protected NDLStitchHierarchy hierarchy(SIPDataItem target) throws Exception {
		NDLStitchHierarchy h = new NDLStitchHierarchy();
		String spatial = target.getSingleValue("dc.coverage.spatial");
		String pub = target.getSingleValue("dc.publisher");
		String temp = target.getSingleValue("dc.coverage.temporal");
		String doctype = target.getSingleValue("ndl.sourceMeta.additionalInfo:DocumentType");
		String degreetyp = target.getSingleValue("ndl.sourceMeta.additionalInfo:DegreeType");

		if(StringUtils.equals(spatial, "ACT Test Preparation")) {
			// type 1
			if(NDLDataUtils.allNotBlank(pub)) {
				h.add(new NDLStitchHierarchyNode(spatial, spatial));
				h.add(new NDLStitchHierarchyNode(pub, pub));
			}
		} else if(type2.contains(spatial)) {
			// type 2
			if(NDLDataUtils.allNotBlank(pub, temp)) {
				h.add(new NDLStitchHierarchyNode(spatial, spatial));
				h.add(new NDLStitchHierarchyNode(temp, temp));
				NDLStitchHierarchyNode l3 = new NDLStitchHierarchyNode(pub, pub);
				l3.addAdditionalData("t2", "true");
				h.add(l3);
			}
		} else if(type3.contains(spatial)) {
			// type 3
			if (NDLDataUtils.allNotBlank(temp)) {
				h.add(new NDLStitchHierarchyNode(spatial, spatial));
				h.add(new NDLStitchHierarchyNode(temp, temp));
			}
		} else if(StringUtils.equals(spatial, "Physics Help")) {
			// type 4
				h.add(new NDLStitchHierarchyNode(spatial, spatial));
		} else if(StringUtils.equals(spatial, "Physics Tutorial")) {
			// type 5
			if(NDLDataUtils.allNotBlank(temp, doctype, degreetyp)) {
				h.add(new NDLStitchHierarchyNode(spatial, spatial));
				NDLStitchHierarchyNode l2 = new NDLStitchHierarchyNode(temp, temp);
				l2.addAdditionalData("pt", "true");
				h.add(l2);
				NDLStitchHierarchyNode l3 = new NDLStitchHierarchyNode(doctype + ": " + degreetyp,
						doctype + ": " + degreetyp);
				// order information
				String l3order = orderFromlast(target.getSingleValue("ndl.sourceMeta.additionalInfo:DocumentType"));
				l3.setOrder(NDLDataUtils.NVL(l3order, String.valueOf(Integer.MAX_VALUE)));

				h.add(l3);
			}
		}
		
		return h;
	}
	
	// type order informations
	List<String> getType2OrderInformations(String o) {
		List<String> oi = new LinkedList<String>();
		
		int l = o.length();
		StringBuilder t = new StringBuilder();
		for(int i = 0; i < l; i++) {
			char ch = o.charAt(i);
			if(!CharUtils.isAsciiNumeric(ch)) {
				// add scanned number
				if(t.length() > 0) {
					oi.add(t.toString());
					t = new StringBuilder(); // reset
				}
			} else {
				// number
				t.append(ch);
			}
		}
		
		// last number to add if any
		if(t.length() > 0) {
			oi.add(t.toString());
		}
		
		if(oi.isEmpty()) {
			// default case
			oi.add(String.valueOf(Integer.MIN_VALUE));
		}
		
		return oi;
	}

	String orderFromlast(String value) {
		if (StringUtils.isNotBlank(value)) {
			StringBuilder o = new StringBuilder();
			// order from last
			for (int i = value.length() - 1; i >= 0; i--) {
				char ch = value.charAt(i);
				if (CharUtils.isAsciiNumeric(ch)) {
					o.append(ch);
				} else {
					break;
				}
			}
			return o.reverse().toString();
		}
		return null;
	}

	@Override
	protected boolean intermediateNodeCustomItemsOrdering(NDLStitchHierarchyTreeNode parent,
			List<NDLStitchHierarchyNode> children) {
		NDLStitchHierarchyNode d = parent.getData();
		Map<String, String> ad = d.getAdditionalData();
		String t = d.getTitle();
		if (ad.containsKey("t2")) {
			// type 2 ordering
			Collections.sort(children, new Comparator<NDLStitchHierarchyNode>() {
				// comparison logic
				@Override
				public int compare(NDLStitchHierarchyNode data1, NDLStitchHierarchyNode data2) {
					/*System.out.println(data1.getHandle());
					System.out.println(data2.getHandle());*/
					Iterator<String> o1 = getType2OrderInformations(NDLDataUtils.getHandleSuffixID(data1.getHandle()))
							.iterator();
					Iterator<String> o2 = getType2OrderInformations(NDLDataUtils.getHandleSuffixID(data2.getHandle()))
							.iterator();
					while(o1.hasNext() && o2.hasNext()) {
						int c = Integer.valueOf(o1.next()).compareTo(Integer.valueOf(o2.next()));
						if(c != 0) {
							// not equal
							return c;
						}
					}
					if(o1.hasNext()) {
						return 1; // o1 greater than o2
					} else if(o2.hasNext()) {
						return -1; // o1 less than o2
					} else {
						// equal
						return 0;
					}
				}
			});

			return true; // ordering done here
		} else if (ad.containsKey("pt")) {
			// specific `Physics Tutorial` ordering
			StandardNDLStitchComparator.sortHasParts(children, StandardNDLStitchComparator.NUMERIC_ASCENDING);
			return true; // ordering done here
		}

		return false;
	}

	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/Physics/in/2019.Nov.13.17.00.24.Physics_class_First_curation.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/Physics/out";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/Physics/out";
		String logicalName = "Physics_Stitch";
		
		PhysicsStitchingTest t = new PhysicsStitchingTest(input, logLocation, outputLocation, logicalName);
		
		t.turnOnLogRelationDetails();
		t.setLeafIsPartLogging(-1); // all logging leaf is-part
		t.turnOnOrphanNodesLogging();
		t.turnOnDuplicateHandlesChecking();
		t.setDefaultAbbreviatedHandleIDGenerationStrategy(1, 2, 3, 4); // handle ID abbreviation strategy
		
		t.addGlobalMetadata("dc.rights.accessRights", "open");
		t.addGlobalMetadata("dc.description.searchVisibility", "false");

		// custom leaf comparator
		t.setLeafComparator(StandardNDLStitchComparator.NUMERIC_ASCENDING);

		t.stitch(); // stitching starts
		
		System.out.println("done");

	}

}
