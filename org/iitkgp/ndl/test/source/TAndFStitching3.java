package org.iitkgp.ndl.test.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.ConfigurationData;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.xml.sax.SAXException;

public class TAndFStitching3 extends NDLSIPCorrectionContainer {

	class StitchingDetail {
		String handle;
		String title;
		String journal;
		String volume;
		String issue;
		String order;
		String collection;
		
		String journalID;
		String volID;
		String issueID;
		String volKey;
		String issueKey;
		
		public StitchingDetail(String handle, String title, String journal, String volume, String issue, String order) {
			this.handle = handle;
			this.title = title;
			this.journal = journal;
			this.volume = volume;
			this.issue = issue;
			this.order = order;
			
			volKey = journal + ":" + volume;
			issueKey = journal + ":" + volume + ":" + issue;
		}
	}
	
	List<StitchingDetail> items = new ArrayList<StitchingDetail>();
	Map<String, String> journalHandles = new HashMap<String, String>();
	Map<String, String> volumeHandles = new HashMap<String, String>();
	Map<String, String> issueHandles = new HashMap<String, String>();
	// journal nodes already exist
	Map<String, NDLStitchingNode> jnodes = new HashMap<String, NDLStitchingNode>();
	
	// construction
	public TAndFStitching3(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	// correction
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		String journal = NDLDataUtils.removeMultipleSpaces(target.getSingleValue("lrmi.educationalRole"));
		String title = NDLDataUtils.removeMultipleSpaces(target.getSingleValue("dc.title"));
		if(StringUtils.equals(journal, title)) {
			// journal
			jnodes.put(journal, new NDLStitchingNode(target));
			return false;
		}
		
		String handle = target.getId();
		int p = handle.indexOf('/');
		String prefix = handle.substring(0, p);
		String id = handle.substring(p + 1);
		
		String tokens[] = target.getSingleValue("dc.identifier.citation").split("/");
		
		String volume = tokens[0];
		String issue = tokens[1];
		String order = tokens[2];
		
		String journalID = journalHandles.get(journal);
		if(StringUtils.isBlank(journalID)) {
			String key = "journals." + ConfigurationData.escapeDot(journal);
			if(containsMappingKey(key)) {
				journalID = getMappingKey(key + ".Handle");
				journalHandles.put(journal, journalID);
			} else {
				// cross check
				throw new IllegalStateException("ERROR[" + handle + "]: " + journal + " is not found.");
			}
		}
		
		return true;
	}
	
	// stitching node detail
	class NDLStitchingNode {
		SIPDataItem item = null;
		Map<String, NDLStitchingNode> children = new LinkedHashMap<String, NDLStitchingNode>();
		NDLStitchingNode parent = null;
		
		public NDLStitchingNode() {
			// default
		}
		
		public NDLStitchingNode(SIPDataItem item) {
			this.item = item;
		}
		
		public NDLStitchingNode(String handle) throws SAXException, IOException {
			// create by handle ID
			item = NDLDataUtils.createBlankSIP(handle);
		}
		
		public NDLStitchingNode(String handle, NDLStitchingNode parent) throws SAXException, IOException {
			// create by handle ID
			item = NDLDataUtils.createBlankSIP(handle, true);
			this.parent = parent;
		}
		
		void resetChildren() {
			children = new LinkedHashMap<String, NDLStitchingNode>();
		}
		
		void addChild(String key, NDLStitchingNode child) {
			children.put(key, child);
		}
		
		NDLStitchingNode getChildByKey(String key) {
			return children.get(key);
		}
		
		void addValue(String field, String ... values) {
			item.add(field, values);
		}
		
		void addValue(String field, Collection<String> values) {
			item.add(field, values);
		}
		
		void setFolder(String folder) {
			item.setFolder(folder);
		}
		
		String getSingleValue(String field) {
			return item.getSingleValue(field);
		}
		
		List<String> getValue(String field) {
			return item.getValue(field);
		}
		
		SIPDataItem getItem() {
			return item;
		}
	}
}