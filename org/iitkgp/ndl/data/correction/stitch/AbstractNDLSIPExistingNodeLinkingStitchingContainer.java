package org.iitkgp.ndl.data.correction.stitch;

import static org.iitkgp.ndl.data.correction.stitch.StitchingConstants.HAS_PART_FIELD;
import static org.iitkgp.ndl.data.correction.stitch.StitchingConstants.IS_PART_FIELD;
import static org.iitkgp.ndl.data.correction.stitch.StitchingConstants.ORPHAN_NODES_LOGGER;
import static org.iitkgp.ndl.data.correction.stitch.StitchingConstants.RELATION_LOGGER;
import static org.iitkgp.ndl.data.correction.stitch.StitchingConstants.RELATION_LOGGER_LONG;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.context.NDLDataValidationContext;
import org.iitkgp.ndl.data.DataOrder;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.compress.CompressedFileMode;
import org.iitkgp.ndl.data.container.AbstractDataContainer;
import org.iitkgp.ndl.data.container.DataContainerNULLConfiguration;
import org.iitkgp.ndl.data.correction.NDLAIPStitchingCorrection;
import org.iitkgp.ndl.data.correction.stitch.exception.NDLStitchInvalidHierarchyInformationException;
import org.iitkgp.ndl.data.iterator.DataSourceNULLConfiguration;
import org.iitkgp.ndl.data.iterator.SIPDataIterator;
import org.iitkgp.ndl.data.writer.NDLDataItemWriter;
import org.iitkgp.ndl.relation.HasPart;
import org.iitkgp.ndl.relation.IsPartOf;
import org.iitkgp.ndl.util.CommonUtilities;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * This class helps to stitch SIP with special form but very simple.
 * Already {@link AbstractNDLSIPStitchingContainer} class offers various patterns of stitching but
 * in this class from a item full hierarchy chain can be discovered and using this 
 * {@link AbstractNDLSIPStitchingContainer#turnOnExistingNodeLinking(java.io.File)} API we can link with existing nodes.
 * But some SIP comes with a special but very simple data pattern, all intermediate nodes are already created
 * and every item has it's immediate parent information instead of full hierarchy chain and order information if any.
 * Now this is typical case AIP stitching {@link NDLAIPStitchingCorrection}. But unfortunately sometimes some SIP
 * comes with this typical data pattern. So to support this this class has been introduced.
 * 
 * <pre>Notes: Let's make it simple.</pre>
 * <li>
 * <ul>Assumed all intermediate nodes are created and every nodes associated with immediate hierarchy information</ul>
 * <ul>No correction can be done other than simple stitching.</ul>
 * <ul>All sort of corrections item delete should be done using correction container</ul>
 * <ul>no data validation is done over here</ul>
 * </li>
 * 
 * Steps to do, for every item system needs.
 * <li>
 * <ul>Immediate parent full handle ID</ul>
 * <ul>Title used in stitching</ul>
 * <ul>
 * Order information if any. Order can only numeric and change the order use {@link DataOrder}.
 * If order is not available then random order is maintained.
 * </ul>
 * <li>
 * 
 * @see AbstractNDLSIPStitchingContainer
 * @author debasis
 */
public abstract class AbstractNDLSIPExistingNodeLinkingStitchingContainer
		extends AbstractDataContainer<DataContainerNULLConfiguration<DataSourceNULLConfiguration>> {
	
	long displayThresholdLimit = Long
			.parseLong(NDLConfigurationContext.getConfiguration("process.display.threshold.limit"));
	protected long processedCounter = 0; // how many data processed
	long start;
	
	String outputLocation; // output location
	String logicalName;
	
	boolean orphanNodesLogging = false;
	long orphanNodesCounter = 0;
	
	boolean logRelationDetails = false;
	long longJSONText = 500;
	
	// handle ID to title mapping
	Map<String, Map<String, String>> titles = new HashMap<>(2);
	
	// hierarchy tree details
	long bytes4TreeNodes = 0;
	Map<String, Map<String, SimpleStitchingNode>> tree = new HashMap<>(2);
	
	// add titles
	void add2titles(String handleid, String title) {
		String[] htokens = NDLDataUtils.splitHandleID(handleid);
		Map<String, String> detail = titles.get(htokens[0]);
		if(detail == null) {
			detail = new HashMap<>(2);
			bytes4TreeNodes += htokens[0].length();
			titles.put(htokens[0], detail);
		}
		bytes4TreeNodes += htokens[1].length() + title.length();
		detail.put(htokens[1], title);
	}
	
	// add hierarchy information to tree
	void add2tree(String parenth, String childh, int orderc) {
		String phtokens[] = NDLDataUtils.splitHandleID(parenth);
		String chtokens[] = NDLDataUtils.splitHandleID(childh);
		
		// parent to child
		Map<String, SimpleStitchingNode> detail = tree.get(phtokens[0]);
		if(detail == null) {
			detail = new HashMap<>(2);
			bytes4TreeNodes += phtokens[0].length();
			tree.put(phtokens[0], detail);
		}
		SimpleStitchingNode detail1 = detail.get(phtokens[1]);
		if(detail1 == null) {
			detail1 = new SimpleStitchingNode();
			bytes4TreeNodes += phtokens[1].length();
			detail.put(phtokens[1], detail1);
		}
		// assuming 64 bytes for integer
		bytes4TreeNodes += chtokens[0].length() + chtokens[1].length() + 64;
		detail1.add(chtokens[0], chtokens[1], orderc);
		
		// child to parent
		detail = tree.get(chtokens[0]);
		if(detail == null) {
			detail = new HashMap<>(2);
			bytes4TreeNodes += chtokens[0].length();
			tree.put(chtokens[0], detail);
		}
		detail1 = detail.get(chtokens[1]);
		if(detail1 == null) {
			detail1 = new SimpleStitchingNode();
			bytes4TreeNodes += chtokens[1].length();
			detail.put(chtokens[1], detail1);
		}
		bytes4TreeNodes += phtokens[0].length() + phtokens[1].length();
		detail1.setParent(phtokens[0], phtokens[1]);
	}
	
	// context setup
	void contextSetup() {
		System.out.println("Initializing context setup....");
		// context startup
		NDLConfigurationContext.init();
		// load validation context
		NDLDataValidationContext.init();
	}
	
	/**
	 * Track orphan nodes detail
	 */
	public void turnOnOrphanNodesLogging() {
		orphanNodesLogging = true;
	}
	
	/**
	 * Log linking detail
	 */
	public void turnOnLogRelationDetails() {
		logRelationDetails = true;
	}
	
	/**
	 * Constructor
	 * @param input source input, from where data to be processed
	 * @param logLocation logging location, where logging takes place during data processing
	 * @param outputLocation where the corrected data kept
	 * @param logicalName logical name for the output data
	 */
	public AbstractNDLSIPExistingNodeLinkingStitchingContainer(String input, String logLocation, String outputLocation,
			String logicalName) {
		super(input, logLocation);
		this.outputLocation = outputLocation;
		this.logicalName = logicalName;
		contextSetup(); // context setup
	}
	
	/**
	 * provides stitching information
	 * @param sip SIP data item from which stitching information to be extracted
	 * @return returns stitching detail
	 */
	protected abstract SimpleStitchingInformation hierarchy(SIPDataItem sip);
	
	// display status of process
	void displayStatus(String ... messages) {
		// display messages
		for(String message : messages) {
			System.out.println(message);
		}
		long intermediate = System.currentTimeMillis(); // intermediate time
		System.out.println(CommonUtilities.durationMessage(intermediate - start)); // duration message
	}
	
	// pass 1
	void pass1(SIPDataIterator reader) throws Exception {
		processedCounter = 0;
		// iterate data
		while(reader.hasNext()) {
			SIPDataItem sip = reader.next();
			
			String handleid = sip.getId();
			SimpleStitchingInformation hierarchy = hierarchy(sip);
			if(hierarchy != null) {
				// stitching detail available 
				// title mapping
				add2titles(handleid, hierarchy.getTitle());
				
				// add 2 tree
				String parenth = hierarchy.getParentFullHandleID();
				if(StringUtils.isNotBlank(parenth)) {
					// parent linking available
					add2tree(parenth, handleid, hierarchy.getOrder());
				}
			}
			
			// display processed item
			if(++processedCounter % displayThresholdLimit == 0) {
				displayStatus("Processed: " + processedCounter + ". Tree creation spaced used: "
						+ CommonUtilities.bytesMessage(bytes4TreeNodes * 2));
			}
		}

		// total statistics
		displayStatus("Processed: " + processedCounter + ". Tree creation spaced used: "
				+ CommonUtilities.bytesMessage(bytes4TreeNodes * 2));
	}
	
	// get stitching title by  handle id
	String stitchingTitle(String prefix, String suffix) {
		String errorm = prefix + '/' + suffix + " not valid. No title is attached with it.";
		if(!titles.containsKey(prefix)) {
			// error
			throw new NDLStitchInvalidHierarchyInformationException(errorm);
		} else {
			Map<String, String> detail = titles.get(prefix);
			if(!detail.containsKey(suffix)) {
				// error
				throw new NDLStitchInvalidHierarchyInformationException(errorm);
			}
			return detail.get(suffix);
		}
	}
	
	// is leaf or not, discovered from tree
	boolean isLeaf(String prefix, String suffix) {
		String errorm = prefix + '/' + suffix + " not valid. it's not associated with tree.";
		if(!tree.containsKey(prefix)) {
			// error
			throw new NDLStitchInvalidHierarchyInformationException(errorm);
		} else {
			Map<String, SimpleStitchingNode> detail = tree.get(prefix);
			if(!detail.containsKey(suffix)) {
				// error
				throw new NDLStitchInvalidHierarchyInformationException(errorm);
			} else {
				SimpleStitchingNode detail1 = detail.get(suffix);
				return detail1.children.isEmpty(); // no children that means it's leaf
			}
		}
	}
	
	// build has parts
	List<HasPart> buildHasparts(Map<String, Map<String, Integer>> children) {
		Map<Integer, List<String>> sortedh = new TreeMap<>();
		for(String cprefix : children.keySet()) {
			Map<String, Integer> detail = children.get(cprefix);
			for(String csuffix : detail.keySet()) {
				int order = detail.get(csuffix);
				String handle = cprefix + '/' + csuffix;
				List<String> handles = sortedh.get(order);
				if(handles == null) {
					// same order can associated with multiple handles
					handles = new ArrayList<>(2);
					sortedh.put(order, handles);
				}
				handles.add(handle);
			}
		}
		
		List<HasPart> hasparts = new ArrayList<>();
		for(int order : sortedh.keySet()) {
			for(String handle : sortedh.get(order)) {
				String htokens[] = NDLDataUtils.splitHandleID(handle);
				boolean leaf = isLeaf(htokens[0], htokens[1]);
				// TODO discuss whether visible false or true
				boolean visible = leaf;
				hasparts.add(new HasPart(stitchingTitle(htokens[0], htokens[1]), handle, !leaf, visible));
			}
		}
		
		return hasparts;
	}
	
	// log linking informations
	void relationLog(String handle, String text) throws Exception {
		if(logRelationDetails) {
			if(text.length() > longJSONText) {
				// long relation logger
				log(RELATION_LOGGER_LONG, handle + " => " + text);
			} else {
				// short relation logger
				log(RELATION_LOGGER, handle + " => " + text);
			}
		}
	}
	
	// stitching
	void stitch(SIPDataIterator reader, NDLDataItemWriter<SIPDataItem> writer) throws Exception {
		processedCounter = 0;
		// iterate data
		while(reader.hasNext()) {
			SIPDataItem sip = reader.next();
			
			String handleid = sip.getId();
			String htokens[] = NDLDataUtils.splitHandleID(handleid);
			Map<String, SimpleStitchingNode> detail = tree.get(htokens[0]);
			boolean orphan = false;
			if(detail != null) {
				// query by prefix
				SimpleStitchingNode detail1 = detail.get(htokens[1]);
				if(detail1 != null) {
					// part of stitching
					String ph1 = detail1.parentHandlePrefix;
					String ph2 = detail1.parentHandleSuffix;
					if(StringUtils.isNotBlank(ph1) && StringUtils.isNotBlank(ph2)) {
						// ispart available
						IsPartOf ispart = new IsPartOf(ph1 + '/' + ph2, stitchingTitle(ph1, ph2));
						String ipartJson = NDLDataUtils.serializeIsPartOf(ispart);
						// logging
						relationLog(htokens[1], ipartJson);
						
						sip.updateSingleValue(IS_PART_FIELD, ipartJson);
					}
					
					Map<String, Map<String, Integer>> children = detail1.children;
					if(children != null && !children.isEmpty()) {
						// haspart available
						List<HasPart> hparts = buildHasparts(children);
						String hpartJson = NDLDataUtils.serializeHasPart(hparts);
						// logging
						relationLog(htokens[1], hpartJson);
						
						sip.updateSingleValue(HAS_PART_FIELD, hpartJson);
					}
				} else {
					// orphan node
					orphan = true;
				}
			} else {
				// orphan node
				orphan = true;
			}
			
			// orphan nodes
			if(orphan && orphanNodesLogging) {
				log(ORPHAN_NODES_LOGGER, handleid);
			}
			
			// writes back final data
			writer.write(sip);
			
			if(++processedCounter % displayThresholdLimit == 0) {
				displayStatus("Processed: " + processedCounter);
			}
		}
		
		// total statistics
		displayStatus("Processed: " + processedCounter);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processData() throws Exception {
		turnOffGlobalLoggingFlag(); // turn off global logging flag
		
		SIPDataIterator reader = null;
		NDLDataItemWriter<SIPDataItem> writer = null;
		try {
			// relations detail logger
			if(logRelationDetails) {
				addTextLogger(RELATION_LOGGER);
				addTextLogger(RELATION_LOGGER_LONG);
			}
			// orphan nodes logger
			if(orphanNodesLogging) {
				addTextLogger(ORPHAN_NODES_LOGGER);
			}
			if(logRelationDetails) {
				System.err.println("WARN: Relation logging is ON....");
			}
			System.out.println("PASS 1: Construction of tree....");
			
			reader = new SIPDataIterator(input);
			reader.init();
			
			start = System.currentTimeMillis(); // start time
			
			// first pass
			// this pass constructs hierarchy tree
			pass1(reader);
			
			reader.reset(); // again reads from start
			
			// writer initialization
			writer = new NDLDataItemWriter<>(outputLocation);
			writer.setCompressOn(getFileName(logicalName), CompressedFileMode.TARGZ);
			writer.init();
			
			System.out.println("Stitching in progress....");
			
			// stitch
			stitch(reader, writer);

			System.out.println("Stitching done.");
			System.out.println("Total " + CommonUtilities.durationMessage(System.currentTimeMillis() - start));
			if(orphanNodesCounter > 0) {
				// orphan nodes found
				System.out.println(
						"Orphan nodes found: " + orphanNodesCounter + ". For details see log if 'OrphanNodesLogging' is ON.");
			} else {
				// no orphan nodes found
				System.out.println("Yahoo!! No orphan nodes found.");
			}
			
		} finally {
			// close resources
			close();
			
			// close other resources
			if(reader != null) {
				reader.close();
			}
			if(writer != null) {
				writer.close();
			}
		}
	}
	
	/**
	 * Stitch data
	 * @throws Exception throws exception in case of errors
	 */
	public void stitch() throws Exception {
		processData();
	}
}