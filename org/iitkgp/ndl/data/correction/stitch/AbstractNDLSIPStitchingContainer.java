package org.iitkgp.ndl.data.correction.stitch;

import static org.iitkgp.ndl.data.correction.stitch.StitchingConstants.HAS_PART_FIELD;
import static org.iitkgp.ndl.data.correction.stitch.StitchingConstants.IS_PART_FIELD;
import static org.iitkgp.ndl.data.correction.stitch.StitchingConstants.ORPHAN_NODES_LOGGER;
import static org.iitkgp.ndl.data.correction.stitch.StitchingConstants.RELATION_LOGGER;
import static org.iitkgp.ndl.data.correction.stitch.StitchingConstants.RELATION_LOGGER_LONG;
import static org.iitkgp.ndl.data.correction.stitch.StitchingConstants.SOURCE_FIELD;
import static org.iitkgp.ndl.data.correction.stitch.StitchingConstants.SOURCE_URI_FIELD;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.context.NDLDataValidationContext;
import org.iitkgp.ndl.context.custom.NDLContext;
import org.iitkgp.ndl.context.custom.NDLContextSwitch;
import org.iitkgp.ndl.context.custom.exception.NDLContextSwitchLoadException;
import org.iitkgp.ndl.data.DataOrder;
import org.iitkgp.ndl.data.DataType;
import org.iitkgp.ndl.data.NDLDataPair;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.compress.CompressedFileMode;
import org.iitkgp.ndl.data.container.AbstractDataContainer;
import org.iitkgp.ndl.data.container.DataContainerNULLConfiguration;
import org.iitkgp.ndl.data.correction.stitch.comparator.NDLStitchComparator;
import org.iitkgp.ndl.data.correction.stitch.comparator.StandardNDLStitchComparator;
import org.iitkgp.ndl.data.correction.stitch.context.NDLStitchingContext;
import org.iitkgp.ndl.data.correction.stitch.exception.NDLSIPStitchBlankTitleException;
import org.iitkgp.ndl.data.correction.stitch.exception.NDLSIPStitchCrossPrefixIDException;
import org.iitkgp.ndl.data.correction.stitch.exception.NDLSIPStitchExistingNodeIdentifiersLoadException;
import org.iitkgp.ndl.data.correction.stitch.exception.NDLSIPStitchExistingNodeNotFoundException;
import org.iitkgp.ndl.data.correction.stitch.exception.NDLStitchInvalidHierarchyInformationException;
import org.iitkgp.ndl.data.correction.stitch.exception.NDLStitchOrderValueSynchronizationException;
import org.iitkgp.ndl.data.exception.NDLIncompleteDataException;
import org.iitkgp.ndl.data.iterator.DataSourceNULLConfiguration;
import org.iitkgp.ndl.data.iterator.SIPDataIterator;
import org.iitkgp.ndl.data.normalizer.NDLDataNormalizationPool;
import org.iitkgp.ndl.data.validator.NDLDataValidationBox;
import org.iitkgp.ndl.data.validator.NDLSchemaDetail;
import org.iitkgp.ndl.data.validator.UniqueErrorTracker;
import org.iitkgp.ndl.data.writer.NDLDataItemWriter;
import org.iitkgp.ndl.relation.HasPart;
import org.iitkgp.ndl.relation.IsPartOf;
import org.iitkgp.ndl.util.CommonUtilities;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.iitkgp.ndl.validator.exception.NDLDataValidationException;

import com.opencsv.CSVReader;

/**
 * NDL SIP data stitching.
 * <pre>The system needs following informations,</pre>
 * <ul>
 * <li>Hierarchy of each stitched item and the order of nodes on each hierarchy level, see {@link #hierarchy(SIPDataItem)}.</li>
 * <li>Global meta data detail. Don't need to provide 'dc.source' and 'dc.source.uri'. It's read by system if available.</li>
 * <li>
 * Intermediate nodes' meta data detail if any.
 * See {@link #addIntermediateNodeMetadata(SIPDataItem, Map)} and {@link #addLevelMetaData(int, String, String)}
 * </li>
 * <li>Comparison logic, item order value mention (see {@link NDLStitchHierarchyNode#setOrder(String)}),
 * if not then it uses haphazard order.
 * </li>
 * <li>
 * See {@link #addLevelComparator(int, NDLStitchComparator)}, {@link #addLevelOrder(int, DataOrder)},
 * {@link #addLevelOrder(int, DataOrder, DataType)}.
 * 'levelOrder' and 'levelComparator' mutually exclusive.
 * </li>
 * </ul>
 * <pre>Notes:</pre>
 * <ul>
 * <li>If handle prefix is not same for all items then use {@link #turnOnFullHandleIDConsideration()}</li>
 * <li>It does not validate control fields rather it validates fields' validity and fields' multiplicity</li>
 * <li>It does not do any sort of normalization, instead normalized data has to be added if required</li>
 * <li>To normalize data use {@link #addNodeValue(SIPDataItem, String, Collection)}</li>
 * <li>To normalize data use {@link #addNodeValue(SIPDataItem, String, String...)}</li>
 * <li>Validates generated/existing handle ID validity</li>
 * <li>Duplicate handle ID checking, see {@link #turnOnDuplicateHandlesChecking()}</li>
 * <li>
 * To make existing node as intermediate node/virtual node use,
 * <ul>
 * <li>{@link #turnOnExistingNodeLinking(File)}</li>
 * <li>{@link #turnOnExistingNodeLinking(String)}</li>
 * <li>{@link #getExistingNodes(String)}</li>
 * </ul>
 * </li>
 * </ul>
 * <pre>
 * How to handle missing nodes for a level,
 * Sometimes {@link NDLStitchHierarchyNode#NULL_HIERARCHY_NODE} has to be added for a missing level.
 * It's required to apply uniform level wise logic, metadata manipulation etc.
 * It's highly recommended to add NULL_NODE in case of missing level.
 * Suppose some 2nd level nodes are missing then a NULL_NODE is added and now 3rd level needs NUMERIC based comparison.
 * And 2nd level has some NO_ORDER or some non-numeric order value then to make uniform comparison logic 2nd level
 * has to be added with some relevant numeric order values.  
 * </pre>
 * @see #addNodeValue(SIPDataItem, String, Collection)
 * @see #addNodeValue(SIPDataItem, String, String...)
 * @author Debasis
 */
public abstract class AbstractNDLSIPStitchingContainer
		extends AbstractDataContainer<DataContainerNULLConfiguration<DataSourceNULLConfiguration>> {
	
	static final long INVALID_HANDLE_ID_DISPLAY_LIMIT = 100;
	
	static final String ROOT_NODE = "__ROOT__";
	
	/**
	 * TITLE field
	 */
	public static final String TITLE_FIELD = "dc.title";
	
	/**
	 * order numeric minimum value
	 */
	protected static final String NUMERIC_SMALLEST_ORDER_VALUE = String.valueOf(Integer.MIN_VALUE);
	
	/**
	 * order numeric maximum value
	 */
	protected static final String NUMERIC_LARGEST_ORDER_VALUE = String.valueOf(Integer.MAX_VALUE);
	
	long displayThresholdLimit = Long
			.parseLong(NDLConfigurationContext.getConfiguration("process.display.threshold.limit"));
	protected long processedCounter = 0; // how many data processed
	long start;
	
	String outputLocation; // output location
	String logicalName;
	
	// initial schema detail
	protected NDLSchemaDetail schemaDetail;
	// NDL data validator
	protected NDLDataValidationBox validator = null;
	
	// root node 0 level
	NDLStitchHierarchyTreeNode root = new NDLStitchHierarchyTreeNode(
			new NDLStitchHierarchyNode(ROOT_NODE, ROOT_NODE, true));
	long intermediateNodeCounter = 0; // tracking intermediate node counter
	long bytes4TreeNodes = 0;
	long parentFolderCounter = 0; // parent folder location
	
	Map<String, Collection<String>> globalMetadata = new HashMap<>(2); // applies for all
	Map<Integer, Map<String, Collection<String>>> levelMetadata = new HashMap<>(2); // level wise metadata
	// level wise order if not mentioned then default is ascending
	Map<Integer, DataOrder> levelOrders = new HashMap<>(2);
	Map<Integer, DataType> levelDataTypes = new HashMap<>(2); // level order data types for ordering
	Map<Integer, NDLStitchComparator> levelComparators = new HashMap<>(2); // level comparators
	NDLStitchComparator leafComparator;
	NDLStitchingContext context = new NDLStitchingContext(); // stitching context
	
	// track error/warning messages
	UniqueErrorTracker errorTracker = new UniqueErrorTracker();
	
	// logging of haspart ispart detail for cross check
	boolean logRelationDetails = false;
	long leafIsPartLogging = 1000;
	long leafIsPartLoggingCount = 0;
	long longJSONText = 500;
	long maxHaspartSize = Long.MIN_VALUE;
	long maxHaspartBytes = Long.MIN_VALUE;
	
	// whether to log orphan nodes or not
	boolean orphanNodesLogging = false;
	long orphanNodesCounter = 0;
	// handle ID validation or process status
	boolean stopOnInValidHandle = false;
	long invalidHandleDisplayCounter = 0;
	
	// intermediate node metadata merging flag
	boolean additionalMetadataMergingOnIntermediateNodeFlag = false;
	
	// source common data
	String hdlpfx;
	String source;
	String sourceURI;
	
	// duplicate handles checking
	Set<String> handles;
	boolean duplicateHandlesChecking = false;
	boolean fullHandleIDConsideration = false; // full handle ID consideration
	long duplicateTrackerBytes = 0;
	
	// existing node linking detail
	boolean existingNodesLinkingFlag = false;
	Set<String> existingHandles;
	Map<String, SIPDataItem> existingNodes;
	Map<String, NDLDataPair<String>> existingNodeIdentifiers;
	
	// normalize
	protected NDLDataNormalizationPool normalizer = new NDLDataNormalizationPool();
	
	// default handle ID generation strategy
	IntermediateNodeHandleIDGenerationStrategy handleIDGenerationStrategy = null;
	
	// context setup
	void contextSetup() {
		System.out.println("Initializing context setup....");
		// context startup
		NDLConfigurationContext.init();
		// load validation context
		NDLDataValidationContext.init();
	}
	
	/**
	 * Constructor
	 * @param input source input, from where data to be processed
	 * @param logLocation logging location, where logging takes place during data processing
	 * @param outputLocation where the corrected data kept
	 * @param logicalName logical name for the output data
	 * @throws Exception throws error in case adding relation logger or any sort context loading error
	 */
	public AbstractNDLSIPStitchingContainer(String input, String logLocation, String outputLocation, String logicalName)
			throws Exception {
		super(input, logLocation);
		this.outputLocation = outputLocation;
		this.logicalName = logicalName;
		contextSetup(); // context setup
		this.validator = new NDLDataValidationBox();
		// control field validation off
		validator.dontShowWarnings();
		validator.turnOffControlFieldsValidationFlag();
		validator.turnOffJsonKeyValidationFlag();
		
		// root level
		root.data.level = 0;
		// root node is virtual so 'existingNode' is false
		root.data.existingNode = false;
		
		// NDL context switching initialization
		NDLContextSwitch.init();
	}
	
	/**
	 * Sets default abbreviated handle ID generation strategy when handle ID is generated by system
	 * <pre>Note: This is typically required when ID field is so long and space separated</pre>
	 * @param levels levels to consider for abbreviated handle generation strategy
	 * @see #setAbbreviatedHandleIDGenerationStrategy(IntermediateNodeHandleIDGenerationStrategy)
	 */
	public void setDefaultAbbreviatedHandleIDGenerationStrategy(int... levels) {
		handleIDGenerationStrategy = new IntermediateNodeAbbreviatedHandleIDGeneration(levels);
	}

	/**
	 * Sets custom abbreviated handle ID generation strategy
	 * @param handleIDGenerationStrategy custom abbreviated handle ID generation strategy
	 * @see #setDefaultAbbreviatedHandleIDGenerationStrategy(int...)
	 */
	public void setAbbreviatedHandleIDGenerationStrategy(
			IntermediateNodeHandleIDGenerationStrategy handleIDGenerationStrategy) {
		this.handleIDGenerationStrategy = handleIDGenerationStrategy;
	}
	
	/**
	 * Track orphan nodes detail
	 */
	public void turnOnOrphanNodesLogging() {
		orphanNodesLogging = true;
	}
	
	/**
	 * This flag indicates whether intermediate node contains merging metadata/additional-data
	 * <pre>This is required when intermediate nodes need it's leaves' informations</pre> 
	 */
	public void turnOnAdditionalMetadataMergingOnIntermediateNode() {
		additionalMetadataMergingOnIntermediateNodeFlag = true;
	}
	
	/**
	 * Orphan nodes cause detailing track
	 * @param item item to find out cause
	 * @return returns cause detailing text
	 */
	protected String orphanNodesCauseDetailingTrack(SIPDataItem item) {
		return null;
	}
	
	/**
	 * Log linking detail
	 */
	public void turnOnLogRelationDetails() {
		logRelationDetails = true;
	}
	
	/**
	 * Turns on process stop flag on invalid handle found
	 */
	public void turnOnStopOnInValidHandle() {
		stopOnInValidHandle = true;
	}
	
	/**
	 * Turns on duplicate handles checking
	 * Behavior is based on handle SUFFIX only.
	 * @see #turnOnDuplicateHandlesChecking(boolean)
	 */
	public void turnOnDuplicateHandlesChecking() {
		duplicateHandlesChecking = true;
		handles = new HashSet<>();
	}
	
	/**
	 * turn on full handle ID consideration
	 * <pre>this flag should be ON when handles prefixes are different for a given source</pre>
	 */
	public void turnOnFullHandleIDConsideration() {
		fullHandleIDConsideration = true;
	}
	
	/**
	 * Turns on duplicate handles checking
	 * @param whether full handle ID checking or not
	 * @deprecated use instead {@link #turnOnFullHandleIDConsideration()}
	 */
	@Deprecated
	public void turnOnDuplicateHandlesChecking(boolean fullHandleIDChecking) {
		throw new IllegalStateException("Use instead #turnOnFullHandleIDConsideration");
	}
	
	/**
	 * Long JSON text length which will be tracked in different file
	 * @param longJSONText JSON text length
	 */
	public void setLongJSONText(long longJSONText) {
		this.longJSONText = longJSONText;
	}
	
	/**
	 * Checks for existing membership for a given key
	 * @param key given key
	 * @return returns true/false
	 */
	protected boolean belongsToExistingNodesByKey(String key) {
		return existingNodesLinkingFlag && existingNodeIdentifiers.containsKey(key);
	}
	
	/**
	 * Checks for existing membership for a given handle suffix
	 * @param handle given handle suffix
	 * @return returns true/false
	 */
	protected boolean belongsToExistingNodesByHandle(String handle) {
		return existingNodesLinkingFlag && existingHandles.contains(handle);
	}
	
	/**
	 * Loads existing nodes identifier CSV file to enable existing nodes linking
	 * <pre>This file contains three columns with a header,</pre>
	 * <ul>
	 * <li>First column is key by which existing node to be identified</li>
	 * <li>Second column is Handle suffix, see {@link NDLDataUtils#getHandleSuffixID(String)}
	 * if {@link #turnOnFullHandleIDConsideration()} is not called, otherwise full handle ID is used.</li>
	 * <li>Third column is title</li>
	 * </ul>
	 * <pre>Note: Here title can be anything (not necessarily from title field all time)</pre>
	 * <pre>Note: Don't get confused with {@link AbstractNDLSIPExistingNodeLinkingStitchingContainer}</pre>
	 * @param existingNodeIdentifierFile existing nodes identifier file
	 * @throws NDLSIPStitchExistingNodeIdentifiersLoadException throws error in case of loading fails
	 */
	public void turnOnExistingNodeLinking(File existingNodeIdentifierFile)
			throws NDLSIPStitchExistingNodeIdentifiersLoadException {
		
		existingNodeIdentifiers = new HashMap<>(2);
		existingHandles = new HashSet<>(2);
		existingNodes = new HashMap<>(2);
		existingNodesLinkingFlag = true;
		
		System.out.println("Loading existing nodes information.....");

		CSVReader reader = null;
		try {
			reader = NDLDataUtils.readCSV(existingNodeIdentifierFile);
			reader.readNext(); // skip headers
			String[] tokens;
			while((tokens = reader.readNext()) != null) {
				int l = tokens.length;
				if(l != 3) {
					// error
					throw new IllegalStateException("Exactly 3 columns expected, but found: " + l);
				}
				if(StringUtils.isBlank(tokens[0]) || StringUtils.isBlank(tokens[1]) || StringUtils.isBlank(tokens[2])) {
					// skip blank tokens
					continue;
				}
				if(!fullHandleIDConsideration && tokens[1].contains("/")) {
					// not a valid handle suffix ID
					throw new NDLDataValidationException(tokens[1] + " not a valid handle suffix ID, remove '/'."
							+ "Full handle id is used when 'fullHandleIDConsideration' flag is ON."
							+ "See #turnOnFullHandleIDConsideration.");
				}
				existingHandles.add(tokens[1]);
				existingNodeIdentifiers.put(tokens[0], new NDLDataPair<String>(tokens[1], tokens[2]));
			}
		} catch(Exception ex) {
			// loading fails
			throw new NDLSIPStitchExistingNodeIdentifiersLoadException(
					"Loading existing nodes information failed: " + ex.getMessage(), ex);
		} finally {
			// close resource
			IOUtils.closeQuietly(reader);
		}
	}
	
	/**
	 * Loads existing nodes identifier file to enable existing nodes linking
	 * @param existingNodeIdentifierFile existing nodes identifier file
	 * @throws NDLSIPStitchExistingNodeIdentifiersLoadException throws error in case of loading fails
	 * @see #turnOnExistingNodeLinking(String)
	 */
	public void turnOnExistingNodeLinking(String existingNodeIdentifierFile)
			throws NDLSIPStitchExistingNodeIdentifiersLoadException {
		turnOnExistingNodeLinking(new File(existingNodeIdentifierFile));
	}
	
	/**
	 * Sets leaf is-part logging count, if -1 then it logs for all
	 * @param leafIsPartLogging leaf logging count
	 */
	public void setLeafIsPartLogging(long leafIsPartLogging) {
		this.leafIsPartLogging = leafIsPartLogging;
	}
	
	// internal usage
	void addMetadata(String key, String value) {
		Collection<String> values = globalMetadata.get(key);
		if(values == null) {
			values = new LinkedList<>();
			globalMetadata.put(key, values);
		}
		values.add(value);
	}
	
	/**
	 * Adds global metadata
	 * @param key metadata key
	 * @param values metadata values
	 */
	public void addGlobalMetadata(String key, String ... values) {
		for(String value : values) {
			addMetadata(key, value);
		}
	}
	
	/**
	 * Adds level metadata
	 * @param level level starts from one.
	 * @param key metadata key
	 * @param value metadata value
	 */
	public void addLevelMetaData(int level, String key, String value) {
		Map<String, Collection<String>> metadata = levelMetadata.get(level);
		if(metadata != null) {
			metadata = new HashMap<>(2);
			levelMetadata.put(level, metadata);
		}
		Collection<String> values = metadata.get(key);
		if(values == null) {
			values = new LinkedList<>();
			metadata.put(key, values);
		}
		values.add(value);
	}
	
	/**
	 * Adds level wise order (comparison logic based on TEXT)
	 * @param level level starts from 1
	 * @param order corresponding order logic
	 */
	public void addLevelOrder(int level, DataOrder order) {
		levelOrders.put(level, order);
	}
	
	/**
	 * Adds level wise order based on given data type
	 * @param level level starts from 1
	 * @param order corresponding order logic
	 * @param type given data type
	 */
	public void addLevelOrder(int level, DataOrder order, DataType type) {
		levelOrders.put(level, order);
		levelDataTypes.put(level, type);
	}
	
	/**
	 * Adds level wise order based on given data type (ascending order)
	 * @param level level starts from 1
	 * @param type given data type
	 */
	public void addLevelOrder(int level, DataType type) {
		levelOrders.put(level, DataOrder.ASCENDING);
		levelDataTypes.put(level, type);
	}
	
	/**
	 * Adds level wise comparator
	 * @param level level starts from 1
	 * @param comparator corresponding comparator
	 * @see #intermediateNodeCustomItemsOrdering(NDLStitchHierarchyTreeNode, List)
	 */
	public void addLevelComparator(int level, NDLStitchComparator comparator) {
		levelComparators.put(level, comparator);
	}
	
	/**
	 * Sets leaf comparator when leaf level comparison/order is bit complex.
	 * When {@link #itemOrder(SIPDataItem)} does not fit alone your leaf level ordering logic.
	 * <pre>Make sure {@link #itemOrder(SIPDataItem)} puts order related value.</pre>
	 * @param leafComparator leaf comparator
	 */
	public void setLeafComparator(NDLStitchComparator leafComparator) {
		this.leafComparator = leafComparator;
	}
	
	// hierarchy validation
	void validateChain(NDLStitchHierarchyNode h) {
		if(StringUtils.isBlank(h.title) || (StringUtils.isBlank(h.handle) && StringUtils.isBlank(h.id))) {
			// error
			throw new NDLStitchInvalidHierarchyInformationException(
					"Title must be provided and either ID/Handle or both must be provided.");
		}
	}
	
	// hierarchy validation in detail
	void validateChaindetail(NDLStitchHierarchyNode h) {
		// TODO more validates to add if any
		// order field validation
		// when type is numeric and order field value is non-numeric
		// date field validation
		DataType type = levelDataTypes.get(h.level);
		if((type == DataType.INTEGER || type == DataType.LONG || type == DataType.REAL)
				&& !NumberUtils.isParsable(h.order)) {
			// numeric order field value validation
			throw new NDLDataValidationException(
					"Level data type is: " + type + ". But non-numeric order field data: " + h.order + " is found.");
		} else if(type == DataType.DATE && !NDLDataUtils.isNDLValidDateFormat(h.order)) {
			// date order field value validation
			throw new NDLDataValidationException(
					"Level data type is: " + type + ". But non-date order field data: " + h.order + " is found.");
		} else if(type != null && NDLStitchHierarchyNode.isNoOrder(h.order)) {
			// warning
			errorTracker.displayError("Level: " + h.level + " order value is blank. Make sure it's not required.");
		}
	}
	
	/**
	 * Sets starting auto-generated handle ID index by which system to try to generate different 
	 * handle along with some other informations
	 * @param autogenerateHandleID handle ID starting index
	 */
	public void setAutogenerateHandleID(long autogenerateHandleID) {
		this.context.setAutogenerateHandleID(autogenerateHandleID);
	}
	
	/**
	 * Stitch data
	 * @throws Exception throws exception in case of errors
	 */
	public void stitch() throws Exception {
		processData();
	}
	
	// is root or not
	boolean isroot(NDLStitchHierarchyNode node) {
		return node.id.equals(ROOT_NODE);
	}
	
	/**
	 * Gets existing node by key (used for linking existing node)
	 * <pre>
	 * To make it available first call
	 * {@link #turnOnExistingNodeLinking(File)} or {@link #turnOnExistingNodeLinking(String)}.
	 * The key in the file must match with this given key.
	 * </pre>
	 * @param key given key
	 * @return returns found node
	 * @throws NDLSIPStitchExistingNodeNotFoundException throws error if match not found
	 * @see #turnOnExistingNodeLinking(File)
	 * @see #turnOnExistingNodeLinking(String)
	 */
	protected NDLStitchHierarchyNode getExistingNodes(String key) throws NDLSIPStitchExistingNodeNotFoundException {
		if(!existingNodeIdentifiers.containsKey(key)) {
			throw new NDLSIPStitchExistingNodeNotFoundException(
					key + " not found. Make sure key present in 'turnOnExistingNodeLinking' file");
		}
		NDLDataPair<String> data = existingNodeIdentifiers.get(key);
		return new NDLStitchHierarchyNode(data.first(), data.second(), true, false);
	}
	
	// gets full handle ID
	String getFullHandle(String pfx, String id) {
		if(StringUtils.isBlank(id)) {
			throw new NDLIncompleteDataException("Handle Suffix is missing.");
		}
		if(!fullHandleIDConsideration) {
			// full handle ID consideration
			if(StringUtils.isBlank(pfx)) {
				throw new NDLIncompleteDataException("Handle prefix is missing.");
			}
			return pfx + "/" + id;
		} else {
			// id itself full handle ID
			return id;
		}
	}
	
	// level wise handle ID generation if needed
	String levelHandleIDGeneration(int level, String id) {
		if(handleIDGenerationStrategy == null) {
			// don't tamper ID
			return id;
		}
		if(!handleIDGenerationStrategy.levelExists(level)) {
			// level not to be considered
			// don't tamper ID
			return id;
		}
		// use defined handle ID generation strategy if level to be considered
		return handleIDGenerationStrategy.generate(level, id, context);
	}
	
	// whether to use full handle ID of handle suffix
	String useHandleID(String id) {
		if(fullHandleIDConsideration) {
			return id;
		} else {
			return NDLDataUtils.getHandleSuffixID(id);
		}
	}
	
	// pass1
	void pass1(SIPDataIterator reader, NDLDataItemWriter<SIPDataItem> writer) throws Exception {
		processedCounter = 0;
		boolean sourcef = false;
		long skipped = 0;
		try {
			// iterate data
			while(reader.hasNext()) {
				SIPDataItem sip = reader.next();
				
				// pre stitching correction if any
				if(!preStitchCorrection(sip)) {
					// no need to retain item (delete case)
					skipped++;
					continue;
				}
				
				String id = sip.getId();
				String handleid = useHandleID(id);
				if(!sourcef) {
					// set if not set
					if(!fullHandleIDConsideration) {
						// full handle ID consideration then no need to store handle prefix
						// handle prefix is different for some items
						hdlpfx = NDLDataUtils.getHandlePrefixID(id);
					}
					source = sip.getSingleValue(SOURCE_FIELD);
					sourceURI = sip.getSingleValue(SOURCE_URI_FIELD);
					sourcef = true;
				} else {
					if(!fullHandleIDConsideration) {
						// cross check whether developer has proper assumption (all prefixes area same)
						String t = NDLDataUtils.getHandlePrefixID(id);
						if(!StringUtils.equals(hdlpfx, t)) {
							// error
							throw new NDLSIPStitchCrossPrefixIDException("Cross prefix ID: <" + hdlpfx + ", " + t
									+ "> found." + "Use #turnOnFullHandleIDConsideration method.");
						}
					}
				}
				
				// handle prefix for virtual node
				// in case of `fullHandleIDConsideration` flag is ON
				String handlePrefix4VN = null;
				if(fullHandleIDConsideration) {
					handlePrefix4VN = NDLDataUtils.getHandlePrefixID(id);
				}
				// gets hierarchy
				NDLStitchHierarchy h = hierarchy(sip);
				
				if(h != null && !h.isEmpty()) {
					// chain exists
					List<NDLStitchHierarchyNode> hchain = h.hierarchy;
					if(!sip.exists(TITLE_FIELD)) {
						// title is missing
						throw new NDLSIPStitchBlankTitleException("Title is missing for: " + id);
					}
					
					NDLStitchHierarchyTreeNode tnode = root;
					int lvl = 1;
					// chain available
					StringBuilder hp = new StringBuilder(); // hierarchical path
					for(NDLStitchHierarchyNode chaindetail : hchain) {
						if(chaindetail.isNULLNode()) {
							// skip null node
							lvl++;
							continue;
						}
						
						validateChain(chaindetail); // chain validation
						// add folder location if any
						if(chaindetail.rootLocation) {
							// use root location
							// assumed data is there
							// TODO data folder missing case handle
							chaindetail.setFolder("/data/" + ++parentFolderCounter);
						} else {
							String f = sip.getFolder();
							 // parent of parent then add parent folder index
							f = f.substring(0, f.lastIndexOf('/'))  + '/' + ++parentFolderCounter;
							chaindetail.setFolder(f);
						}
						// construct tree
						chaindetail.level = lvl++;
						
						// validate chain detail in more
						validateChaindetail(chaindetail);
						
						// here level handle ID generation strategy is added
						NDLStitchHierarchyTreeNodeCreation ncreation = tnode.add(chaindetail, context,
								hp.append('_')
										.append(StringUtils.isBlank(chaindetail.handle)
												? levelHandleIDGeneration(chaindetail.level, chaindetail.id)
												: chaindetail.handle)
										.toString(),
								handlePrefix4VN);
						if(ncreation.isForceRemoved()) {
							// remove the handle ID from from duplicate tracker
							handles.remove(ncreation.removedHandleID);
						}
						
						// intermediate node metadata merging
						if(additionalMetadataMergingOnIntermediateNodeFlag) {
							ncreation.node.merge(chaindetail.metadata, chaindetail.additionalData);
						}
						
						String hdl = ncreation.node.data.handle;
						if(ncreation.created && duplicateHandlesChecking) {
							// duplicate handles checking if node created
							// size calculation
							int hl = hdl.length();
							duplicateTrackerBytes += hl;
							bytes4TreeNodes += hl;
							// duplicate handles checking
							if(!handles.add(hdl)) {
								// duplicate found
								throw new NDLDataValidationException("Duplicate handle ID: " + hdl);
							}
						}
						
						if(ncreation.invalidHandle) {
							// invalid handle id handle
							String hm = "Invalid HANDLE ID: " + hdl;
							if(stopOnInValidHandle) {
								// throw error
								throw new NDLDataValidationException(hm);
							} else {
								// put some warning
								if(++invalidHandleDisplayCounter < INVALID_HANDLE_ID_DISPLAY_LIMIT) {
									System.err.println(hm);
								}
							}
						}
						
						tnode = ncreation.node; // next parent node
						if(ncreation.created) {
							// node created
							intermediateNodeCounter++;
							// size calculation
							bytes4TreeNodes += ncreation.bytes4Creation;
						}
					}
					
					// leaf handle id duplicate check
					if(duplicateHandlesChecking) {
						// size calculation
						int hl = handleid.length();
						duplicateTrackerBytes += hl;
						bytes4TreeNodes += hl;
						// duplicate handles checking
						boolean noduplicatehandle;
						noduplicatehandle = handles.add(handleid);
						if(!noduplicatehandle) {
							// duplicate found
							throw new NDLDataValidationException("Duplicate handle ID: " + id
									+ ". May be prefix is different, in that case use `turnOnDuplicateHandlesChecking(true)`.");
						}
					}
					
					// add leaf
					// leaf title can be customized instead of reading from title field all time
					NDLStitchHierarchyNode leaf = new NDLStitchHierarchyNode(handleid, h.getLeafTitle(sip), true,
							false); // mark leaf as existing node
					leaf.leaf = true;
					leaf.order = itemOrder(sip); // gets item order
					leaf.level = lvl;
					
					// validate chain detail in more
					validateChaindetail(leaf);
					
					// TODO though for leaf level information does not matter
					NDLStitchHierarchyTreeNodeCreation ncreation = tnode.add(leaf, context);
					// size calculation
					bytes4TreeNodes += ncreation.bytes4Creation;
					
					// adds item ispart-of information
					String iparttxt = NDLDataUtils
							.serializeIsPartOf(new IsPartOf(getFullHandle(hdlpfx, tnode.data.handle), tnode.data.title));
					if(logRelationDetails) {
						if(leafIsPartLogging == -1 || ++leafIsPartLoggingCount < leafIsPartLogging) {
							// leaf level linking logging restricted
							log(RELATION_LOGGER, handleid + " => " + iparttxt);
						}
					}
					sip.updateSingleValue(IS_PART_FIELD, iparttxt);
				} else {
					if(h == null || !belongsToExistingNodesByHandle(handleid)) {
						// further query for double confirm
						orphanNodesCounter++;
						// track orphan nodes
						if(orphanNodesLogging) {
							String cause = orphanNodesCauseDetailingTrack(sip);
							StringBuilder logm = new StringBuilder(sip.getId());
							if(StringUtils.isNotBlank(cause)) {
								// cause detailing
								logm.append(":- ").append(cause);
							}
							log(ORPHAN_NODES_LOGGER, logm.toString());
						}
					}
				}
				
				// display processed item
				if(++processedCounter % displayThresholdLimit == 0) {
					displayStatus("Processed: " + processedCounter + " items. Orphan nodes: " + orphanNodesCounter
							+ ". " + intermediateNodeCounter + " intermediate " + "nodes created("
							+ CommonUtilities.bytesMessage(bytes4TreeNodes * 2) + ")");
				}
				
				// post stitching correction if any
				postStitchCorrection(sip);
				
				NDLDataUtils.validateNDLData(sip, validator); // validation
				
				if(existingNodesLinkingFlag && existingHandles.contains(handleid)) {
					// an existing node
					// archive it for further linking and storing
					existingNodes.put(handleid, sip);
					// tentative size calculation
					bytes4TreeNodes += handleid.length() + sip.size();
				} else {
					// not an existing node
					// writes back intermediate data
					writer.write(sip);
				}
			}

			// last processed message
			System.out.println("Processed: " + processedCounter + " items. Orphan nodes: " + orphanNodesCounter + ". "
					+ intermediateNodeCounter + " intermediate nodes created("
					+ CommonUtilities.bytesMessage(bytes4TreeNodes * 2) + ") And skipped(deleted): " + skipped);
			System.out.println(CommonUtilities.durationMessage(System.currentTimeMillis() - start)); // duration message
			
			// duplicate tracker memory free
			if(duplicateHandlesChecking) {
				System.out.println("Yahoo!! NO duplicate handle ID found. So freeing up duplicates tracker memory...");
				handles.clear();
				System.out.println(CommonUtilities.bytesMessage(duplicateTrackerBytes * 2) + " released...");
			}
			
		} catch(Exception ex) {
			// error
			writer.close();
			
			throw ex; // propagate error
		} finally {
			// closes resources
			reader.close();
		}
	}
	
	// haspart comparator
	Comparator<NDLStitchHierarchyNode> hpartcomp = new Comparator<NDLStitchHierarchyNode>() {
		// comparison logic
		@Override
		public int compare(NDLStitchHierarchyNode o1, NDLStitchHierarchyNode o2) {
			if(o1.level != o2.level) {
				// cross level comparison it happens when NULL_node added (make sure NULL node is added purposefully)
				// warning
				StringBuilder warning = new StringBuilder();
				warning.append("WARN: Cross levels(").append(o1.level).append(", ").append(o2.level)
						.append(") compared.")
						.append("it happens when NULL_node added, make sure NULL node is added purposefully.");
				errorTracker.displayError(warning.toString());
			}
			int lvl = o1.level;
			NDLStitchComparator comp = levelComparators.get(lvl);
			if(comp != null) {
				// level wise comparator defined
				return comp.comparator(o1.order, o2.order);
			} else {
				if(o1.leaf && o2.leaf && leafComparator != null) {
					// leaf level custom comparison
					return leafComparator.comparator(o1.order, o2.order);
				}
				
				DataType type = levelDataTypes.get(lvl);
				int c;
				if(type == null) {
					// plain text based comparison
					c = o1.order.compareTo(o2.order);
				} else {
					// specific type based comparison
					try {
						c = NDLDataUtils.getByDataType(o1.order, type)
								.compareTo(NDLDataUtils.getByDataType(o2.order, type));
					} catch(Exception ex) {
						// make sure all levels are synchronized with order value
						// if cross level comparison happens then make sure NULL node is added purposefully
						// instead use `intermediateNodeCustomItemsOrdering` or `setLeafComparator`
						StringBuilder errorm = new StringBuilder("[ERROR: " + ex.toString() + "][Level: <" + o1.level
								+ ", " + o2.level + "> Value:<" + o1.order + ", " + o2.order + "> Type: " + type
								+ "]Make sure all levels are synchronized with order value.")
										.append("If cross level comparison happens then make sure NULL node is added purposefully.")
										.append("Instead use `intermediateNodeCustomItemsOrdering` or `setLeafComparator` ")
										.append("for complex comparison logic to make things simpler.");
						throw new NDLStitchOrderValueSynchronizationException(errorm.toString(), ex);
					}
				}
				DataOrder odr = levelOrders.get(lvl);
				if(odr != null) {
					// level wise order defined
					if(odr == DataOrder.ASCENDING) {
						// ascending comparison
						return c;
					} else {
						// descending
						return -c;
					}
				} else {
					// normal ascending comparison
					return c;
				}
			}
		}
	};
	
	// source common data validation if provided explicit
	void sourceCommonDataValidation(String key, Collection<String> values) {
		String value = values.iterator().next();
		if(StringUtils.equals(key, SOURCE_FIELD) && !StringUtils.equals(source, value)) {
			throw new NDLDataValidationException("Existing source: (" + source + ") but provided: (" + value + ")");
		}
		if(StringUtils.equals(key, SOURCE_URI_FIELD) && !StringUtils.equals(sourceURI, value)) {
			throw new NDLDataValidationException("Existing source URI: (" + sourceURI + ") but provided: (" + value + ")");
		}
	}
	
	// intermediate node creation
	boolean inodeCreate(NDLDataItemWriter<SIPDataItem> writer, Map<String, List<HasPart>> hplink2existing,
			Map<String, IsPartOf> iplink2existing, NDLStitchHierarchyTreeNode bfsnode, List<HasPart> hasparts,
			String hdlpfx) throws Exception {
		
		NDLStitchHierarchyNode data = bfsnode.data;
		boolean hpf = hasparts != null && !hasparts.isEmpty();
		
		// add ispart if exists
		NDLStitchHierarchyTreeNode p = bfsnode.parent;
		IsPartOf ipart = null;
		if(p != null && !isroot(p.data)) {
			// parent is other than root (root is a DUMMY)
			ipart = new IsPartOf(getFullHandle(hdlpfx, p.data.handle), p.data.title);
		}
		
		if(data.isCreate() && hpf) {
			// create intermediate node
			SIPDataItem isipnode = NDLDataUtils.createBlankSIP(getFullHandle(hdlpfx, data.handle));
			isipnode.add(TITLE_FIELD, data.title); // adds title
			isipnode.setFolder(data.folder);
			// adds metadata
			prepareIntermediateNodeMetadata(isipnode, data.getRawAdditionalData());
			// level wise specific meta data
			Map<String, Collection<String>> mdv = data.getMetadata();
			for(String key : mdv.keySet()) {
				Collection<String> md = data.getMetadata(key);
				isipnode.add(key, md);
			}
			// level wise meta data
			int lvl = data.level;
			Map<String, Collection<String>> lvalues = levelMetadata.get(lvl);
			if(lvalues != null) {
				for(String key : lvalues.keySet()) {
					Collection<String> v = lvalues.get(key);
					sourceCommonDataValidation(key, v); // source common data validation
					isipnode.add(key, v);
				}
			}
			// global metadata
			if(globalMetadata != null) {
				for(String key : globalMetadata.keySet()) {
					Collection<String> v = globalMetadata.get(key);
					sourceCommonDataValidation(key, v); // source common data validation
					isipnode.add(key, v);
				}
			}
			
			// add has-part
			String hparttxt = NDLDataUtils.serializeHasPart(hasparts);
			
			// track long has-part
			int s = hasparts.size();
			int l = hparttxt.length();
			if(s > maxHaspartSize) {
				maxHaspartSize = s;
			}
			if(l > maxHaspartBytes) {
				maxHaspartBytes = l;
			}
			trackLongHaspart(s, l, isipnode.getId());
			
			if(logRelationDetails) {
				if(hparttxt.length() < longJSONText) {
					// short
					log(RELATION_LOGGER, NDLDataUtils.getHandleSuffixID(isipnode.getId()) + " => " + hparttxt);
				} else {
					// long
					log(RELATION_LOGGER_LONG, NDLDataUtils.getHandleSuffixID(isipnode.getId()) + " => " + hparttxt);
				}
			}
			isipnode.add(HAS_PART_FIELD, hparttxt);
			
			// is-part
			if(ipart != null) {
				String iparttxt = NDLDataUtils.serializeIsPartOf(ipart);
				isipnode.add(IS_PART_FIELD, iparttxt);
				if(logRelationDetails) {
					log(RELATION_LOGGER, NDLDataUtils.getHandleSuffixID(isipnode.getId()) + " => " + iparttxt);
				}
			}
			
			// add if missing
			isipnode.addIfNotContains(SOURCE_FIELD, source);
			isipnode.addIfNotContains(SOURCE_URI_FIELD, sourceURI);
			
			NDLDataUtils.validateNDLData(isipnode, validator); // validation
			
			// write intermediate item
			writer.write(isipnode);
			return true;
		}
		
		// in case of linking to existing node (non-leaf)
		if(!data.leaf && data.existingNode) {
			if(hpf) {
				hplink2existing.put(data.handle, hasparts);
			}
			if(ipart != null) {
				iplink2existing.put(data.handle, ipart);
			}
		}
		
		return false;
	}
	
	/**
	 * defines parent item (intermediate/virtual) specific ordering irrespective of level.
	 * it's more generalized ordering definition
	 * <pre>
	 * Hints: For ordering use {@link Collections#sort(List, Comparator)},
	 * first parameter is `children` and second parameter is custom comparator for ordering items;
	 * or {@link StandardNDLStitchComparator#sortHasParts(List, NDLStitchComparator)}.
	 * </pre>
	 * @param parent parent to identify custom ordering logic
	 * @param children associated children of given parent node
	 * @return returns true if ordering is done here otherwise false
	 * so that next ordering logic can take place
	 * @see #addLevelComparator(int, NDLStitchComparator)
	 * @see #addLevelOrder(int, DataOrder)
	 * @see #addLevelOrder(int, DataType)
	 * @see #addLevelOrder(int, DataOrder, DataType)
	 */
	protected boolean intermediateNodeCustomItemsOrdering(NDLStitchHierarchyTreeNode parent,
			List<NDLStitchHierarchyNode> children) {
		return false;
	}
	
	// haspart build
	List<HasPart> buildHaspart(NDLStitchHierarchyTreeNode bfsnode, Collection<NDLStitchHierarchyTreeNode> cnodes)
			throws Exception {
		
		List<HasPart> hasparts = null;
		
		if(!bfsnode.data.id.equals(ROOT_NODE) && !bfsnode.data.leaf) {
			// other than ROOT and not leaf
			// intermediate node
			int l = cnodes.size();
			List<NDLStitchHierarchyNode> thasparts = new ArrayList<>(l);
			for(NDLStitchHierarchyTreeNode cnode : cnodes) {
				NDLStitchHierarchyNode tnode = new NDLStitchHierarchyNode(getFullHandle(hdlpfx, cnode.data.handle),
						cnode.data.title);
				tnode.handle = tnode.id; // use ID as handle
				tnode.order = cnode.data.order;
				tnode.level = cnode.data.level;
				tnode.leaf = cnode.children.isEmpty();
				thasparts.add(tnode);
			}
			// sort the data using comparison logic
			if(!intermediateNodeCustomItemsOrdering(bfsnode, thasparts)) {
				// if custom logic does not want to do ordering then look for next/default ordering logic
				Collections.sort(thasparts, hpartcomp);
			}
			
			hasparts = new ArrayList<>(l);
			for(NDLStitchHierarchyNode hp : thasparts) {
				// TODO discuss whether visible false or true
				boolean vflag = hp.leaf ? true
						: intermediateNodeVisibilityFlag(hp.title, hp.handle, hp.level, hp.additionalData);
				// here id is handle
				hasparts.add(new HasPart(hp.title, hp.handle, !hp.leaf, vflag));
			}
		}
		
		return hasparts;
	}
	
	// display status of process
	void displayStatus(String ... messages) {
		// display messages
		for(String message : messages) {
			System.out.println(message);
		}
		long intermediate = System.currentTimeMillis(); // intermediate time
		System.out.println(CommonUtilities.durationMessage(intermediate - start)); // duration message
	}
	
	// pass2
	void pass2(File nextInput, NDLDataItemWriter<SIPDataItem> writer, File ifile) throws Exception {
		
		boolean fdelete = false;
		try {
			// writes intermediate nodes from tree
			Queue<NDLStitchHierarchyTreeNode> bfsnodes = new LinkedList<>();
			// existing linking(haspart-ispart) if any
			Map<String, List<HasPart>> hplink2existing = new HashMap<>(2);
			Map<String, IsPartOf> iplink2existing = new HashMap<>(2);
			bfsnodes.add(root);
			processedCounter = 0;
			// BFS scanning
			// TODO memory usage and release tracking
			long memoryReleased = 0;
			while(!bfsnodes.isEmpty()) {
				// keep on iterating until queue is empty
				NDLStitchHierarchyTreeNode bfsnode = bfsnodes.poll();
				Collection<NDLStitchHierarchyTreeNode> cnodes = bfsnode.children.values();
				
				// has-part build up
				List<HasPart> hasparts = buildHaspart(bfsnode, cnodes);
				
				// intermediate node creation
				if(inodeCreate(writer, hplink2existing, iplink2existing, bfsnode, hasparts, hdlpfx)) {
					// node creation successful
					// display processed item
					if(++processedCounter % displayThresholdLimit == 0) {
						displayStatus("Intermediate nodes processed: " + processedCounter + " items.",
								"Memory released: " + CommonUtilities.bytesMessage(memoryReleased * 2),
								"Maximum hasparts: " + maxHaspartSize + "(" + maxHaspartBytes + " bytes JSON)");
					}
				}
				
				// add next level nodes
				bfsnodes.addAll(cnodes);
				
				// free up children after use
				memoryReleased += bfsnode.size();
				bfsnode.children.clear();				 
			}
			
			// last processed message
			System.out.println("Intermediate nodes processed: " + processedCounter + " items.");
			System.out.println("Memory released: " + CommonUtilities.bytesMessage(memoryReleased * 2));
			System.out.println("Maximum hasparts: " + maxHaspartSize + "(" + maxHaspartBytes + " bytes JSON)");
			System.out.println(CommonUtilities.durationMessage(System.currentTimeMillis() - start)); // duration message
			
			if(!hplink2existing.isEmpty() || !iplink2existing.isEmpty()) {
				// existing nodes to updated with associated links
				
				// linking to existing node will be established
				System.out.println("PASS 3: Final data write and linking has-part information to existing nodes...");
				
				if(existingNodesLinkingFlag) {
					// linking happens if exists
					long existingNodeLinkingCounter = 0;
					processedCounter = 0;
					for(SIPDataItem sip : existingNodes.values()) {
						if(link(hplink2existing, iplink2existing, sip)) {
							// link has-part is-part to existing node
							existingNodeLinkingCounter++;
						}
						
						// display processed item
						if(++processedCounter % displayThresholdLimit == 0) {
							displayStatus(
									"Processed: " + processedCounter + " items. Existing nodes linked: "
											+ existingNodeLinkingCounter + ".",
									"Maximum hasparts: " + maxHaspartSize + "(" + maxHaspartBytes + ")");
						}
						
						NDLDataUtils.validateNDLData(sip, validator); // validation
						// writes back final data
						writer.write(sip);
					}
					
					// last processing message
					System.out.println("Processed: " + processedCounter + " items. Existing nodes linked: "
							+ existingNodeLinkingCounter + ".");
					System.out.println("Maximum hasparts: " + maxHaspartSize + "(" + maxHaspartBytes + ")");
					System.out.println(CommonUtilities.durationMessage(System.currentTimeMillis() - start)); // duration message
				}
				
				if(!existingNodesLinkingFlag) {
					// close existing writer (intermediate data)
					writer.close();
					
					// if 'turnOnExistingNodeLinking' not called and handled manually
					// next iteration required for existing nodes linking
					SIPDataIterator reader = new SIPDataIterator(nextInput);
					reader.init();
					
					fdelete = true; // delete temporary file
					
					try {
						// final writer
						writer = new NDLDataItemWriter<>(outputLocation);
						writer.setCompressOn(getFileName(logicalName), CompressedFileMode.TARGZ);
						writer.init();
						
						// iterate intermediate data
						pass3(reader, writer, hplink2existing, iplink2existing);
					} finally {
						// close resources
						reader.close();
					}
				}
			}
			
		} finally {
			// closes resources
			writer.close();
			
			if(fdelete) {
				// remove intermediate file
				ifile.delete();
			}
		}
	}
	
	/**
	 * tracks long has-part if needed
	 * <pre>Don't confuse with {@link #setLongJSONText(long)}</pre>
	 * @param nodes number of children
	 * @param bytes length of has-part JSON
	 * @param handle for which handle ID
	 * @throws Exception throws error in case tracking (writing into log file) fails
	 * @see #setLongJSONText(long)
	 */
	protected void trackLongHaspart(long nodes, long bytes, String handle) throws Exception {
		// blank
	}
	
	// has-part is-part linking to existing node
	boolean link(Map<String, List<HasPart>> hplink2existing, Map<String, IsPartOf> iplink2existing, SIPDataItem sip)
			throws Exception {
		String hdlsfx = NDLDataUtils.getHandleSuffixID(sip.getId());
		boolean f = false;
		if(hplink2existing.containsKey(hdlsfx)) {
			// add to existing, linking information
			List<HasPart> hparts = hplink2existing.get(hdlsfx);
			String hparttxt = NDLDataUtils.serializeHasPart(hparts);
			
			// track long has-part
			int s = hparts.size();
			int l = hparttxt.length();
			if(s > maxHaspartSize) {
				maxHaspartSize = s;
			}
			if(l > maxHaspartBytes) {
				maxHaspartBytes = l;
			}
			trackLongHaspart(s, l, sip.getId());
			
			if(logRelationDetails) {
				if(hparttxt.length() < longJSONText) {
					// short
					log(RELATION_LOGGER, hdlsfx + " => " + hparttxt);
				} else {
					// long
					log(RELATION_LOGGER_LONG, hdlsfx + " => " + hparttxt);
				}
			}
			sip.updateSingleValue(HAS_PART_FIELD, hparttxt);
			f = true;
		}
		if(iplink2existing.containsKey(hdlsfx)) {
			// i-part update
			String ipart = sip.getSingleValue(IS_PART_FIELD);
			boolean updatef = true;
			if(StringUtils.isNotBlank(ipart)) {
				// this is the case if is-part is updated already
				// now cross-validate it
				try {
					IsPartOf eiparto = NDLDataUtils.deserializeIsPartOfJSON(ipart);
					String eh = eiparto.getHandle();
					String et = eiparto.getHandle();
					IsPartOf niparto = NDLDataUtils.deserializeIsPartOfJSON(ipart);
					String nh = niparto.getHandle();
					String nt = niparto.getHandle();
					if(!StringUtils.equals(eh, nh) || !StringUtils.equals(et, nt)) {
						// mismatch, put warning
						System.err.println(hdlsfx + " IS-PART is already updated but mismatched so overwriting it.");
					} else {
						// no need to update
						updatef = false;
					}
				} catch(Exception ex) {
					// suppress error
				}
			}
			if(updatef) {
				// add to existing, linking information
				String iparttxt = NDLDataUtils.serializeIsPartOf(iplink2existing.get(hdlsfx));
				if(logRelationDetails) {
					log(RELATION_LOGGER, hdlsfx + " => " + iparttxt);
				}
				sip.updateSingleValue(IS_PART_FIELD, iparttxt);
				f = true;
			}
		}
		return f;
	}
	
	// pass3
	void pass3(SIPDataIterator reader, NDLDataItemWriter<SIPDataItem> writer,
			Map<String, List<HasPart>> hplink2existing, Map<String, IsPartOf> iplink2existing) throws Exception {
		processedCounter = 0;
		long existingNodeLinkingCounter = 0;
		// next iteration to add link to existing nodes
		while(reader.hasNext()) {
			SIPDataItem sip = reader.next();
			
			// link has-part is-part to existing node if any
			boolean f = link(hplink2existing, iplink2existing, sip);
			
			if(f) {
				// existing node linked
				existingNodeLinkingCounter++;
			}
			
			// display processed item
			if(++processedCounter % displayThresholdLimit == 0) {
				displayStatus("Processed: " + processedCounter + " items. Existing nodes linked: "
						+ existingNodeLinkingCounter + ".");
			}
			
			NDLDataUtils.validateNDLData(sip, validator); // validation
			// writes back final data
			writer.write(sip);
		}
		
		// last processing message
		System.out.println("Processed: " + processedCounter + " items. Existing nodes linked: "
				+ existingNodeLinkingCounter + ".");
		System.out.println(CommonUtilities.durationMessage(System.currentTimeMillis() - start)); // duration message
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processData() throws Exception {
		
		turnOffGlobalLoggingFlag(); // turn off global logging flag
		
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
			
			if(!duplicateHandlesChecking) {
				// warn message
				System.err.println("WARN: Duplicate handles checking is OFF, make sure handles are distinct. "
						+ "And to check by program use 'turnOnDuplicateHandlesChecking'.");
			}
			
			if(additionalMetadataMergingOnIntermediateNodeFlag) {
				// warn message
				System.err.println("WARN: Make sure the less varieties meta-data/additional-data merged into intermediate node.");
			}
			
			// writer (temporary)
			NDLDataItemWriter<SIPDataItem> writer = new NDLDataItemWriter<>(outputLocation);
			writer.setCompressOn(getFileName(logicalName), CompressedFileMode.TARGZ);
			writer.init();
			File nextInput = writer.getCompressedFile();
			
			if(logRelationDetails) {
				System.err.println("WARN: Relation logging is ON....");
			}
			System.out.println("PASS 1: Construction of tree....");
			
			SIPDataIterator reader = new SIPDataIterator(input);
			reader.init();
			
			start = System.currentTimeMillis(); // start time
			// first pass
			// this pass constructs hierarchy tree
			pass1(reader, writer);
			
			// pass 2
			// second pass from tree it creates intermediate nodes
			System.out.println("PASS 2: Ceation of intermediate nodes and linking has-part and is-part....");
			pass2(nextInput, writer, nextInput);
			
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
			
			// context switch destroy
			// TODO right now it does do anything
			NDLContextSwitch.destroy();
		}
	}
	
	/**
	 * Add node value(s) for a given field and value(s)
	 * <pre>Note: Here values are normalized.</pre>
	 * @param item current node to update
	 * @param field field name
	 * @param values corresponding field value(s)
	 */
	protected void addNodeValue(SIPDataItem item, String field, String ... values) {
		Set<String> mvalues = new LinkedHashSet<String>(2);
		for(String value : values) {
			if(StringUtils.isNotBlank(value)) {
				// valid value
				mvalues.addAll(normalizer.normalize(field, value));
			}
		}
		item.add(field, mvalues);
	}
	
	/**
	 * Add nodes value for a given field and values
	 * <pre>Note: Here values are normalized.</pre>
	 * @param item current node to update
	 * @param field field name
	 * @param values corresponding field values
	 */
	protected void addNodeValue(SIPDataItem item, String field, Collection<String> values) {
		Set<String> mvalues = new LinkedHashSet<String>(2);
		for(String value : values) {
			mvalues.addAll(normalizer.normalize(field, value));
		}
		item.add(field, mvalues);
	}
	
	/**
	 * Adds intermediate node meta data if global and level wise metadata not fit
	 * <pre>If any field has to be normalized here then do it here, don't leave it to system.</pre>
	 * <pre>To normalize use,</pre>
	 * <ul>
	 * <li>{@link #addNodeValue(SIPDataItem, String, Collection)}</li>
	 * <li>{@link #addNodeValue(SIPDataItem, String, String...)}</li>
	 * </ul>
	 * @param item intermediate node/item
	 * @param additionalData additional data for node specific metadata manipulation
	 * @throws Exception throws error in case adding metadata
	 * @see #addNodeValue(SIPDataItem, String, Collection)
	 * @see #addNodeValue(SIPDataItem, String, String...)
	 * @see #addGlobalMetadata(String, String...)
	 * @deprecated use rathe {@link #prepareIntermediateNodeMetadata(SIPDataItem, Map)}
	 */
	@Deprecated
	protected void addIntermediateNodeMetadata(SIPDataItem item, Map<String, String> additionalData) throws Exception {
		// this method is deprecated
		throw new UnsupportedOperationException(
				"This method is deprecated, use instead 'prepareIntermediateNodeMetadata'");
	}
	
	/**
	 * Adds intermediate node meta data if global and level wise metadata not fit
	 * <pre>If any field has to be normalized here then do it here, don't leave it to system.</pre>
	 * <pre>To normalize use,</pre>
	 * <ul>
	 * <li>{@link #addNodeValue(SIPDataItem, String, Collection)}</li>
	 * <li>{@link #addNodeValue(SIPDataItem, String, String...)}</li>
	 * </ul>
	 * @param item intermediate node/item
	 * @param additionalData additional data for node specific metadata manipulation
	 * @throws Exception throws error in case adding metadata
	 * @see #addNodeValue(SIPDataItem, String, Collection)
	 * @see #addNodeValue(SIPDataItem, String, String...)
	 * @see #addGlobalMetadata(String, String...)
	 */
	protected void prepareIntermediateNodeMetadata(SIPDataItem item, Map<String, Collection<String>> additionalData)
			throws Exception {
		// blank
	}
	
	/**
	 * Gets item order value
	 * @param item current working item
	 * @return returns order value
	 */
	protected String itemOrder(SIPDataItem item) {
		return ""; // default order value
	}
	
	/**
	 * returns intermediate node visibility in case it has visibility true
	 * <pre>make sure before turning on visibility for an intermediate node</pre>
	 * <pre>
	 * note: override only if some intermediate nodes have visibility true for a level,
	 * otherwise leave it to system
	 * </pre>
	 * @param title title to decide visibility flag
	 * @param handle handle to decide visibility flag
	 * @param level item hierarchy level
	 * @param additionalData additional data to identify an item more precisely
	 * @return returns visibility flag
	 * @deprecated use rather {@link #intermediateNodeVisibilityFlag(String, String, int, Map)}
	 */
	@Deprecated
	protected boolean intermediateItemVisibilityFlag(String title, String handle, int level,
			Map<String, String> additionalData) {
		// this method is deprecated
		throw new UnsupportedOperationException(
				"This method is deprecated, use instead 'intermediateNodeVisibilityFlag'");
	}
	
	/**
	 * returns intermediate node visibility in case it has visibility true
	 * <pre>make sure before turning on visibility for an intermediate node</pre>
	 * <pre>
	 * note: override only if some intermediate nodes have visibility true for a level,
	 * otherwise leave it to system
	 * </pre>
	 * @param title title to decide visibility flag
	 * @param handle handle to decide visibility flag
	 * @param level item hierarchy level
	 * @param additionalData additional data to identify an item more precisely
	 * @return returns visibility flag
	 */
	protected boolean intermediateNodeVisibilityFlag(String title, String handle, int level,
			Map<String, Collection<String>> additionalData) {
		return false;
	}
	
	/**
	 * Manipulates hierarchy detail for current item. Hierarchy detail should be in order (parent to child).
	 * {@link NDLStitchHierarchyNode} this object needs to be populated.
	 * <pre>Notes:</pre>
	 * <ul> 
	 * <li>only intermediate nodes to be provided</li>
	 * <li>here correction can be done, but if highly urgent; otherwise it's not recommended.</li>
	 * <li>it's highly recommended to put minimum information other than ID and Title with each node</li>
	 * <li>
	 * to make existing node as intermediate node/virtual node use,
	 * <ul>
	 * <li>{@link #turnOnExistingNodeLinking(File)}</li>
	 * <li>{@link #turnOnExistingNodeLinking(String)}</li>
	 * <li>{@link #getExistingNodes(String)}</li>
	 * </ul>
	 * </li>
	 * <li>
	 * If leaf title to be changed (not to use default title)
	 * then use  {@link NDLStitchHierarchy#setLeafTitle(String)}
	 * </li>
	 * </ul>
	 * @param item current working item
	 * @return returns hierarchy detail of exists otherwise NULL
	 * @throws Exception throws exception in case of errors
	 * @see #turnOnExistingNodeLinking(File)
	 * @see #turnOnExistingNodeLinking(String)
	 * @see #getExistingNodes(String)
	 * @see #preStitchCorrection(SIPDataItem)
	 * @see #postStitchCorrection(SIPDataItem)
	 * @see NDLStitchHierarchy#setLeafTitle(String)
	 */
	protected abstract NDLStitchHierarchy hierarchy(SIPDataItem item) throws Exception;
	
	/**
	 * This section is responsible for correction of data before stitching.
	 * Any pre-stitching correction should be moved here.
	 * @param item item to correct before stitching
	 * @return returns true if item will exist in stitching otherwise false (item delete case)
	 * @throws Exception throws exception in case of any error
	 */
	protected boolean preStitchCorrection(SIPDataItem item) throws Exception {
		return true;
	}
	
	/**
	 * This section is responsible for correction of data after stitching.
	 * Any post-stitching correction should be moved here.
	 * @param item item to correct after stitching
	 * @throws Exception throws exception in case of any error
	 */
	protected void postStitchCorrection(SIPDataItem item) throws Exception {
		// blank implementation
	}
	
	/**
	 * Switching context to custom context
	 * @param contextName custom context
	 * @throws NDLContextSwitchLoadException throws error in case of loading error
	 * @see NDLContextSwitch#switchContext(NDLContext, NDLDataNormalizationPool)
	 */
	public void switchContext(NDLContext context) throws NDLContextSwitchLoadException {
		// switch context
		NDLContextSwitch.switchContext(context, normalizer);
	}
	
	/**
	 * Restores context to original
	 * @throws NDLContextSwitchLoadException throws error in case of loading error
	 * @see NDLContextSwitch#restoreContext() 
	 */
	public static void restoreContext() throws NDLContextSwitchLoadException {
		// restore context
		NDLContextSwitch.restoreContext();
	}
}