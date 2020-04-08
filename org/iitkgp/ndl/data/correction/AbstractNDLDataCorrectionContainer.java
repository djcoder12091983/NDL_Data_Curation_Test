package org.iitkgp.ndl.data.correction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.iitkgp.ndl.context.custom.NDLContext;
import org.iitkgp.ndl.context.custom.NDLContextSwitch;
import org.iitkgp.ndl.context.custom.exception.NDLContextSwitchLoadException;
import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.Filter;
import org.iitkgp.ndl.data.NDLAssetType;
import org.iitkgp.ndl.data.NDLDataItem;
import org.iitkgp.ndl.data.NDLDataPair;
import org.iitkgp.ndl.data.NDLField;
import org.iitkgp.ndl.data.compress.CompressedFileMode;
import org.iitkgp.ndl.data.container.AbstractDataContainer;
import org.iitkgp.ndl.data.container.AbstractNDLDataContainer;
import org.iitkgp.ndl.data.container.ConfigurationData;
import org.iitkgp.ndl.data.container.DataContainerNULLConfiguration;
import org.iitkgp.ndl.data.container.NDLDuplicateFieldCorrectionContainerAdapter;
import org.iitkgp.ndl.data.exception.NDLMultivaluedException;
import org.iitkgp.ndl.data.generation.NDLCSVToSIPGeneration;
import org.iitkgp.ndl.data.hierarchy.NDLRelationNode;
import org.iitkgp.ndl.data.iterator.AIPDataIterator;
import org.iitkgp.ndl.data.iterator.AbstractNDLDataIterator;
import org.iitkgp.ndl.data.iterator.DataIterator;
import org.iitkgp.ndl.data.iterator.DataSourceNULLConfiguration;
import org.iitkgp.ndl.data.iterator.SIPDataIterator;
import org.iitkgp.ndl.data.normalizer.NDLDataNormalizationPool;
import org.iitkgp.ndl.data.normalizer.NDLDataNormalizer;
import org.iitkgp.ndl.data.normalizer.exception.DataNormalizationException;
import org.iitkgp.ndl.data.writer.NDLDataItemWriter;
import org.iitkgp.ndl.relation.HasPart;
import org.iitkgp.ndl.relation.IsPartOf;
import org.iitkgp.ndl.util.CommonUtilities;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * <pre>Responsible for NDL data (SIP/AIP etc.) correction/curation process, logging, assets add etc.</pre>
 * <pre>This class takes care of normalization if any new value added.</pre>
 * <ul>
 * <li>To add custom logic see {@link #correctTargetItem(NDLDataItem)}</li>
 * <li>To add initialization logic see {@link #preProcessData()} {@link #init(DataContainerNULLConfiguration)}</li>
 * <li>To add destroy/complete logic see {@link #postProcessData()} {@link #close()}</li>
 * <li>To add text logging file use
 * {@link #addTextLogger(String)}
 * {@link #addTextLogger(String, String)}
 * </li>
 * <li>To add CSV logging file use
 * {@link #addCSVLogger(String, org.iitkgp.ndl.data.CSVConfiguration)}
 * {@link #addCSVLogger(String)}
 * {@link #addCSVLogger(String, String[], org.iitkgp.ndl.data.CSVConfiguration)}
 * {@link #addCSVLogger(String, String[])}
 * </li>
 * <li>
 * To add mapping resource use
 * {@link #addMappingResource(java.io.File, String)}
 * {@link #addMappingResource(java.io.File, String, String)}
 * </li>
 * </ul>
 * <pre><b>When override a void-method then don't forget to call super method.</b></pre>
 * @param <D> Data item
 * @param <R> Data reader
 * @see AbstractDataContainer
 * @see AbstractNDLDataContainer
 * @see NDLDataItem
 * @see AbstractNDLDataIterator
 * @see SIPDataIterator
 * @see AIPDataIterator
 * @see NDLCSVToSIPGeneration
 * @author Debasis
 */
public abstract class AbstractNDLDataCorrectionContainer<D extends NDLDataItem, R extends DataIterator<D, DataSourceNULLConfiguration>>
		extends AbstractNDLDataContainer<D, R, DataSourceNULLConfiguration> {
	
	String outputLocation; // output location
	NDLDataItemWriter<D> writer = null;
	// asset trackers
	Map<String, String> assetLocations = new HashMap<String, String>(4);
	Map<String, String> assetDefaultLocations = new HashMap<String, String>(4);
	Map<String, Integer> assetStatistics = new HashMap<String, Integer>(4);
	// multiple value separator
	char multipleValueSeparator;
	boolean multipleValueSeparatorFlag = false;
	
	List<D> extraItems = new LinkedList<D>(); // during process if any items splitted into multiple items
	// global normalizers
	NDLDataNormalizationPool normalizers = new NDLDataNormalizationPool();
	// current processing item
	D currentTargetItem = null;
	// delete item relation details
	Map<String, Set<String>> deleteItemRelations = new HashMap<String, Set<String>>(2);
	Set<String> deletedItems = new HashSet<String>(2);
	Set<String> discardItems = new HashSet<String>(2);
	// hierarchy loading flag
	// flag to determine whether to load or not
	protected boolean loadHierarchyFlag = false;
	// this determines whether to access hierarchy information during process
	// if any meta-data to be processed from hierarchy
	protected boolean accessHierarchyFlag = false;
	Map<String, NDLRelationNode> relations = new HashMap<String, NDLRelationNode>(2);
	// duplicate fixation
	Map<String, NDLDuplicateFieldCorrectionContainerAdapter<D>> duplicateFixations = new HashMap<String, NDLDuplicateFieldCorrectionContainerAdapter<D>>(2);
	
	// this flag indicates whether process ll continue even a processing error happens
	boolean continueOnCorrectionError = false;
	final static String __CONTINUE_ON_CORRECTION_ERROR_TRACKER_LOGGER__ = "__DATA_PROCESSING_ERROR_LOGGER__";
	final static String __CONTINUE_ON_CORRECTION_ERROR_TRACKER_HANDLE_LIST__ = "__DATA_PROCESSING_ERROR_HANDLE_LIST__";
	int errorToleranceLevel = 100; // tolerate error on 100 items
	int processingErrorCounter = 0;
	boolean detailProcessingErrorLog = true;
	boolean escapeHTMLFlag = false;
	
	// remove multiple spaces and lines
	boolean removeMultipleSpacesAndLines = false;
	boolean manualMultipleSpacesAndLinesRemoval = false;
	
	/**
	 * Turns off loading hierarchy flag. Before turning off make sure system doesn't need it. 
	 * This typically requires when an AIP item to be deleted
	 * or hierarchy needs to be accessed for a given item
	 */
	@Deprecated
	public void turnOffLoadHierarchyFlag() {
		System.err.println("Now by default load hierarchy flag is OFF. If need to ON then use 'turnOnLoadHierarchyFlag'.");
		this.loadHierarchyFlag = false;
	}
	
	/**
	 * Turns on loading hierarchy flag. Before turning on make sure system needs it. 
	 * This typically requires when an AIP item to be deleted
	 * or hierarchy needs to be accessed for a given item
	 */
	public void turnOnLoadHierarchyFlag() {
		System.err.println("Hierarchy loading is ON, make sure it really needs it.");
		this.loadHierarchyFlag = true;
	}
	
	/**
	 * Turns on access hierarchy flag.
	 * This flag should be ON if item metadata to be processed from parent item
	 */
	public void turnOnAccessHierarchyFlag() {
		this.accessHierarchyFlag = true;
	}
	
	/**
	 * This flag indicates process will continue even a processing error happens
	 * @throws IOException throws error if flag set error occurs
	 */
	public void turnOnContinueOnCorrectionError() throws IOException {
		turnOnContinueOnCorrectionError(100);
	}
	
	/**
	 * This flag indicates process will continue even a processing error happens
	 * @param errorToleranceLevel how many error@items system can tolerate, sets -1 for all items
	 * @throws IOException throws error if flag set error occurs
	 */
	public void turnOnContinueOnCorrectionError(int errorToleranceLevel) throws IOException {
		continueOnCorrectionError = true;
		addTextLogger(__CONTINUE_ON_CORRECTION_ERROR_TRACKER_LOGGER__);
		addTextLogger(__CONTINUE_ON_CORRECTION_ERROR_TRACKER_HANDLE_LIST__);
		this.errorToleranceLevel = errorToleranceLevel;
	}
	
	/**
	 * Sets tolerance level (how many error@items system can tolerate)
	 * <pre>Sets -1 to log details for all error items</pre>
	 * @param errorToleranceLevel how many error@items system can tolerate
	 * @see #turnOnContinueOnCorrectionError(int)
	 */
	@Deprecated
	public void setErrorToleranceLevel(int errorToleranceLevel) {
		this.errorToleranceLevel = errorToleranceLevel;
	}
	
	/**
	 * Turns off 'detail processing error logging', means it logs brief error messages
	 */
	public void turnOffDetailProcessingErrorLog() {
		detailProcessingErrorLog = false;
	}
	
	/**
	 * Turns on HTML escape flag
	 */
	public void turnOnEscapeHTMLFlag() {
		escapeHTMLFlag = true;
	}
	
	/**
	 * Gets current acting/target item
	 * @return returns corresponding item
	 */
	public D getCurrentTargetItem() {
		return currentTargetItem;
	}
	
	/**
	 * Adds field wise duplicate fixation details
	 * @param field which field to handle duplicates
	 * @param braceStart this field determines which character wraps the suffix part, default value is '('
	 * @param braceEnd this field determines which character wraps the suffix part, default value is ')'
	 * @param pairs field configuration by which duplicates to be fixed, fields must be single valued
	 *              each pair contains field name and display name, if display name not required then leave it blank
	 * @see NDLDuplicateFieldCorrectionContainerAdapter
	 */
	public void addDuplicateFixation(String field, char braceStart, char braceEnd, NDLDataPair<String> ... pairs) {
		duplicateFixations.put(field,
				new NDLDuplicateFieldCorrectionContainerAdapter<D>(field, braceStart, braceEnd, pairs));
	}
	
	/**
	 * Adds field wise duplicate fixation details
	 * @param field which field to handle duplicates
	 * @param pairs field configuration by which duplicates to be fixed, fields must be single valued
	 *              each pair contains field name and display name, if display name not required then leave it blank
	 * @see NDLDuplicateFieldCorrectionContainerAdapter
	 */
	public void addDuplicateFixation(String field, NDLDataPair<String> ... pairs) {
		duplicateFixations.put(field, new NDLDuplicateFieldCorrectionContainerAdapter<D>(field, pairs));
	}
	
	/**
	 * Adds field wise duplicate fixation details
	 * @param field which field to handle duplicates
	 * @param braceStart this field determines which character wraps the suffix part, default value is '('
	 * @param braceEnd this field determines which character wraps the suffix part, default value is ')'
	 * @param fields field configuration by which duplicates to be fixed, fields must be single valued
	 * @see NDLDuplicateFieldCorrectionContainerAdapter
	 */
	public void addDuplicateFixation(String field, char braceStart, char braceEnd, String ... fields) {
		duplicateFixations.put(field, new NDLDuplicateFieldCorrectionContainerAdapter<D>(field, braceStart, braceEnd, fields));
	}
	
	/**
	 * Adds field wise duplicate fixation details
	 * @param field which field to handle duplicates
	 * @param fields field configuration by which duplicates to be fixed, fields must be single valued
	 * @see NDLDuplicateFieldCorrectionContainerAdapter
	 */
	public void addDuplicateFixation(String field, String ... fields) {
		duplicateFixations.put(field, new NDLDuplicateFieldCorrectionContainerAdapter<D>(field, fields));
	}
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param outputLocation output location where corrected data to be stored
	 */
	public AbstractNDLDataCorrectionContainer(String input, String logLocation, String outputLocation) {
		super(input, logLocation,
				new DataContainerNULLConfiguration<DataSourceNULLConfiguration>(new DataSourceNULLConfiguration()));
		this.outputLocation = outputLocation;
		
		// NDL context switching initialization
		NDLContextSwitch.init();
	}
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param outputLocation output location where corrected data to be stored
	 * @param globalLoggingFlag global logging flag
	 */
	public AbstractNDLDataCorrectionContainer(String input, String logLocation, String outputLocation,
			boolean globalLoggingFlag) {
		super(input, logLocation,
				new DataContainerNULLConfiguration<DataSourceNULLConfiguration>(new DataSourceNULLConfiguration()));
		this.outputLocation = outputLocation;
		if(!globalLoggingFlag) {
			// turn off
			turnOffGlobalLoggingFlag();
		}
		
		// NDL context switching initialization
		NDLContextSwitch.init();
	}
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param name logical name to add prefix to log files
	 * @param outputLocation output location where corrected data to be stored
	 */
	public AbstractNDLDataCorrectionContainer(String input, String logLocation, String name, String outputLocation) {
		super(input, logLocation, name,
				new DataContainerNULLConfiguration<DataSourceNULLConfiguration>(new DataSourceNULLConfiguration()));
		this.outputLocation = outputLocation;
		
		// NDL context switching initialization
		NDLContextSwitch.init();
	}
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param name logical name to add prefix to log files
	 * @param outputLocation output location where corrected data to be stored
	 * @param globalLoggingFlag global logging flag
	 */
	public AbstractNDLDataCorrectionContainer(String input, String logLocation, String name, String outputLocation,
			boolean globalLoggingFlag) {
		super(input, logLocation, name,
				new DataContainerNULLConfiguration<DataSourceNULLConfiguration>(new DataSourceNULLConfiguration()));
		this.outputLocation = outputLocation;
		if(!globalLoggingFlag) {
			// turn off
			turnOffGlobalLoggingFlag();
		}
		
		// NDL context switching initialization
		NDLContextSwitch.init();
	}
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param name logical name to add prefix to log files
	 * @param outputLocation output location where corrected data to be stored
	 * @param globalLoggingFlag global logging flag
	 * @param validationFlag validation flag for data validation
	 */
	public AbstractNDLDataCorrectionContainer(String input, String logLocation, String name, String outputLocation,
			boolean globalLoggingFlag, boolean validationFlag) {
		super(input, logLocation, name,
				new DataContainerNULLConfiguration<DataSourceNULLConfiguration>(new DataSourceNULLConfiguration()),
				validationFlag);
		this.outputLocation = outputLocation;
		if(!globalLoggingFlag) {
			// turn off
			turnOffGlobalLoggingFlag();
		}
		
		// NDL context switching initialization
		NDLContextSwitch.init();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void init(DataContainerNULLConfiguration<DataSourceNULLConfiguration> configuration) throws IOException {
		// super call
		super.init(configuration);
		writer = new NDLDataItemWriter<D>(outputLocation, getFileName()); // writer
		// handle compression mode for writing
		if(isCompressed()) {
			// compression mode is ON
			writer.setCompressOn(getFileName("Corrected"), CompressedFileMode.TARGZ);
		}
		writer.init(); // initialization writer
	}
	
	/**
	 * This method actually stores corrected data and assets if any 
	 */
	@Override
	public void postProcessItem(D item) throws Exception {
		// super call
		super.postProcessItem(item);
		
		// data validations
		NDLDataUtils.validateNDLData(item, validator);
		
		// adds assets if any
		String assetID = getAssetID(item);
		List<String> missing = NDLDataUtils.addAsset(item, assetLocations, assetID, assetDefaultLocations); // adds assets
		if(!missing.isEmpty()) {
			// logging missing details
			for(String miss : missing) {
				String message = item.getId() + ": " + miss + " asset not found.";
				log(message);
				//System.err.println(message);
				
				// stat tracking
				Integer c = assetStatistics.get(miss);
				if(c == null) {
					assetStatistics.put(miss, 1);
				} else {
					assetStatistics.put(miss, c.intValue() + 1);
				}
			}
		}
		// write the corrected data
		writeItem(item);
	}
	
	/**
	 * Gets folder name to store data for a given item
	 * @param item given item
	 * @return returns folder name
	 */
	public String getFolderName(D item) {
		// blank
		return null;
	}
	
	/**
	 * Corrects whole set of data
	 * @throws Exception throws Exception in case data correction error occurs
	 */
	public void correctData() throws Exception {
		processData(); // process data
	}
	
	/**
	 * Determines whether item to be deleted or not
	 * @param item item to explored whether to delete or not
	 * @return returns true if it's a candidate of delete
	 * @throws Exception throws exception in case exploration occurs with an error
	 */
	protected boolean isDeletedItem(D item) throws Exception {
		return false;
	}
	
	/**
	 * Adds delete item relation details
	 * @param parent parent handle id after slash
	 * @param child parent handle id after slash
	 */
	protected void addDeleteItemRelation(String parent, String child) {
		Set<String> children = deleteItemRelations.get(parent);
		if(children == null) {
			children = new HashSet<String>(2);
			deleteItemRelations.put(parent, children);
		}
		children.add(child);
	}
	
	// adds deleted item
	protected void addDeletedItem(String id) {
		deletedItems.add(id);
	}
	
	/**
	 * Handles/updates parent/associated item on delete of an item
	 * @param parent parent or associated item of deleted item
	 * @throws Exception throws exception in case handle encounters error
	 */
	protected void handleParentOnDelete(D parent) throws Exception {
		String id = NDLDataUtils.getHandleSuffixID(parent.getId());
		Set<String> children = deleteItemRelations.get(id);
		if(!children.isEmpty()) {
			// handle stitching part
			String haspart = parent.getSingleValue("dc.relation.haspart");
			List<HasPart> parts = NDLDataUtils.deserializeHasPartJSON(haspart);
			if(!parts.isEmpty()) {
				// has-part exists
				Iterator<HasPart> iparts = parts.iterator();
				while(iparts.hasNext()) {
					HasPart part = iparts.next();
					if(children.contains(NDLDataUtils.getHandleSuffixID(part.getHandle()))) {
						// child entry to be deleted
						iparts.remove();
					}
				}
				// update new has-part
				parent.updateSingleValue("dc.relation.haspart", NDLDataUtils.serializeHasPart(parts));
			}
			
			// TODO more relations if any
		}
	}
	
	// gets relational information
	NDLRelationNode getRelationNode(D item) {
		List<HasPart> parts = NDLDataUtils.deserializeHasPartJSON(item.getSingleValue("dc.relation.haspart"));
		IsPartOf ispart = NDLDataUtils.deserializeIsPartOfJSON(item.getSingleValue("dc.relation.ispartof"));
		boolean existIspart = ispart != null;
		boolean existParts = !parts.isEmpty();
		if(existIspart || existParts) {
			// valid relation node
			NDLRelationNode node = new NDLRelationNode(item.getId(), item.getSingleValue("dc.title"));
			if(existParts) {
				for(HasPart part : parts) {
					// TODO title information not required
					node.addChild(part.getHandle());
				}
			}
			if(existIspart) {
				// TODO title information not required
				node.setParent(ispart.getHandle());
			}
			return node;
		} else {
			// not a valid relational item
			return null;
		}
		
		// TODO more relations if any
	}
	
	/**
	 * Gets stitching hierarchical information for current item
	 * @param separator information separated by, default is -&gt;
	 * @return returns hierarchical information
	 */
	public String getHierarchyInformation(String separator) {
		D item = getCurrentTargetItem();
		NDLRelationNode node = relations.get(NDLDataUtils.getHandleSuffixID(item.getId()));
		NDLRelationNode parentNode = node.getParent();
		if(parentNode == null) {
			// no information exists
			return StringUtils.EMPTY;
		}
		String parent = parentNode.getId();
		Stack<String> hierarchy = new Stack<String>();
		while(StringUtils.isNotBlank(parent)) {
			// till parent exists
			node = relations.get(parent);
			hierarchy.push(node.getTitle());
			
			NDLRelationNode parentRelation = node.getParent();
			if(parentRelation != null) {
				parent = parentRelation.getId();
			} else {
				// no parent available
				parent = null;
			}
		}
		return NDLDataUtils.join(hierarchy, separator);
	}
	
	/**
	 * Gets hierarchical information separated by '-&gt;' for current item
	 * @return returns hierarchical information
	 * @see #getHierarchyInformation(String)
	 */
	public String getHierarchyInformation() {
		return getHierarchyInformation("->");
	}
	
	/**
	 * Source type specific loading custom hierarchy information
	 * @param item current processing item
	 * @throws Exception throws exception in case loading fails
	 */
	protected void loadCustomRelationNode(D item) throws Exception {
		// blank
	}
	
	// gets associated handles for a given handle
	// basically it returns all child handles including self item
	Collection<String> getAssociatedHandles(String handle) {
		Set<String> associates = new HashSet<String>(2);
		associates.add(handle);
		Queue<String> nodes = new LinkedList<String>();
		nodes.add(handle);
		// BFS scan to get all nodes of sub-tree
		while(nodes.isEmpty()) {
			NDLRelationNode node = relations.get(nodes.poll());
			Collection<String> children = node.getChildren();
			for(String child : children) {
				associates.add(child);
				nodes.add(child);
			}
		}
		
		return associates;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void preProcessData() throws Exception {
		// super call
		super.preProcessData();
		// loads hierarchy
		
		if(removeMultipleSpacesAndLines) {
			// warning message to avoid unnecessary steps
			System.err.println("WARN: Make sure removal of extra spaces and lines is required,"
					+ " because it will take long time to run.");
		} else {
			// warning message to force necessary steps
			System.err.println("WARN: Make sure removal of extra spaces and lines is not required."
					+ " If not then call `turnOnRemoveMultipleSpacesAndLines`.");
		}
		
		if(loadHierarchyFlag) {
			System.out.println("Loading hierarchy infromations ...");
			// flag is ON
			while(dataReader.hasNext()) {
				D item = dataReader.next();
				String itemid = NDLDataUtils.getHandleSuffixID(item.getId());
				NDLRelationNode node = getRelationNode(item);
				if(node != null) {
					// valid relational node
					relations.put(itemid, node);
				}
				if(isDeletedItem(item)) {
					// to be deleted
					discardItems.add(itemid);
				}
				// source type specific hierarchy information loading
				loadCustomRelationNode(item);
			}
			// reset the reader
			dataReader.reset();
			// process delete informations if any
			if(!discardItems.isEmpty()) {
				for(String discardItem : discardItems) {
					deletedItems.addAll(getAssociatedHandles(discardItem));
					NDLRelationNode parent = relations.get(discardItem).getParent();
					if(parent != null) {
						// bottom up approach
						NDLRelationNode parentNode;
						do {
							// delete nodes till non-leaf item found
							String parentID = parent.getId();
							parentNode = relations.get(parentID);
							parentNode.deleteChildByHandle(discardItem);
							// delete mapping
							Set<String> deletes = deleteItemRelations.get(parentID);
							if(deletes == null) {
								deletes = new HashSet<String>(2);
								deleteItemRelations.put(parentID, deletes);
							}
							deletes.add(discardItem);
							
							// next
							discardItem = parentID;
							parent = relations.get(discardItem).getParent();
						} while(parent != null && parentNode.isLeaf());
					}
				}
			}
			if(!accessHierarchyFlag) {
				// flag is OFF, destroy relations
				relations.clear();
			}
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean processItem(D item) throws Exception {
		// custom logic
		currentTargetItem = item;
		String handle = item.getId();
		String id = NDLDataUtils.getHandleSuffixID(handle);
		if(deletedItems.contains(id)) {
			// delete item
			return false;
		}
		
		boolean processed = true;
		try {
			processed = correctTargetItem(currentTargetItem);
		} catch(Exception ex) {
			// processing error occurs
			if(continueOnCorrectionError
					&& (errorToleranceLevel == -1 || ++processingErrorCounter < errorToleranceLevel)) {
				// bypass error
				String emessage = "ERROR@" + handle + " => " + ex.getMessage();
				System.err.println(emessage);
				if(detailProcessingErrorLog) {
					// detail error
					log(__CONTINUE_ON_CORRECTION_ERROR_TRACKER_LOGGER__, "ERROR@" + handle);
					log(__CONTINUE_ON_CORRECTION_ERROR_TRACKER_LOGGER__, CommonUtilities.exceptionDetail(ex));
				} else {
					// brief messages
					log(__CONTINUE_ON_CORRECTION_ERROR_TRACKER_LOGGER__, emessage);
				}
				// track handle list
				log(__CONTINUE_ON_CORRECTION_ERROR_TRACKER_HANDLE_LIST__, handle);
			} else {
				// propagate error
				throw ex;
			}
		}
		
		if (!processed) {
			// skipped
			return false;
		} else {
			// duplicate fixation
			for(String field : duplicateFixations.keySet()) {
				duplicateFixations.get(field).fix(item);
			}
			// checks whether item to be updated due to delete operations
			if(deleteItemRelations.containsKey(id)) {
				handleParentOnDelete(item);
			}
			
			if(removeMultipleSpacesAndLines) {
				// removal of extra spaces and lines
				removeMultipleLinesAndSpaceForAllFields();
			}
			
			// successful
			return true;
		}
	}
	
	/**
	 * Corrects target item
	 * @param target target data item
	 * @return returns true if process successful otherwise false
	 * @throws Exception throws error in case of transformation error
	 */
	protected abstract boolean correctTargetItem(D target) throws Exception;
	
	/**
	 * This method allows extra items if any
	 * @throws Exception throws exception in case of errors
	 */
	protected void handleExtraItems() throws Exception {
		// blank
	}
	
	// sets item folder before write
	void setFolderBeforeWrite(D item) {
		String folder = getFolderName(item);
		if(StringUtils.isNotBlank(folder)) {
			// sets folder if provided
			item.setFolder(folder);
		}
	}
	
	/**
	 * Writes list of items.
	 * This is typically required when on custom basis extra items generated and stored
	 * @param items items to write
	 * @throws IOException throws error in case write fails
	 */
	protected void writeItems(List<D> items) throws IOException {
		for(D item : items) {
			writeItem(item);
		}
	}
	
	/**
	 * Writes item.
	 * This is typically required when on custom basis extra items generated and stored
	 * @param item item to write
	 * @throws IOException throws error in case write fails
	 */
	protected void writeItem(D item) throws IOException {
		setFolderBeforeWrite(item);
		writer.write(item);
	}
	
	/**
	 * This methods persists extra items if any
	 */
	@Override
	public void postProcessData() throws Exception {
		// super call
		super.postProcessData();
		handleExtraItems(); // handle extra items if any
		// persists extra items
		if(!extraItems.isEmpty()) {
			System.out.println("Extra items writing: " + extraItems.size());
			for(D item : extraItems) {
				writeItem(item);
			}
		}
		// duplicate fixation counts
		for(String field : duplicateFixations.keySet()) {
			System.out.println(
					"[" + field + "] still duplicate count: " + duplicateFixations.get(field).getDuplicateCount());
		}
		// shows asset statistics
		showAssetStatistics();
		
		// context switch destroy
		// TODO right now it does do anything
		NDLContextSwitch.destroy();
	}
	
	// shows asset statistics
	void showAssetStatistics() {
		// asset statistics
		for(String asset : assetStatistics.keySet()) {
			if(asset.endsWith(":" + NDLDataUtils.DEFAULT_ASSET)) {
				// default asset
				System.out.println("Total " + asset + ": " + assetStatistics.get(asset));
			} else {
				// missing
				System.out.println("Total " + asset + " missing: " + assetStatistics.get(asset));
			}
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void intermediateProcessHandler() {
		// shows asset statistics
		showAssetStatistics();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {
		// super call
		super.close();
		// close writer
		IOUtils.closeQuietly(writer);
	}
	
	/**
	 * Adds normalizer against a field, if additionally normalizer needs to be loaded which is not defined
	 * in <b>/conf/default.data.normalization.conf.properties</b>
	 * @param field field with which normalizer is associated
	 * @param normalizer normalizer class, it should be type of {@link NDLDataNormalizer}
	 * @see NDLDataNormalizationPool#addNormalizer(String, String)
	 * @see #addNormalizer(String, NDLDataNormalizer)
	 */
	public void addNormalizer(String field, String normalizer) {
		normalizers.addNormalizer(field, normalizer);
	}
	
	/**
	 * Adds normalizer as class instance
	 * @param field for which field normalizer to be added
	 * @param normalizer normalizer to be added
	 */
	public void addNormalizer(String field, NDLDataNormalizer normalizer) {
		normalizers.addNormalizer(field, normalizer);
	}
	
	/**
	 * Deregisters normalizer for a given field
	 * @param field given field for which normalizer to be deregistered. 
	 */
	public void deregisterNormalizer(String field) {
		normalizers.deregisterNormalizer(field);
	}
	
	/**
	 * Adds extra item if an item required to split into multiple items
	 * @param item item to add
	 */
	public void addItem(D item) {
		extraItems.add(item);
	}
	
	/**
	 * <pre>Gets ID to identify asset from asset location (thumbnails/licenses/fulltexts etc.).</pre>
	 * <pre>Default implementation is suffix ID across the source, for different logic this should be overridden.</pre>
	 * @param item from which the ID will be extracted
	 * @return returns associated ID
	 */
	protected String getAssetID(D item) { 
		String id = item.getId();
		return id.substring(id.indexOf('/') + 1);
	}
	
	/**
	 * Adds NDL asset location from which assets to be loaded by some identifier
	 * @param type NDL asset type
	 * @param location NDL asset location
	 * @see #getAssetID(NDLDataItem)
	 */
	public void addAssetLocation(NDLAssetType type, String location) {
		assetLocations.put(type.getType(), location);
	}
	
	/**
	 * Adds NDL asset default/fallback location from which assets to be loaded by some identifier
	 * @param type NDL asset type
	 * @param location NDL asset location
	 * @see #getAssetID(NDLDataItem)
	 */
	public void addAssetDefaultLocation(NDLAssetType type, String location) {
		assetDefaultLocations.put(type.getType(), location);
	}
	
	/**
	 * Sets multiple value separator character by which multiple values splitted and placed accordingly
	 * @param multipleValueSeparator multiple value separator character
	 */
	public void setMultipleValueSeparator(char multipleValueSeparator) {
		this.multipleValueSeparator = multipleValueSeparator;
		multipleValueSeparatorFlag = true;
	}
	
	 /**
     * Gets single value for a given field
     * @param field given field name
     * @return a single value corresponding to name, NULL if not found
     * @throws NDLMultivaluedException throws error when single exists in multiple-entry node
     * @see NDLDataItem#getSingleValue(String)
     */
	public String getSingleValue(String field) {
		return currentTargetItem.getSingleValue(field);
	}
	
	/**
     * Gets list of values for a given field/attribute
     * @param field field name
     * @return a list of value corresponding to name, empty in case no data found
     * @see NDLDataItem#getValue(String)
     */
	public List<String> getValue(String field) {
		return currentTargetItem.getValue(field);
	}
	
	/**
	 * Adds values to given field
	 * @param field target field to add
	 * @param values value to be added
	 */
	public void add(String field, String ... values) {
		if(values.length == 0) {
			throw new IllegalArgumentException("At least one value is expected.");
		}
		Set<String> normalizedValues = new HashSet<String>(2);
		for(String value : values) {
			normalizedValues.addAll(normalizers.normalize(field, value));
		}
		currentTargetItem.add(field, normalizedValues);
	}
	
	/**
	 * Adds values to given field
	 * @param field target field to add
	 * @param values value to be added
	 */
	public void add(String field, Set<String> values) {
		Set<String> normalizedValues = new HashSet<String>(2);
		for(String value : values) {
			normalizedValues.addAll(normalizers.normalize(field, value));
		}
		currentTargetItem.add(field, normalizedValues);
	}
	
	/**
	 * Adds value to given field
	 * @param field target field to add
	 * @param value value to be added
	 */
	public void add(String field, String value) {
		if(!currentTargetItem.contains(field, value)) {
			// cross check before adding
			if(multipleValueSeparatorFlag) {
				// separator exists
				currentTargetItem.addIfNotContains(field, normalizers.normalize(field, multipleValueSeparator, value));
			} else {
				// normal add
				currentTargetItem.addIfNotContains(field, normalizers.normalize(field, value));
			}
		}
	}
	
	/**
	 * Adds value to given field by a custom multiple value separator character
	 * @param field target field to add
	 * @param value value to be added
	 * @param multipleValueSeparator multiple value separator character by which multiple values splitted and placed accordingly
	 * @see #add(String, String)
	 */
	public void add(String field, String value, char multipleValueSeparator) {
		currentTargetItem.addIfNotContains(field, normalizers.normalize(field, multipleValueSeparator, value));
	}
	
	/**
	 * Adds value to given field by a custom multiple value separator character
	 * @param field target field to add
	 * @param values values to be added
	 * @param multipleValueSeparator multiple value separator character by which multiple values splitted and placed accordingly
	 * @see #add(String, String)
	 */
	public void add(String field, Collection<String> values, char multipleValueSeparator) {
		Set<String> newValues = new HashSet<String>(2);
		for(String value : values) {
			newValues.addAll(normalizers.normalize(field, multipleValueSeparator, value));
		}
		currentTargetItem.addIfNotContains(field, newValues);
	}
	
	/**
	 * Deletes values from fields by given pattern in regular expression
	 * @param field field to delete values
	 * @param regex given pattern in regular expression
	 * @return returns number of values deleted
	 */
	public int deleteIfContainsByRegex(String field, String regex) {
		NDLField ndlField = new NDLField(field);
		String jsonKey = ndlField.getJsonKey();
		List<NDLDataNode> nodes = currentTargetItem.getNodes(field);
		int c = 0;
		for(NDLDataNode node : nodes) {
			// iterate nodes
			String val = NDLDataUtils.getNDLFieldValue(node.getTextContent(), jsonKey, escapeHTMLFlag, true);
			val = val.replaceAll("\\r?\\n", " "); // normalize the text
			if(val != null) {
				// value found
				if(val.matches(regex)) {
					// delete criteria
					node.remove();
					c++;
				}
			}
		}
		return c;
	}
	
	/**
	 * Deletes field for a given set of inclusive values
	 * @param field target field to delete
	 * @param includes inclusive data set to delete
	 * @param ignoreCase whether case is considered or not
	 * @throws IOException throws error if logging fails
	 * @return returns no of fields affected
	 * @see #deleteIfContains(String, Set)
	 */
	public int deleteIfContains(String field, Set<String> includes, boolean ignoreCase) throws IOException {
		Set<String> filter = new HashSet<String>(2);
		// normalize filter data-set
		for(String v : includes) {
			filter.add(ignoreCase ? v.toLowerCase().trim() : v.trim());
		}
		NDLField ndlField = new NDLField(field);
		String jsonKey = ndlField.getJsonKey();
		List<NDLDataNode> nodes = currentTargetItem.getNodes(field);
		int c = 0;
		for(NDLDataNode node : nodes) {
			// iterate nodes
			String val = NDLDataUtils.getNDLFieldValue(node.getTextContent(), jsonKey, escapeHTMLFlag, true);
			if(val != null) {
				// value found
				if(ignoreCase) {
					// normalize
					val = val.toLowerCase();
				}
				if(filter.contains(val)) {
					// to be deleted
					node.remove();
					log(val + " deleted from " + field); // log
					c++;
				}
			}
		}
		return c;
	}
	
	/**
	 * Deletes content if partially matched
	 * @param field field name
	 * @param values values to check 
	 * @param ignoreCase ignore case flag
	 * @return returns no of fields affected
	 * @throws IOException throws error if logging fails
	 */
	public int deleteIfPartiallyContains(String field, Collection<String> values, boolean ignoreCase)
			throws IOException {
		List<String> filter = new LinkedList<String>();
		// normalize filter data-set
		for(String v : values) {
			filter.add(ignoreCase ? v.toLowerCase().trim() : v.trim());
		}
		NDLField ndlField = new NDLField(field);
		String jsonKey = ndlField.getJsonKey();
		List<NDLDataNode> nodes = currentTargetItem.getNodes(field);
		int c = 0;
		for(NDLDataNode node : nodes) {
			// iterate nodes
			String val = NDLDataUtils.getNDLFieldValue(node.getTextContent(), jsonKey, escapeHTMLFlag, true);
			if(val != null) {
				// value found
				if(ignoreCase) {
					// normalize
					val = val.toLowerCase();
				}
				boolean found = false;
				for(String f : filter) {
					if(StringUtils.contains(val, f)) {
						// match found
						found = true;
						break;
					}
				}
				if(found) {
					// delete
					node.remove();
					log(val + " deleted from " + field); // log
					c++;
				}
			}
		}
		return c;
	}
	
	/**
	 * Deletes content if partially matched
	 * @param field field name
	 * @param values values to check
	 * @return returns no of fields affected 
	 * @throws IOException throws error if logging fails
	 */
	public int deleteIfPartiallyContains(String field, Collection<String> values) throws IOException {
		return deleteIfPartiallyContains(field, values, true);
	}
	
	/**
	 * Deletes content if partially matched
	 * @param field field name
	 * @param values values to check
	 * @return returns no of fields affected 
	 * @throws IOException throws error if logging fails
	 */
	public int deleteIfPartiallyContains(String field, String ... values) throws IOException {
		return deleteIfPartiallyContains(field, Arrays.asList(values), true);
	}
	
	/**
	 * Deletes content if partially matched
	 * @param field field name
	 * @param ignoreCase ignore case flag
	 * @param values values to check
	 * @return returns no of fields affected 
	 * @throws IOException throws error if logging fails
	 */
	public int deleteIfPartiallyContains(String field, boolean ignoreCase, String ... values) throws IOException {
		return deleteIfPartiallyContains(field, Arrays.asList(values), ignoreCase);
	}
	
	/**
	 * Deletes field for a given set of inclusive values
	 * @param field target field to delete
	 * @param includes inclusive data set to delete
	 * @return returns no of fields affected
	 * @throws IOException throws error if logging fails
	 */
	public int deleteIfContains(String field, Set<String> includes) throws IOException {
		return deleteIfContains(field, includes, false); // default ignore-case flag is false
	}
	
	/**
	 * Deletes field
	 * @param field field name
	 * @throws IOException throws error if logging fails
	 * @return returns no of fields affected
	 * @see NDLDataItem#delete(String, Filter)
	 */
	public int delete(String field) throws IOException {
		return currentTargetItem.delete(field);
	}
	
	/**
	 * Deletes field
	 * @param fields fields name
	 * @return returns no of fields affected
	 * @throws IOException throws error if logging fails
	 * @see NDLDataItem#delete(String, Filter)
	 */
	public int delete(String ... fields) throws IOException {
		int c = 0;
		for(String field : fields) {
			c += currentTargetItem.delete(field);
		}
		return c;
	}
	
	/**
	 * Deletes a field value mapped by key {@link #addMappingResource(java.io.File, String)},
	 * key is second parameter (logical name). The resource file contains only one column.
	 * @param field which field to delete
	 * @param keyName key is second parameter (logical name) of {@link #addMappingResource(java.io.File, String)}
	 * @param ignoreCase this flag determines matching case sensitivity, if this flag is on then make sure
	 * {@link #addMappingResource(java.io.File, String, boolean)} is used with <b>ignoreCase</b> ON.
	 * @return returns no of fields affected
	 * @throws IOException throws error if logging fails
	 * @see #addMappingResource(java.io.File, String)
	 */
	public int deleteByMappingKey(String field, String keyName, boolean ignoreCase) throws IOException {
		NDLField ndlField = new NDLField(field);
		List<NDLDataNode> nodes = currentTargetItem.getNodes(field);
		int c = 0;
		for(NDLDataNode node : nodes) {
			String value = NDLDataUtils.getNDLFieldValue(node.getTextContent(), ndlField.getJsonKey(), escapeHTMLFlag, true);
			if(ignoreCase) {
				value = value.toLowerCase();
			}
			if(containsMappingKey(ConfigurationData.escapeDot(keyName) + "." + ConfigurationData.escapeDot(value))) {
				// match found, so remove the field
				node.remove();
				c++;
			}
		}
		return c;
	}
	
	/**
	 * Deletes a field value mapped by key {@link #addMappingResource(java.io.File, String)},
	 * key is second parameter (logical name). The resource file contains only one column.
	 * @param field which field to delete
	 * @param keyName key is second parameter (logical name) of {@link #addMappingResource(java.io.File, String)}
	 * @return returns no of fields affected
	 * @throws IOException throws error if logging fails
	 * @see #addMappingResource(java.io.File, String)
	 * @see #deleteByMappingKey(String, String, boolean)
	 */
	public int deleteByMappingKey(String field, String keyName) throws IOException {
		return deleteByMappingKey(field, keyName, false);
	}
	
	/**
	 * Deletes field for a given set of inclusive values
	 * @param field target field to delete
	 * @param includes inclusive data set to delete
	 * @param ignoreCase this flag determines matching case sensitivity, if this flag is on then make sure
	 * @return returns no of fields affected
	 * @throws IOException throws error if logging fails
	 * @see #deleteIfContains(String, Set)
	 */
	public int deleteIfContains(String field, boolean ignoreCase, String ... includes) throws IOException {
		Set<String> newIncludes = new HashSet<String>(2);
		for(String value : includes) {
			if(StringUtils.isNotBlank(value)) {
				// valid value
				newIncludes.add(value);
			}
		}
		return deleteIfContains(field, newIncludes, ignoreCase); // default ignore-case flag is false
	}
	
	/**
	 * Deletes field for a given set of inclusive values
	 * @param field target field to delete
	 * @param includes inclusive data set to delete
	 * @return returns no of fields affected
	 * @throws IOException throws error if logging fails
	 * @see #deleteIfContains(String, Set)
	 */
	public int deleteIfContains(String field, String ... includes) throws IOException {
		return deleteIfContains(field, false, includes);
	}
	
	/**
	 * Deletes field for a given set of exclusive values, 
	 * @param field target field to delete
	 * @param excludes exclusive data set, except these rest gets deleted
	 * @param ignoreCase whether case is considered or not
	 * @return returns no of fields affected
	 * @throws IOException throws error if logging fails
	 * @see #deleteIfNotContains(String, Set)
	 */
	public int deleteIfNotContains(String field, Set<String> excludes, boolean ignoreCase) throws IOException {
		Set<String> filter = new HashSet<String>(2);
		// normalize filter data-set
		for(String v : excludes) {
			filter.add(ignoreCase ? v.toLowerCase().trim() : v.trim());
		}
		NDLField ndlField = new NDLField(field);
		String jsonKey = ndlField.getJsonKey();
		List<NDLDataNode> nodes = currentTargetItem.getNodes(field);
		int c = 0;
		for(NDLDataNode node : nodes) {
			String val = NDLDataUtils.getNDLFieldValue(node.getTextContent(), jsonKey, escapeHTMLFlag, true);
			if(val != null) {
				// value found
				if(ignoreCase) {
					// normalize
					val = val.toLowerCase();
				}
				if(!filter.contains(val)) {
					// to be deleted
					node.remove();
					log(val + " deleted from " + field); // log
					c++;
				}
			}
		}
		return c;
	}
	
	/**
	 * Moves from source to destination
	 * @param source old field
	 * @param destination new field
	 * @return returns no of fields affected
	 * @throws IOException throws error if logging fails
	 * @see NDLDataItem#move(String, String, Filter)
	 * @see NDLDataItem#move(String, String, org.iitkgp.ndl.data.Transformer)
	 * @see NDLDataItem#move(String, String, Filter, org.iitkgp.ndl.data.Transformer)
	 */
	public int move(String source, String destination) throws IOException {
		return currentTargetItem.move(source, destination);
	}
	

	/**
	 * Moves from source to destination if source contains given set of values
	 * @param source source field
	 * @param destination destination field
	 * @param values sources' given set of values for move
	 * @return returns no of fields affected
	 * @see #moveIfPartiallyContains(String, String, Collection)
	 * @see #moveIfPartiallyContains(String, String, Collection, boolean)
	 */
	public int moveIfContains(String source, String destination, Collection<String> values) {
		return currentTargetItem.move(source, destination, new Filter<String>() {
			@Override
			public boolean filter(String data) {
				return values.contains(data);
			}
		});
	}
	
	/**
	 * Moves from source to destination if source partially contains given set of values
	 * @param source source field
	 * @param destination destination field
	 * @param values sources' given set of values for move
	 * @param ignoreCase ignore case flag, whether matching logic is ignoring case of strict
	 * @return returns no of fields affected
	 */
	public int moveIfPartiallyContains(String source, String destination, Collection<String> values,
			boolean ignoreCase) {
		return currentTargetItem.move(source, destination, new Filter<String>() {
			@Override
			public boolean filter(String data) {
				for(String value : values) {
					boolean f = ignoreCase ? StringUtils.containsIgnoreCase(data, value)
							: StringUtils.contains(data, value);
					if(f) {
						return true;
					}
				}
				return false;
			}
		});
	}
	
	/**
	 * Moves from source to destination if source partially contains given set of values
	 * @param source source field
	 * @param destination destination field
	 * @param values sources' given set of values for move
	 * @return returns no of fields affected
	 * @see #moveIfPartiallyContains(String, String, Collection, boolean)
	 */
	public int moveIfPartiallyContains(String source, String destination, Collection<String> values) {
		return moveIfPartiallyContains(source, destination, values, false);
	}
	
	/**
	 * Moves from source to destination if source contains given set of values
	 * @param source source field
	 * @param destination destination field
	 * @param partialMatch partial match flag, whether matching logic is partial or full
	 * @param ignoreCase ignore case flag, whether matching logic is ignoring case of strict
	 * @param values sources' given set of values for move
	 * @return returns no of fields affected
	 * @throws IOException throws error if logging fails
	 */
	public int moveIfContains(String source, String destination, boolean partialMatch, boolean ignoreCase,
			String... values) throws IOException {
		if(values.length == 0) {
			throw new IllegalArgumentException("Values should be at least one.");
		}
		return currentTargetItem.move(source, destination, new Filter<String>() {
			@Override
			public boolean filter(String data) {
				// filter logic
				for(String value : values) {
					boolean f = false;
					if(partialMatch) {
						if(ignoreCase) {
							f = StringUtils.containsIgnoreCase(data, value);
						} else {
							f = StringUtils.contains(data, value);
						}
					} else {
						if(ignoreCase) {
							f = StringUtils.equalsIgnoreCase(data, value);
						} else {
							f = StringUtils.equals(data, value);
						}
					}
					if(f) {
						return true;
					}
				}
				return false;
			}
		});
	}
	
	/**
	 * Moves from source to destination if source contains given set of values
	 * @param source source field
	 * @param destination destination field
	 * @param partialMatch partial match flag, whether matching logic is partial or full
	 * @param values sources' given set of values for move
	 * @return returns no of fields affected
	 * @throws IOException throws error if logging fails
	 * @see #moveIfContains(String, String, boolean, boolean, String...)
	 */
	public int moveIfContains(String source, String destination, boolean partialMatch, String... values)
			throws IOException {
		return moveIfContains(source, destination, partialMatch, false, values);
	}
	
	/**
	 * Moves from source to destination if source contains given set of values
	 * @param source source field
	 * @param destination destination field
	 * @param values sources' given set of values for move
	 * @return returns no of fields affected
	 * @throws IOException throws error if logging fails
	 * @see #moveIfContains(String, String, boolean, boolean, String...)
	 */
	public int moveIfContains(String source, String destination, String... values) throws IOException {
		return moveIfContains(source, destination, false, false, values);
	}
	
	/**
	 * Deletes field for a given set of exclusive values, 
	 * @param field target field to delete
	 * @return returns no of fields affected
	 * @param excludes exclusive data set, except these rest gets deleted
	 * @throws IOException throws error if logging fails
	 */
	public int deleteIfNotContains(String field, Set<String> excludes) throws IOException {
		return deleteIfNotContains(field, excludes, false); // default ignore-case flag is false
	}
	
	/**
	 * Deletes field for a given set of exclusive values, 
	 * @param field target field to delete
	 * @param excludes exclusive data set, except these rest gets deleted
	 * @return returns no of fields affected
	 * @throws IOException throws error if logging fails
	 */
	public int deleteIfNotContains(String field, String ... excludes) throws IOException {
		Set<String> newExcludes = new HashSet<String>(2);
		for(String value : excludes) {
			if(StringUtils.isNotBlank(value)) {
				// valid value
				newExcludes.add(value);
			}
		}
		return deleteIfNotContains(field, newExcludes, false); // default ignore-case flag is false
	}
	
	// for internal usage
	int split(String field, String separator, boolean regex) {
		D item = getCurrentTargetItem();
		NDLField ndlField = new NDLField(field);
		List<NDLDataNode> nodes = item.getNodes(field);
		int c = 0;
		for(NDLDataNode node : nodes) {
			String value = NDLDataUtils.getNDLFieldValue(node.getTextContent(), ndlField.getJsonKey(), escapeHTMLFlag, true);
			node.remove(); // remove and add new values
			Set<String> values = regex ? NDLDataUtils.getUniqueValues(value, separator)
					: NDLDataUtils.getUniqueValues(value, separator.charAt(0));
			c += values.size();
			item.add(field, values); // add
		}
		return c;
	}
	
	/**
	 * Splits the field value by 'multipleValueSeparator' 
	 * @param field field value to split
	 * @param multipleValueSeparator split by 'multipleValueSeparator'
	 * @return returns how many values splitted
	 */
	public int split(String field, char multipleValueSeparator) {
		return split(field, String.valueOf(multipleValueSeparator), false);
	}
	
	/**
	 * Splits the field value by 'multipleValueSeparator' 
	 * @param field field value to split
	 * @param regex split by 'regex'
	 * @return returns how many values splitted
	 */
	public int splitByRegex(String field, String regex) {
		return split(field, regex, true);
	}

	
	/**
	 * Normalizes fields
	 * @param fields target fields to normalize
	 * @throws DataNormalizationException throws error if normalization fails
	 * @see #addNormalizer(String, String)
	 */
	public void normalize(String ... fields) throws DataNormalizationException {
		for(String field : fields) {
			normalize(field, null); // without separator
		}
	}
	
	/**
	 * Normalizes a field for a given item
	 * @param item target item
	 * @param field target filed to normalize
	 * @param multipleValueSeparator multiple value separator character by which multiple values splitted and placed accordingly
	 * @throws DataNormalizationException throws error if normalization fails
	 * @see #addNormalizer(String, String)
	 * @see #normalize(String...)
	 */
	public void normalize(D item, String field, Character multipleValueSeparator)
			throws DataNormalizationException {
		NDLField ndlField = new NDLField(field);
		List<NDLDataNode> nodes = item.getNodes(field);
		for(NDLDataNode node : nodes) {
			String value = NDLDataUtils.getNDLFieldValue(node.getTextContent(), ndlField.getJsonKey(), escapeHTMLFlag, true);
			Set<String> normalizedValues = normalizers.normalize(field, multipleValueSeparator, value); // normalize
			if(normalizedValues.isEmpty()) {
				// normalization fails
				try {
					log("Field: " + field + " normalization fails with: " + value);
				} catch(IOException ex) {
					// suppress error
					System.err.println("Logging fails.");
				}
			} else {
				// remove and update with new one
				node.remove(); // remove old one
				item.addIfNotContains(field, normalizedValues); // add
			}
		}
	}
	
	/**
	 * Normalizes a field current target item
	 * @param field target filed to normalize
	 * @param multipleValueSeparator multiple value separator character by which multiple values splitted and placed accordingly
	 * @throws DataNormalizationException throws error if normalization fails
	 * @see #normalize(NDLDataItem, String, Character)
	 */
	public void normalize(String field, Character multipleValueSeparator) throws DataNormalizationException {
		normalize(currentTargetItem, field, multipleValueSeparator);
	}
	
	/**
	 * Normalizes field with given normalizer
	 * @param field given field
	 * @param normalizer given normalizer
	 */
	public void normalizeByOwn(String field, NDLDataNormalizer normalizer) {
		normalizeByOwn(field, normalizer, false);
	}
	
	/**
	 * Normalizes field with given normalizer
	 * @param field given field
	 * @param normalizer given normalizer
	 * @param deleteInvalid deletes data if conversion is not successful
	 */
	public void normalizeByOwn(String field, NDLDataNormalizer normalizer, boolean deleteInvalid) {
		NDLField ndlField = new NDLField(field);
		List<NDLDataNode> nodes = currentTargetItem.getNodes(field);
		for(NDLDataNode node : nodes) {
			String value = NDLDataUtils.getNDLFieldValue(node.getTextContent(), ndlField.getJsonKey(), escapeHTMLFlag, true);
			Collection<String> values = normalizer.normalize(value);
			if(values != null) {
				// normalized
				node.remove();
				for(String v : values) {
					currentTargetItem.addIfNotContains(field, v);
				}
			} else {
				if(deleteInvalid) {
					node.remove();
				}
			}
		}
	}
	
	/**
	 * Removes multiple spaces from given set of fields
	 * @param fields given set of fields
	 */
	protected void removeMultipleSpaces(Set<String> fields) {
		for(String field : fields) {
			splitAndReplace(NDLDataUtils.SPACE_REGX, " ", false, field);
		}
	}
	
	/**
	 * Removes multiple spaces from given set of fields
	 * @param fields given set of fields
	 * @param escapeXML this flag requires special character to handle
	 */
	protected void removeMultipleSpaces(Set<String> fields, boolean escapeXML) {
		for(String field : fields) {
			splitAndReplace(NDLDataUtils.SPACE_REGX, " ", false, field);
		}
	}
	
	/**
	 * Removes multiple lines and space for all fields
	 */
	protected void removeMultipleLinesAndSpaceForAllFields() {
		Set<String> fields = currentTargetItem.getAllFields();
		removeMultipleLines(fields);
		removeMultipleSpaces(fields);
	}
	
	/**
	 * Removes multiple lines and space for specific fields
	 * @param fields specific fields
	 */
	protected void removeMultipleLinesAndSpace(String ... fields) {
		removeMultipleLines(fields);
		removeMultipleSpaces(fields);
	}
	
	/**
	 * Removes multiple lines and space for all fields
	 * @param escapeXML this flag requires special character to handle
	 */
	protected void removeMultipleLinesAndSpaceForAllFields(boolean escapeXML) {
		Set<String> fields = currentTargetItem.getAllFields();
		removeMultipleLines(fields, escapeXML);
		removeMultipleSpaces(fields, escapeXML);
	}
	
	/**
	 * Removes multiple spaces for given fields
	 * @param fields given field
	 */
	protected void removeMultipleSpaces(String ... fields) {
		splitAndReplace(NDLDataUtils.SPACE_REGX, " ", false, fields);
	}
	
	/**
	 * Removes multiple spaces for given fields
	 * <pre>If 'escapeXML' is ON then use full-formed JSON field, don't use generic JSON field name.</pre>
	 * @param escapeXML this flag requires special character to handle
	 * @param fields given field
	 */
	protected void removeMultipleSpaces(boolean escapeXML, String ... fields) {
		splitAndReplace(NDLDataUtils.SPACE_REGX, " ", escapeXML, fields);
	}
	
	/**
	 * Removes new lines and replace the new lines by a given character
	 * @param replace given replacement character
	 * @param fields fields to remove new lines
	 */
	protected void removeMultipleLines(char replace, Set<String> fields) {
		for(String field : fields) {
			splitAndReplace(NDLDataUtils.MULTILINE_REGX, String.valueOf(replace), false, field);
		}
	}
	
	/**
	 * Removes new lines and replace the new lines by a given character
	 * @param replace given replacement character
	 * @param fields fields to remove new lines
	 */
	protected void removeMultipleLines(char replace, String ... fields) {
		splitAndReplace(NDLDataUtils.MULTILINE_REGX, String.valueOf(replace), false, fields);
	}
	
	/**
	 * Removes new lines and replace the new lines by a given character
	 * <pre>If 'escapeXML' is ON then use full-formed JSON field, don't use generic JSON field name.</pre>
	 * @param replace given replacement character
	 * @param escapeXML this flag requires special character to handle
	 * @param fields fields to remove new lines
	 */
	protected void removeMultipleLines(char replace, boolean escapeXML, String ... fields) {
		splitAndReplace(NDLDataUtils.MULTILINE_REGX, String.valueOf(replace), false, fields);
	}
	
	/**
	 * Removes new lines and replace the new lines by space
	 * @param fields fields to remove new lines
	 * @param escapeXML this flag requires special character to handle
	 */
	protected void removeMultipleLines(Set<String> fields, boolean escapeXML) {
		for(String field : fields) {
			splitAndReplace(NDLDataUtils.MULTILINE_REGX, " ", false, field);
		}
	}
	
	/**
	 * Removes new lines and replace the new lines by space
	 * @param fields fields to remove new lines
	 */
	protected void removeMultipleLines(Set<String> fields) {
		for(String field : fields) {
			splitAndReplace(NDLDataUtils.MULTILINE_REGX, " ", false, field);
		}
	}
	
	/**
	 * Removes new lines and replace the new lines by space
	 * @param fields fields to remove new lines
	 */
	protected void removeMultipleLines(String ... fields) {
		splitAndReplace(NDLDataUtils.MULTILINE_REGX, " ", false, fields);
	}
	
	/**
	 * Removes new lines and replace the new lines by space
	 * <pre>If 'escapeXML' is ON then use full-formed JSON field, don't use generic JSON field name.</pre>
	 * @param escapeXML this flag requires special character to handle
	 * @param fields fields to remove new lines
	 */
	protected void removeMultipleLines(boolean escapeXML, String ... fields) {
		splitAndReplace(NDLDataUtils.MULTILINE_REGX, " ", escapeXML, fields);
	}
	
	/**
	 * Turns on removal of extra spaces and multiple lines
	 * @see #turnOnManualMultipleSpacesAndLinesRemoval()
	 */
	public void turnOnRemoveMultipleSpacesAndLines() {
		removeMultipleSpacesAndLines = true;
		manualMultipleSpacesAndLinesRemoval = false;
	}
	
	/**
	 * Turns on removal of extra spaces and multiple lines.
	 * <pre>Note: it's required when removal API(s) called according to program requirement.</pre>
	 */
	public void turnOnManualMultipleSpacesAndLinesRemoval() {
		manualMultipleSpacesAndLinesRemoval = true;
		removeMultipleSpacesAndLines = false;
	}
	
	// split values and replace
	void splitAndReplace(String regex, String replace, boolean escapeXML, String ... fields) {
		if(!removeMultipleSpacesAndLines && !manualMultipleSpacesAndLinesRemoval) {
			// cross check if called by without setting the flag
			throw new IllegalStateException(
					"Set the flag (`turnOnRemoveMultipleSpacesAndLines`) to remove extra spaces and lines."
							+ " No need to call removal API(s), it's handled by internally if this flag is ON."
							+ " If removal API(s) call point is according to program requirement then set the flag "
							+ "`turnOnManualMultipleSpacesAndLinesRemoval`.");
		}
		if(fields.length == 0) {
			throw new IllegalArgumentException("Fields must be one.");
		}
		for(String field : fields) {
			NDLField ndlField = new NDLField(field);
			String json = ndlField.getJsonKey();
			List<NDLDataNode> nodes = currentTargetItem.getNodes(field);
			for(NDLDataNode node : nodes) {
				String value = NDLDataUtils.getNDLFieldValue(node.getTextContent(), json, escapeHTMLFlag, true);
				String tokens[] = value.split(regex);
				StringBuilder modified = new StringBuilder();
				for(String token : tokens) {
					modified.append(token).append(replace);
				}
				if(modified.length() > 0) {
					modified.deleteCharAt(modified.length() - 1);
				} else {
					// invalid node to be removed
					node.remove();
				}
				
				String modifiedv = modified.toString().trim();
				if(escapeXML) {
					// because NDLDataUtils#getNDLFieldValue uses StringEscapeUtils#unescapeHtml4
					modifiedv = StringEscapeUtils.escapeXml11(modifiedv);
				}
				// update value
				if(StringUtils.isNotBlank(ndlField.getJsonKey())) {
					// JSON
					node.setTextContent(NDLDataUtils.getJson(json, modifiedv));
				} else {
					node.setTextContent(modifiedv);
				}
			}
		}
	}
	
	/**
	 * Transforms selected field by a given exact mapping
	 * @param field selected field
	 * @param map map to use transform field value
	 * @return returns affected fields
	 */
	public int transformFieldByExactMatch(String field, Map<String, String> map) {
		NDLField ndlField = new NDLField(field);
		String jsonKey = ndlField.getJsonKey();
		List<NDLDataNode> nodes = currentTargetItem.getNodes(field);
		int c = 0;
		for(NDLDataNode node : nodes) {
			String value = NDLDataUtils.getNDLFieldValue(node.getTextContent(), jsonKey, escapeHTMLFlag, true);
			if (map.containsKey(value)) {
				// match found
				String v = map.get(value);
				if(NDLDataUtils.deleteField(v)) {
					// field delete
					node.remove();
				} else {
					if(StringUtils.isBlank(jsonKey)) {
						node.setTextContent(v); // update new value
					} else {
						node.setTextContent(NDLDataUtils.getJson(jsonKey, v));
					}
					c++;
				}
			}
		}
		return c;
	}
	
	/**
	 * Transforms and normalize selected field by a given exact mapping
	 * @param field selected field
	 * @param map map to use transform field value
	 * @param multipleValueSeparator this character describes multiple value separator
	 * @return returns affected fields
	 */
	public int transformAndNormalizeFieldByExactMatch(String field, Map<String, String> map,
			Character multipleValueSeparator) {
		NDLField ndlField = new NDLField(field);
		String jsonKey = ndlField.getJsonKey();
		List<NDLDataNode> nodes = currentTargetItem.getNodes(field);
		int c = 0;
		for(NDLDataNode node : nodes) {
			String value = NDLDataUtils.getNDLFieldValue(node.getTextContent(), jsonKey, escapeHTMLFlag, true);
			if (map.containsKey(value)) {
				node.remove(); // delete
				// match found
				String v = map.get(value);
				if(!NDLDataUtils.deleteField(v)) {
					add(field, v, multipleValueSeparator);
					c++;
				}
			}
		}
		return c;
	}
	
	/**
	 * Transforms and normalize selected field by a given exact mapping
	 * @param field selected field
	 * @param map map to use transform field value
	 * @return returns affected fields
	 */
	public int transformAndNormalizeFieldByExactMatch(String field, Map<String, String> map) {
		return transformAndNormalizeFieldByExactMatch(field, map, multipleValueSeparator);
	}
	
	/**
	 * <pre>Transforms selected field by a given exact mapping.
	 * The mapping is given by {@link #addMappingResource(java.io.File, String, String)}</pre>
	 * This mapping resource file has two columns, one is primary key and another one is value.
	 * @param field selected field
	 * @param keyName key name is the logical name of mapping resource
	 * {@link #addMappingResource(java.io.File, String, String)}
	 * @param multivalueSeparator if target value contains multiple values and this parameter says separator
	 * @return returns affected fields
	 */
	public int transformFieldByExactMatch(String field, String keyName, Character multivalueSeparator) {
		NDLField ndlField = new NDLField(field);
		String jsonKey = ndlField.getJsonKey();
		List<NDLDataNode> nodes = currentTargetItem.getNodes(field);
		int c = 0;
		for(NDLDataNode node : nodes) {
			String value = NDLDataUtils.getNDLFieldValue(node.getTextContent(), jsonKey, escapeHTMLFlag, true);
			String key = ConfigurationData.escapeDot(keyName) + "." + ConfigurationData.escapeDot(value);
			if(containsMappingKey(key)) {
				// match found to so transform it
				String v = getMappingKey(key);
				if(NDLDataUtils.deleteField(v)) {
					// field delete
					node.remove();
				} else {
					StringTokenizer tokens = new StringTokenizer(v, String.valueOf(multivalueSeparator));
					while(tokens.hasMoreTokens()) {
						if(StringUtils.isBlank(jsonKey)) {
							node.setTextContent(tokens.nextToken()); // update new value
						} else {
							node.setTextContent(NDLDataUtils.getJson(jsonKey, tokens.nextToken()));
						}
						c++;
					}
				}
			}
		}
		return c;
	}
	

	/**
	 * <pre>Transforms selected field by a given exact mapping.
	 * The mapping is given by {@link #addMappingResource(java.io.File, String, String)}</pre>
	 * This mapping resource file has two columns, one is primary key and another one is value.
	 * @param field selected field
	 * @param keyName key name is the logical name of mapping resource
	 * @return returns affected fields
	 */
	public int transformFieldByExactMatch(String field, String keyName) {
		return transformFieldByExactMatch(field, keyName, multipleValueSeparator);
	}
	
	/**
	 * <pre>Transforms selected field by a given exact mapping from another field</pre>
	 * <pre>If the map size huge than it's not recommended to use because it's internally use brute force.</pre>
	 * <pre>The algorithm is supposed to be efficient later on.</pre>
	 * @param field which field to transform
	 * @param from from which field map the transformation logic is decided 
	 * @param map given map
	 * @param multivalueSeparator if target value contains multiple values and this parameter says separator
	 * @param ignoreCase this flag determines matching ignores case or not
	 * @return returns affected fields
	 */
	public int transformFieldByExactMatch(String field, String from, Map<String, String> map,
			Character multivalueSeparator, boolean ignoreCase) {
		List<String> field2Values = currentTargetItem.getValue(from);
		int c = 0;
		for(String field2Value : field2Values) {
			Set<String> values = new HashSet<String>(2); // values to be added
			// each value
			for(String key : map.keySet()) {
				// each key of map
				boolean match = ignoreCase ? StringUtils.equalsIgnoreCase(field2Value, key)
						: StringUtils.equals(field2Value, key);
				if(match) {
					// exact match
					values.add(map.get(key));
				}
			}
			
			// add values
			if(multivalueSeparator == null) {
				add(field, values);
			} else {
				add(field, values, multivalueSeparator);
			}
			
			c += values.size();
		}
		return c;
	}
	
	/**
	 * <pre>Transforms selected field by a given exact mapping from another field</pre>
	 * <pre>If the map size huge than it's not recommended to use because it's internally use brute force.</pre>
	 * <pre>The algorithm is supposed to be efficient later on.</pre>
	 * @param field which field to transform
	 * @param from from which field map the transformation logic is decided 
	 * @param map given map
	 * @return returns affected fields
	 * @see #transformFieldByExactMatch(String, String, Map, Character, boolean)
	 */
	public int transformFieldByExactMatch(String field, String from, Map<String, String> map) {
		return transformFieldByExactMatch(field, from, map, null, false);
		
	}
	
	/**
	 * <pre>Transforms selected field by a given exact mapping from another field</pre>
	 * <pre>If the map size huge than it's not recommended to use because it's internally use brute force.</pre>
	 * <pre>The algorithm is supposed to be efficient later on.</pre>
	 * @param field which field to transform
	 * @param from from which field map the transformation logic is decided 
	 * @param map given map
	 * @param multivalueSeparator if target value contains multiple values and this parameter says separator
	 * @return returns affected fields
	 * @see #transformFieldByExactMatch(String, String, Map, Character, boolean)
	 */
	public int transformFieldByExactMatch(String field, String from, Map<String, String> map,
			Character multivalueSeparator) {
		return transformFieldByExactMatch(field, from, map, multivalueSeparator, false);
	}
	
	/**
	 * <pre>Transforms selected field by a given exact mapping from another field</pre>
	 * <pre>If the map size huge than it's not recommended to use because it's internally use brute force.</pre>
	 * <pre>The algorithm is supposed to be efficient later on.</pre>
	 * @param field which field to transform
	 * @param from from which field map the transformation logic is decided 
	 * @param map given map
	 * @param ignoreCase this flag determines matching ignores case or not
	 * @return returns affected fields
	 * @see #transformFieldByExactMatch(String, String, Map, Character, boolean)
	 */
	public int transformFieldByExactMatch(String field, String from, Map<String, String> map,
			boolean ignoreCase) {
		return transformFieldByExactMatch(field, from, map, null, ignoreCase);
	}
	
	/**
	 * <pre>Transforms selected field by a given partial mapping from another field</pre>
	 * <pre>If the map size huge than it's not recommended to use because it's internally use brute force.</pre>
	 * <pre>The algorithm is supposed to be efficient later on.</pre>
	 * @param field which field to transform
	 * @param from from which field map the transformation logic is decided 
	 * @param map given map
	 * @param multivalueSeparator if target value contains multiple values and this parameter says separator
	 * @param ignoreCase this flag determines matching ignores case or not
	 * @return returns affected fields
	 */
	public int transformFieldByPartialMatch(String field, String from, Map<String, String> map,
			Character multivalueSeparator, boolean ignoreCase) {
		List<String> field2Values = currentTargetItem.getValue(from);
		int c = 0;
		for(String field2Value : field2Values) {
			Set<String> values = new HashSet<String>(2); // values to be added
			// each value
			for(String key : map.keySet()) {
				// each key of map
				boolean match = ignoreCase ? StringUtils.containsIgnoreCase(field2Value, key)
						: StringUtils.contains(field2Value, key);
				if(match) {
					// partial match
					values.add(map.get(key));
				}
			}
			
			// add values
			if(multivalueSeparator == null) {
				add(field, values);
			} else {
				add(field, values, multivalueSeparator);
			}
			
			c += values.size();
		}
		return c;
	}
	
	/**
	 * <pre>Transforms selected field by a given partial map from another field</pre>
	 * <pre>If the map size huge than it's not recommended to use because it's internally use brute force.</pre>
	 * <pre>The algorithm is supposed to be efficient later on.</pre>
	 * @param field which field to transform
	 * @param from from which field map the transformation logic is decided 
	 * @param map given map
	 * @return returns affected fields
	 * @see #transformFieldByPartialMatch(String, String, Map, Character, boolean)
	 */
	public int transformFieldByPartialMatch(String field, String from, Map<String, String> map) {
		return transformFieldByPartialMatch(field, from, map, null, false);
	}
	
	/**
	 * <pre>Transforms selected field by a given partial map from another field</pre>
	 * <pre>If the map size huge than it's not recommended to use because it's internally use brute force.</pre>
	 * <pre>The algorithm is supposed to be efficient later on.</pre>
	 * @param field which field to transform
	 * @param from from which field map the transformation logic is decided 
	 * @param map given map
	 * @param multivalueSeparator if target value contains multiple values and this parameter says separator
	 * @return returns affected fields
	 * @see #transformFieldByPartialMatch(String, String, Map, Character, boolean)
	 */
	public int transformFieldByPartialMatch(String field, String from, Map<String, String> map,
			Character multivalueSeparator) {
		return transformFieldByPartialMatch(field, from, map, multivalueSeparator, false);
	}
	
	/**
	 * <pre>Transforms selected field by a given partial map from another field</pre>
	 * <pre>If the map size huge than it's not recommended to use because it's internally use brute force.</pre>
	 * <pre>The algorithm is supposed to be efficient later on.</pre>
	 * @param field which field to transform
	 * @param from from which field map the transformation logic is decided 
	 * @param map given map
	 * @param ignoreCase this flag determines matching ignores case or not
	 * @return returns affected fields
	 * @see #transformFieldByPartialMatch(String, String, Map, Character, boolean)
	 */
	public int transformFieldByPartialMatch(String field, String from, Map<String, String> map,
			boolean ignoreCase) {
		return transformFieldByPartialMatch(field, from, map, null, ignoreCase);
	}
	
	/**
	 * <pre>Transforms selected field by a given partial word map from another field</pre>
	 * <pre>If the map size huge than it's not recommended to use because it's internally use brute force.</pre>
	 * <pre>The algorithm is supposed to be efficient later on.</pre>
	 * @param field field which field to transform
	 * @param from from which field map the transformation logic is decided 
	 * @param map given word map
	 * @param wordTokenzierRegex word splitter pattern(regular expression), default is space
	 * @param multivalueSeparator if target value contains multiple values and this parameter says separator
	 * @param ignoreCase this flag determines matching ignores case or not
	 * @return returns number of affected fields
	 */
	public int transformFieldByWordMatch(String field, String from, Map<String, String> map, String wordTokenzierRegex,
			Character multivalueSeparator, boolean ignoreCase) {
		List<String> field2Values = currentTargetItem.getValue(from);
		int c = 0;
		for(String field2Value : field2Values) {
			Set<String> values = new HashSet<String>(2); // values to be added
			// each value
			for(String key : map.keySet()) {
				// each key of map
				boolean match = false;
				String tokens[] = field2Value.split(wordTokenzierRegex);
				for(String token : tokens) {
					boolean f = ignoreCase ? StringUtils.equalsIgnoreCase(token, key)
							: StringUtils.equals(token, key);
					if(f) {
						match = true;
						break;
					}
				}
				if(match) {
					// partial match
					values.add(map.get(key));
				}
				
				// add values
				if(multivalueSeparator == null) {
					add(field, values);
				} else {
					add(field, values, multivalueSeparator);
				}
				
				c += values.size();
			}
		}
		return c;
	}
	
	/**
	 * <pre>Transforms selected field by a given partial word map from another field</pre>
	 * <pre>If the map size huge than it's not recommended to use because it's internally use brute force.</pre>
	 * <pre>The algorithm is supposed to be efficient later on.</pre>
	 * @param field field which field to transform
	 * @param from from which field map the transformation logic is decided 
	 * @param map given word map
	 * @return returns number of affected fields
	 */
	public int transformFieldByWordMatch(String field, String from, Map<String, String> map) {
		return transformFieldByWordMatch(field, from, map, " +", null, false);
	}
	
	/**
	 * <pre>Transforms selected field by a given partial word map from another field</pre>
	 * <pre>If the map size huge than it's not recommended to use because it's internally use brute force.</pre>
	 * <pre>The algorithm is supposed to be efficient later on.</pre>
	 * @param field field which field to transform
	 * @param from from which field map the transformation logic is decided 
	 * @param map given word map
	 * @param multivalueSeparator if target value contains multiple values and this parameter says separator
	 * @param ignoreCase this flag determines matching ignores case or not
	 * @return returns number of affected fields
	 */
	public int transformFieldByWordMatch(String field, String from, Map<String, String> map,
			Character multivalueSeparator, boolean ignoreCase) {
		return transformFieldByWordMatch(field, from, map, " +", multivalueSeparator, ignoreCase);
	}
	
	/**
	 * <pre>Transforms selected field by a given partial map with regular expression from another field</pre>
	 * <pre>If the map size huge than it's not recommended to use because it's internally use brute force.</pre>
	 * <pre>The algorithm is supposed to be efficient later on.</pre>
	 * @param field which field to transform
	 * @param from from which field map the transformation logic is decided
	 * @param map given map with regular expression
	 * @return returns affected fields
	 */
	public int transformFieldByRegexMatch(String field, String from, Map<String, String> map) {
		return transformFieldByRegexMatch(field, from, map, null);
	}
	
	/**
	 * <pre>Transforms selected field by a given partial map with regular expression from another field</pre>
	 * <pre>If the map size huge than it's not recommended to use because it's internally use brute force.</pre>
	 * <pre>The algorithm is supposed to be efficient later on.</pre>
	 * @param field which field to transform
	 * @param from from which field map the transformation logic is decided
	 * @param map given map with regular expression
	 * @param multivalueSeparator if target value contains multiple values and this parameter says separator
	 * @return returns affected fields
	 */
	public int transformFieldByRegexMatch(String field, String from, Map<String, String> map,
			Character multivalueSeparator) {
		List<String> field2Values = currentTargetItem.getValue(from);
		int c = 0;
		for(String field2Value : field2Values) {
			Set<String> values = new HashSet<String>(2); // values to be added
			// each value
			for(String key : map.keySet()) {
				// each key of map
				boolean match = field2Value.matches(key);
				if(match) {
					// partial match
					values.add(map.get(key));
				}
			}
			
			// add values
			if(multivalueSeparator == null) {
				add(field, values);
			} else {
				add(field, values, multivalueSeparator);
			}
			
			c += values.size();
		}
		return c;
	}
	
	/**
	 * Fills field values by handle ID (suffix id, value after slash)
	 * Note: Values get normalized if required
	 * @param keyName key name is the logical name of mapping resource
	 * @param multipleValueSeparator this separator defines multiple values
	 * @param pairs defines the mapping '&lt;column_name,ndl_field_name&gt;'
	 * @return returns number of fields affected
	 * @see #transformFieldsById(String, String...)
	 */
	public int transformFieldsById(String keyName, char multipleValueSeparator,
			String... pairs) {
		return transformFieldsById(keyName, true, multipleValueSeparator, pairs);
	}
	
	/**
	 * Fills field values by given field value
	 * Note: Values get normalized if required
	 * @param keyName key name is the logical name of mapping resource
	 * @param field field name for mapping
	 * @param multipleValueSeparator this separator defines multiple values
	 * @param pairs defines the mapping '&lt;column_name,ndl_field_name&gt;'
	 * @return returns number of fields affected
	 * @see #transformFieldsById(String, String...)
	 */
	public int transformFieldsByFieldValue(String keyName, String field, char multipleValueSeparator,
			String... pairs) {
		List<String> fvalues = currentTargetItem.getValue(field);
		int c = 0;
		for(String fvalue : fvalues) {
			c  += transformFields(keyName, fvalue, true, multipleValueSeparator, pairs);
		}
		return c;
	}
	
	/**
	 * Fills field values by given field value
	 * Note: Values get normalized if required
	 * @param keyName key name is the logical name of mapping resource
	 * @param field field name for mapping
	 * @param preserveOldValue flag to indicate preserving old value or not
	 * @param multipleValueSeparator this separator defines multiple values
	 * @param pairs defines the mapping '&lt;column_name,ndl_field_name&gt;'
	 * @return returns number of fields affected
	 * @see #transformFieldsById(String, String...)
	 */
	public int transformFieldsByFieldValue(String keyName, String field, boolean preserveOldValue,
			char multipleValueSeparator, String... pairs) {
		List<String> fvalues = currentTargetItem.getValue(field);
		int c = 0;
		for(String fvalue : fvalues) {
			c  += transformFields(keyName, fvalue, preserveOldValue, multipleValueSeparator, pairs);
		}
		return c;
	}
	
	/**
	 * Fills field values by given field value
	 * Note: Values get normalized if required
	 * @param keyName key name is the logical name of mapping resource
	 * @param field field name for mapping
	 * @param preserveOldValue flag to indicate preserving old value or not
	 * @param pairs defines the mapping '&lt;column_name,ndl_field_name&gt;'
	 * @return returns number of fields affected
	 * @see #transformFieldsById(String, String...)
	 */
	public int transformFieldsByFieldValue(String keyName, String field, boolean preserveOldValue, String... pairs) {
		List<String> fvalues = currentTargetItem.getValue(field);
		int c = 0;
		for(String fvalue : fvalues) {
			c  += transformFields(keyName, fvalue, preserveOldValue, multipleValueSeparator, pairs);
		}
		return c;
	}
	
	/**
	 * Fills field values by given value
	 * Note: Values get normalized if required
	 * @param keyName key name is the logical name of mapping resource
	 * @param value given value (by which particular row is identified)
	 * @param preserveOldValue this flag is set then don't delete old values
	 * @param multipleValueSeparator this separator defines multiple values
	 * @param pairs defines the mapping '&lt;column_name,ndl_field_name&gt;'
	 * @return returns number of fields affected
	 */
	int transformFields(String keyName, String value, boolean preserveOldValue, char multipleValueSeparator,
			String... pairs) {
		// for internal usage
		int c = 0;
		String key = ConfigurationData.escapeDot(keyName) + "."
				+ ConfigurationData.escapeDot(value);
		if(!containsMappingKey(key)) {
			// mapping does not exist
			return c;
		}
		for(String pair : pairs) {
			String[] kv = NDLDataUtils.getKeyValue(pair);
			String v = getMappingKey(key + "." + kv[0]);
			if(StringUtils.isNotBlank(v)) {
				if(!preserveOldValue) {
					// if old values to be deleted then delete the field otherwise preserve
					try {
						delete(kv[1]);
					} catch(IOException ex) {
						// error
						System.err.println("[WARN] " + kv[1] + " could not be deleted, " + ex.getMessage());
					}
				}
				// value available
				add(kv[1], v, multipleValueSeparator);
				c++;
			}
		}
		return c;
	}
	
	/**
	 * Fills field values by handle ID (suffix id, value after slash)
	 * Note: Values get normalized if required
	 * @param keyName key name is the logical name of mapping resource
	 * @param preserveOldValue this flag is set then don't delete old values
	 * @param multipleValueSeparator this separator defines multiple values
	 * @param pairs defines the mapping '&lt;column_name,ndl_field_name&gt;'
	 * @return returns number of fields affected
	 * @see #transformFieldsById(String, String...)
	 */
	public int transformFieldsById(String keyName, boolean preserveOldValue, char multipleValueSeparator,
			String... pairs) {
		return transformFields(keyName, NDLDataUtils.getHandleSuffixID(currentTargetItem.getId()), preserveOldValue,
				multipleValueSeparator, pairs);
	}
	
	/**
	 * Fills field values by handle ID (suffix id, value after slash)
	 * Note: Values get normalized if required and 'multipleValueSeparator' is default '|'
	 * @param keyName key name is the logical name of mapping resource
	 * @param pairs defines the mapping '&lt;column_name,ndl_field_name&gt;'
	 * @return returns number of fields affected 
	 */
	public int transformFieldsById(String keyName, String ... pairs) {
		return transformFieldsById(keyName, multipleValueSeparator, pairs);
	}
	
	/**
	 * Fills field values by handle ID (suffix id, value after slash)
	 * Note: Values get normalized if required and 'multipleValueSeparator' is default '|'
	 * @param keyName key name is the logical name of mapping resource
	 * @param preserveOldValue this flag is set then don't delete old values
	 * @param pairs defines the mapping '&lt;column_name,ndl_field_name&gt;'
	 * @return returns number of fields affected 
	 */
	public int transformFieldsById(String keyName, boolean preserveOldValue, String ... pairs) {
		return transformFieldsById(keyName, preserveOldValue, multipleValueSeparator, pairs);
	}
	
	/**
	 * Checks given field unassigned
	 * @param field field to check
	 * @return returns true if unassigned, otherwise false
	 */
	public boolean isUnassigned(String field) {
		return isAllUnassigned(field);
	}

	/**
	 * Checks all fields unassigned
	 * @param fields fields to check
	 * @return returns true if all fields unassigned, otherwise false
	 */
	public boolean isAllUnassigned(String ... fields) {
		for(String field : fields) {
			if(currentTargetItem.exists(field)) {
				// found any assigned
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Checks whether field is single valued
	 * @param field field name
	 * @return returns true if single valued or NULL otherwise false
	 */
	public boolean isSingleValued(String field) {
		int s = getValue(field).size();
		return s <= 1;
	}
	
	/**
	 * Checks whether field is multiple valued
	 * @param field field name
	 * @return returns true if multiple valued otherwise false
	 */
	public boolean isMultiValued(String field) {
		int s = getValue(field).size();
		return s > 1;
	}
	
	/**
	 * Checks any of the fields unassigned
	 * @param fields fields to check
	 * @return returns true if any of the fields unassigned, otherwise false
	 */
	public boolean isAnyUnassigned(String ... fields) {
		for(String field : fields) {
			if(!currentTargetItem.exists(field)) {
				// found any unassigned
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Moves first value from source to destination if exists
	 * @param source source field name
	 * @param destination destination field name
	 * @param normalizeFlag this flag forces field to go through normalization
	 * @return returns true if moved successfully otherwise false
	 */
	public boolean moveFirst(String source, String destination, boolean normalizeFlag) {
		List<NDLDataNode> nodes = currentTargetItem.getNodes(source);
		if(nodes.isEmpty()) {
			// no value to move
			return false;
		}
		for(NDLDataNode node : nodes) {
			// always take first node
			if(normalizeFlag) {
				add(destination, node.getTextContent());
			} else {
				currentTargetItem.addIfNotContains(destination, node.getTextContent());
			}
			node.remove();
			break;
		}
		return true;
	}
	
	/**
	 * Moves first value from source to destination if exists
	 * Note: This method just moves the value without normalization 
	 * @param source source field name
	 * @param destination destination field name
	 * @return returns true if moved successfully otherwise false
	 */
	public boolean moveFirst(String source, String destination) {
		return moveFirst(source, destination, false);
	}
	
	/**
	 * Moves value from source to destination if given index exists
	 * Note: This method just moves the value without normalization 
	 * @param source source field name
	 * @param destination destination field name
	 * @param indices which index to be moved (index 0 based)
	 * @return returns count of moved items
	 */
	public int move(String source, String destination, Integer ... indices) {
		return move(source, destination, false, indices);
	}
	
	/**
	 * Moves value from source to destination if given index exists
	 * Note: This method just moves the value without normalization 
	 * @param source source field name
	 * @param destination destination field name
	 * @param normalizeFlag this flag forces field to go through normalization
	 * @param indices which index to be moved (index 0 based)
	 * @return returns count of moved items
	 */
	public int move(String source, String destination, boolean normalizeFlag, Integer ... indices) {
		List<NDLDataNode> nodes = currentTargetItem.getNodes(source);
		if(nodes.isEmpty()) {
			// no value to move
			return 0;
		}
		int idx = 0;
		int c = 0;
		for(NDLDataNode node : nodes) {
			for(int index : indices) {
				if(idx == index) {
					// index is valid
					if(normalizeFlag) {
						add(destination, node.getTextContent());
					} else {
						currentTargetItem.addIfNotContains(destination, node.getTextContent());
					}
					node.remove();
					c++;
				}
			}
			idx++;
		}
		return c;
	}
	
	/**
	 * Merges field values by given a separator
	 * @param separator given separator
	 * @param fields field names, field values can be multiple
	 */
	public void merge(Character separator, String ... fields) {
		List<String> values = new LinkedList<String>();
		int l = fields.length;
		if(l < 2) {
			throw new IllegalArgumentException("At least two fields expected");
		}
		String field = fields[0];
		String firstValue = currentTargetItem.getSingleValue(field);
		if(StringUtils.isNotBlank(firstValue)) {
			values.add(firstValue);
		}
		for(int i = 1; i < l; i++) {
			values.addAll(currentTargetItem.getValue(fields[i]));
			// delete field
			currentTargetItem.delete(fields[i]);
		}
		currentTargetItem.updateSingleValue(field, NDLDataUtils.join(values, separator));
	}
	
	/**
	 * Merges field values by space separator
	 * @param fields field names, field values can be multiple
	 */
	public void merge(String ... fields) {
		merge(' ', fields);
	}
	
	/**
	 * Retains first node and deletes rest nodes of given fields
	 * @param fields given set of fields
	 */
	public void retainFirst(String ... fields) {
		for(String field : fields) {
			currentTargetItem.retainByIndex(field, 0);
		}
	}
	
	/**
	 * Removes HTML tags from given fields
	 * @param fields given field names
	 */
	public void removeHTMLTags(String ... fields) {
		for(String field : fields) {
			List<NDLDataNode> nodes = currentTargetItem.getNodes(field);
			for(NDLDataNode node : nodes) {
				node.setTextContent(NDLDataUtils.removeHTMLTags(node.getTextContent()));
			}
		}
	}
	
	/**
	 * Removes HTML tags and handle simple SUP/SUB from given fields
	 * <pre>Note: If complex situation occurs then it fails to do conversion and returns original data</pre>
	 * @param fields given field names
	 * @see NDLDataUtils#removeHTMLTagsAndFixLatex(String)
	 */
	public void removeHTMLTagsAndFixLatex(String ... fields) {
		for(String field : fields) {
			List<NDLDataNode> nodes = currentTargetItem.getNodes(field);
			for(NDLDataNode node : nodes) {
				node.setTextContent(NDLDataUtils.removeHTMLTagsAndFixLatex(node.getTextContent()));
			}
		}
	}
	
	/**
	 * Switching context to custom context
	 * @param contextName custom context
	 * @throws NDLContextSwitchLoadException throws error in case of loading error
	 * @see NDLContextSwitch#switchContext(NDLContext, NDLDataNormalizationPool)
	 */
	public void switchContext(NDLContext context) throws NDLContextSwitchLoadException {
		// switch context
		NDLContextSwitch.switchContext(context, normalizers);
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