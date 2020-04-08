package org.iitkgp.ndl.data.generation;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.RowData;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.compress.CompressedFileMode;
import org.iitkgp.ndl.data.container.DataContainerNULLConfiguration;
import org.iitkgp.ndl.data.correction.AbstractNDLDataCorrectionContainer;
import org.iitkgp.ndl.data.exception.PrimaryIndexNotFoundException;
import org.iitkgp.ndl.data.generation.exception.DuplicatePrimaryKeyValueException;
import org.iitkgp.ndl.data.generation.exception.FullHandleIDMissingException;
import org.iitkgp.ndl.data.iterator.CSVDataIterator;
import org.iitkgp.ndl.data.iterator.DataSourceCSVConfiguration;
import org.iitkgp.ndl.data.writer.NDLDataItemWriter;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.xml.sax.SAXException;

/**
 * <pre>This class is responsible for generation of SIP from raw sources CSV.</pre>
 * <pre>This class takes care of generating handles, handle prefix
 * see {@link #NDLCSVToSIPGeneration(String, String, String, String)}
 * and {@link #NDLCSVToSIPGeneration(String, String, String, String, char)}</pre>
 * <pre>To override the behavior see {@link #setHandleIDPrefix(String)}</pre>
 * <pre>Default handle ID counter is <b>1</b> to override see {@link #setStartHandleID(long)},
 * if {@link #setPrimaryIDColumn(String)} is used then primary column is used in generating handle ID.</pre>
 * <pre>Use full handle ID information, {@link #turnOnFullHandleIDColumnFlag(String)} and {@link #getFullHandleID(String)}</pre>
 * <pre>Note: System takes care of duplicate primary key column value once for this run.</pre> 
 * <ul>
 * <li>To add custom logic see {@link #generateTargetItem(RowData, RowData)}</li>
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
 * </ul>
 * <pre><b>When override a method then don't forget to call super method.</b></pre>
 * <pre>See {@link CSVDataIterator} {@link AbstractCSVRawSourceDataGeneration} {@link AbstractNDLDataCorrectionContainer}
 * {@link #addGlobalData(String, String)}</pre>
 * <pre>See {@link #turnOnNormalizationFlag()} for data normalization, default behavior is blind 
 * copy (without modifying/normalizing the value)</pre>
 * <pre><b>Example: </b> {@code
 * // sample CSV to SIP conversion
 * public class CSVToSIPGenerationTest extends CSVToSIPGeneration {
 * 	// constructor
 * 	public CSVToSIPGenerationTest(String input, String logLocation, String outputLocation, String name,
 * 	char valueSeparator) {
 * 		super(input, logLocation, outputLocation, name, valueSeparator);
 * 	}
 * 	// generation logic
 * 	protected boolean generateTargetItem(RowData csv, RowData target) throws Exception {
 * 		String id = csv.getSingleData("id"); // ID
 * 		if(containsMappingKey("delete." + id)) {
 * 			// to be deleted
 * 			return false;
 * 		}
 * 		add("dc.xxx.yyy", "some value"); // add some value to field dc.xxx.yyy
 * 		copy("csv column", "dc.xxx.yy2"); // copy csv column to dc.xxx.yy2
 * 		// etc.
 * 		
 * 		// success generation
 * 		return true;
 * 	}
 * 	// testing
 * 	public static void main(String[] args) throws Exception {
 * 		String input = "input source"; // flat AIP location or compressed AIP location
 * 		String logLocation = "log location"; // log location if any
 * 		String outputLocation = "output location where to write the data";
 * 		String name = "logical name of the source";
 * 		// multiple value separator is pipe
 * 		
 * 		CSVToSIPGenerationTest p = new CSVToSIPGenerationTest(input, logLocation, outputLocation, name, '|');
 * 		String deleteFile = "delete file"; // which has single column contains handles to delete
 * 		p.addMappingResource(deleteFile, "delete"); // this logical name used to access the handle
 * 		p.genrateData(); // generates data
 * 	}
 * }
 * }</pre>
 * 
 * @author Debasis
 */
public abstract class NDLCSVToSIPGeneration
		extends AbstractCSVRawSourceDataGeneration<NDLDataItemWriter<SIPDataItem>, SIPDataItem> {
	
	static final String DEFAULT_HANDLE_PREFIX = "123456789_";
	
	// handle details
	long startHandleID = 1;
	String handleIDPrefix;
	boolean compressed = true; // default compression is ON
	long itemCounter = 0; // item folder counter
	Set<String> fullHandleIDTracker = new HashSet<String>(2); // duplicate full handle ID tracker
	
	/**
	 * Constructor, default data source configuration is "comma separated values"
	 * @param input source data (either compressed version or folder version)
	 * @param logLocation logging location
	 * @param outputLocation output location
	 * @param name <pre>logical name which differentiates log file(s), output file(s) from other source</pre>
	 */
	public NDLCSVToSIPGeneration(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, name, outputLocation);
		handleIDPrefix = getDefaultHandlePrefix(name); // handle prefix
	}
	
	/**
	 * Constructor, default data source configuration is "comma separated values"
	 * @param input source data (either compressed version or folder version)
	 * @param logLocation logging location
	 * @param outputLocation output location
	 * @param name <pre>logical name which differentiates log file(s), output file(s) from other source</pre>
	 * @param validationFlag validation flag to indicate whether validation takes place or not
	 */
	public NDLCSVToSIPGeneration(String input, String logLocation, String outputLocation, String name,
			boolean validationFlag) {
		super(input, logLocation, name, outputLocation, false, validationFlag);
		handleIDPrefix = getDefaultHandlePrefix(name); // handle prefix
	}
	
	/**
	 * Constructor
	 * @param input source data (either compressed version or folder version)
	 * @param logLocation logging location
	 * @param name <pre>logical name which differentiates log file(s), output file(s) from other source</pre>
	 * @param outputLocation output location
	 * @param valueSeparator CSV value separated, if not set then default is comma. 
	 */
	public NDLCSVToSIPGeneration(String input, String logLocation, String outputLocation, String name,
			char valueSeparator) {
		super(input, logLocation, name, outputLocation, valueSeparator);
		handleIDPrefix = getDefaultHandlePrefix(name); // handle prefix
	}
	
	/**
	 * Constructor
	 * @param input source data (either compressed version or folder version)
	 * @param logLocation logging location
	 * @param name <pre>logical name which differentiates log file(s), output file(s) from other source</pre>
	 * @param outputLocation output location
	 * @param valueSeparator CSV value separated, if not set then default is comma.
	 * @param validationFlag validation flag to indicate whether validation takes place or not 
	 */
	public NDLCSVToSIPGeneration(String input, String logLocation, String outputLocation, String name,
			char valueSeparator, boolean validationFlag) {
		super(input, logLocation, name, outputLocation, valueSeparator, false, validationFlag);
		handleIDPrefix = getDefaultHandlePrefix(name); // handle prefix
	}
	
	/**
	 * Gets full handle ID
	 * @param handle current processing item's full-handle-ID
	 * @return returns full handle ID value
	 */
	public String getFullHandleID(String handle) {
		return handle;
	}
	
	/**
	 * Sets output compressed flag
	 * @param compressed compression flag
	 */
	public void setCompressed(boolean compressed) {
		this.compressed = compressed;
	}
	
	/**
	 * Turns of compression flag
	 */
	public void turnOffCompressionFlag() {
		this.compressed = false;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isCompressed() {
		return compressed;
	}
	
	// gets default handle prefix
	String getDefaultHandlePrefix(String name) {
		return DEFAULT_HANDLE_PREFIX + name; // handle prefix
	}
	
	/**
	 * Sets starts handle ID, if {@link #setPrimaryIDColumn(String)}is used then primary column 
	 * is used in generating handle ID. 
	 * @param startHandleID start handle ID
	 * @see #setPrimaryIDColumn(String)
	 */
	public void setStartHandleID(long startHandleID) {
		this.startHandleID = startHandleID;
	}
	
	/**
	 * Sets handle ID prefix
	 * @param handleIDPrefix handle ID prefix
	 */
	public void setHandleIDPrefix(String handleIDPrefix) {
		this.handleIDPrefix = handleIDPrefix;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void init(DataContainerNULLConfiguration<DataSourceCSVConfiguration> configuration) throws IOException {
		// super call
		super.init(configuration);
		// custom code
		writer = new NDLDataItemWriter<SIPDataItem>(outputLocation, getFileName());
		// handle compression mode for writing
		if(isCompressed()) {
			// compression mode is ON
			writer.setCompressOn(getFileName("Corrected"), CompressedFileMode.TARGZ);
		}
		writer.init(); // initialization writer
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public SIPDataItem createTargetItem(RowData item) throws Exception {
		SIPDataItem sip = createSIPItem(item);
		// validation
		NDLDataUtils.validateNDLData(sip, validator);
		return sip; // returns target item
	}
	
	// creates SIP item from row-data
	SIPDataItem createSIPItem(RowData item) throws IOException, SAXException {
		String handle = getNextHandle(item);
		
		SIPDataItem sip = NDLDataUtils.createBlankSIP(getFileName(true) + "/" + ++itemCounter, handle);
		Set<Entry<String, Collection<String>>> fields = item.entrySet();
		
		for(Entry<String, Collection<String>> field : fields) {
			// iterate all fields and add values, avoid duplicate SET used
			Set<String> values = new HashSet<String>(2);
			values.addAll(field.getValue());
			sip.add(field.getKey(), values);
		}
		return sip;
	}
	
	// gets next handle ID
	String getNextHandle(RowData item) {
		if(fullHandleIDColumnFlag) {
			// reading full handle ID
			if(StringUtils.isBlank(currentFullHandleID)) {
				// full handle ID missing
				throw new FullHandleIDMissingException("Full handle id missing at: (" + item.getSourceName()
						+ ", Row No: " + item.getRowIndex() + ")");
			}
			String fullHandleID = getFullHandleID(currentFullHandleID);
			if(StringUtils.isBlank(fullHandleID)) {
				// modified full handle ID is blank
				throw new FullHandleIDMissingException("Modified full handle ID is blank, check #getFullHandleID method");
			}
			// duplicacy check
			if(fullHandleIDTracker.contains(fullHandleID)) {
				// duplicate
				throw new DuplicatePrimaryKeyValueException("Handle ID is duplicate: " + fullHandleID);
			}
			fullHandleIDTracker.add(fullHandleID);
			return fullHandleID;
		} else {
			// default strategy
			String nextID = null;
			if(primaryColumnAvailable) {
				nextID = currentPrimaryColumnValue;
			} else {
				nextID = String.valueOf(++startHandleID);
			}
			if(StringUtils.isBlank(nextID)) {
				// erroneous condition
				throw new PrimaryIndexNotFoundException("Primary value not found.");
			}
			return handleIDPrefix + "/" + nextID;
		}
	}
	
	/**
	 * This method by default try to locate asset by primary column otherwise custom logic to be provided
	 */
	@Override
	protected String getAssetID(RowData item) {
		if(primaryColumnAvailable) {
			return getPrimaryValue(item);
		} else {
			// asset id not found
			return null;
		}
	}
}