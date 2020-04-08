package org.iitkgp.ndl.data.generation;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.NDLDataItem;
import org.iitkgp.ndl.data.NDLDataPair;
import org.iitkgp.ndl.data.RowData;
import org.iitkgp.ndl.data.container.AbstractNDLDataContainer;
import org.iitkgp.ndl.data.container.DataContainerNULLConfiguration;
import org.iitkgp.ndl.data.exception.PrimaryIndexNotFoundException;
import org.iitkgp.ndl.data.generation.exception.DuplicatePrimaryKeyValueException;
import org.iitkgp.ndl.data.iterator.CSVDataIterator;
import org.iitkgp.ndl.data.iterator.DataSourceCSVConfiguration;
import org.iitkgp.ndl.data.writer.DataWriter;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * <pre>This class is responsible for generation of data (SIP, CSV etc.) from raw sources CSV.
 * CSV file first row is header part.</pre>
 * Default multiple value separator pipe.
 * @param <W> Data writer for post process of each item
 * @param <O> Target data type
 * @see CSVDataIterator
 * @see AbstractNDLDataContainer
 * @author Debasis
 */
public abstract class AbstractCSVRawSourceDataGeneration<W extends DataWriter<O>, O extends NDLDataItem>
		extends AbstractRawSourceDataGeneration<RowData, CSVDataIterator, W, O, DataSourceCSVConfiguration> {
	
	// multiple value separator in a single cell
	char multipleValueSeparator = '|'; // see `fullDataCopyFlag`
	DataSourceCSVConfiguration dataSourceConfig = new DataSourceCSVConfiguration(NDLDataUtils.DEFAULT_CSV_SEPARATOR,
			NDLDataUtils.DEFAULT_CSV_QUOTE_CHARACTER);
	String primaryIDColumn; // primary ID column
	boolean primaryColumnAvailable = false;
	Set<String> primaryColumnTracker = null; // handle ID tracking for duplicate entry check
	// track current processing item
	RowData currentCSVRow = null;
	RowData currentTargetRow = null;
	String currentPrimaryColumnValue = null;
	// global properties (it will blindly add to every item)
	RowData globalFields = new RowData();
	int multilineLimit; // multi-line limit
	
	// full handle ID details
	boolean fullHandleIDColumnFlag = false;
	String fullHandleIDColumn;
	String currentFullHandleID;
	
	// full data copy flag
	boolean fullDataCopyFlag = false;
	
	/**
	 * Constructor, default data source configuration is "comma separated values"
	 * @param input source data (either compressed version or folder version)
	 * @param logLocation logging location
	 * @param outputLocation output location
	 * @param name <pre>logical name which differentiates log file(s), output file(s) from other source</pre>
	 * @param globalLoggingFlag global logging flag
	 * @param validationFlag validation flag to indicate whether validation takes place or not
	 */
	public AbstractCSVRawSourceDataGeneration(String input, String logLocation, String name, String outputLocation,
			boolean globalLoggingFlag, boolean validationFlag) {
		super(input, logLocation, name, validationFlag);
		dataReader = new CSVDataIterator(input);
		this.outputLocation = outputLocation;
		containerConfiguration = new DataContainerNULLConfiguration<DataSourceCSVConfiguration>(dataSourceConfig);
		if(!globalLoggingFlag) {
			// turn off
			turnOffGlobalLoggingFlag();
		}
	}
	
	/**
	 * Constructor, default data source configuration is "comma separated values"
	 * @param input source data (either compressed version or folder version)
	 * @param logLocation logging location
	 * @param outputLocation output location
	 * @param name <pre>logical name which differentiates log file(s), output file(s) from other source</pre>
	 * @param globalLoggingFlag global logging flag
	 */
	public AbstractCSVRawSourceDataGeneration(String input, String logLocation, String name, String outputLocation,
			boolean globalLoggingFlag) {
		this(input, logLocation, name, outputLocation, globalLoggingFlag, true);
	}
	
	/**
	 * Constructor, default data source configuration is "comma separated values"
	 * @param input source data (either compressed version or folder version)
	 * @param logLocation logging location
	 * @param outputLocation output location
	 * @param name <pre>logical name which differentiates log file(s), output file(s) from other source</pre>
	 */
	public AbstractCSVRawSourceDataGeneration(String input, String logLocation, String name, String outputLocation) {
		this(input, logLocation, name, outputLocation, false, true);
	}
	
	/**
	 * Constructor
	 * @param input source data (either compressed version or folder version)
	 * @param logLocation logging location
	 * @param name <pre>logical name which differentiates log file(s), output file(s) from other source</pre>
	 * @param outputLocation output location
	 * @param valueSeparator CSV value separated, if not set then default is comma. 
	 * @param globalLoggingFlag global logging flag
	 */
	public AbstractCSVRawSourceDataGeneration(String input, String logLocation, String name, String outputLocation,
			char valueSeparator, boolean globalLoggingFlag) {
		this(input, logLocation, name, outputLocation, valueSeparator, globalLoggingFlag, false);
	}
	
	/**
	 * Constructor
	 * @param input source data (either compressed version or folder version)
	 * @param logLocation logging location
	 * @param name <pre>logical name which differentiates log file(s), output file(s) from other source</pre>
	 * @param outputLocation output location
	 * @param valueSeparator CSV value separated, if not set then default is comma. 
	 * @param globalLoggingFlag global logging flag
	 * @param validationFlag validation flag to indicate whether validation takes place or not
	 */
	public AbstractCSVRawSourceDataGeneration(String input, String logLocation, String name, String outputLocation,
			char valueSeparator, boolean globalLoggingFlag, boolean validationFlag) {
		super(input, logLocation, name, validationFlag);
		dataReader = new CSVDataIterator(input);
		this.outputLocation = outputLocation;
		dataSourceConfig = new DataSourceCSVConfiguration(valueSeparator, NDLDataUtils.DEFAULT_CSV_QUOTE_CHARACTER);
		containerConfiguration = new DataContainerNULLConfiguration<DataSourceCSVConfiguration>(dataSourceConfig);
	}
	
	/**
	 * Constructor
	 * @param input source data (either compressed version or folder version)
	 * @param logLocation logging location
	 * @param name <pre>logical name which differentiates log file(s), output file(s) from other source</pre>
	 * @param outputLocation output location
	 * @param valueSeparator CSV value separated, if not set then default is comma.
	 */
	public AbstractCSVRawSourceDataGeneration(String input, String logLocation, String name, String outputLocation,
			char valueSeparator) {
		this(input, logLocation, name, outputLocation, valueSeparator, false);
	}
	
	/**
	 * This flag turns on reading full handle ID
	 * @param fullHandleIDColumn full handle ID column name
	 */
	public void turnOnFullHandleIDColumnFlag(String fullHandleIDColumn) {
		fullHandleIDColumnFlag = true;
		this.fullHandleIDColumn = fullHandleIDColumn;
	}
	
	/**
	 * This flag enables for all columns to copy full data without splitting data
	 */
	public void turnOnFullDataCopyFlag() {
		fullDataCopyFlag = true;
	}
	
	/**
	 * Sets multiple line limit
	 * @param multilineLimit multiple line limit
	 */
	public void setMultilineLimit(int multilineLimit) {
		dataSourceConfig.setMultilineLimit(multilineLimit);
	}
	
	/**
	 * Gets multiple line limit
	 * @return returns multiple line limit
	 */
	public int getMultilineLimit() {
		return dataSourceConfig.getMultilineLimit();
	}
	
	/**
	 * Adds global fields (these fields added to all items even to hierarchy)
	 * @param key field key
	 * @param value field value
	 */
	public void addGlobalData(String key, String value) {
		globalFields.addData(key, value);
	}
	
	/**
	 * Checks primary column is unique, if mentioned
	 * @throws Exception throws error if primary key is duplicate or blank
	 */
	@Override
	public void preProcessItem(RowData item) throws Exception {
		// super call
		super.preProcessItem(item);
		// check duplicate primary column value
		String primaryValue = getPrimaryValue(item);
		if(StringUtils.isNotBlank(primaryValue)) {
			// primary key value is blank, that means primary key column not mentioned
			// process only if available
			if(primaryColumnTracker.contains(primaryValue)) {
				throw new DuplicatePrimaryKeyValueException("Primary key is duplicate: " + primaryValue);
			} else {
				// add to set to track duplicate
				primaryColumnTracker.add(primaryValue);
			}
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean processRawSourceItem(RowData item) throws Exception {
		currentAssetID = getAssetID(item); // asset ID
		currentPrimaryColumnValue = getPrimaryValue(item); // primary column value
		if(fullHandleIDColumnFlag) {
			currentFullHandleID = item.getSingleData(fullHandleIDColumn);
		}
		currentCSVRow = item;
		currentTargetRow = new RowData();
		if(!generateTargetItem(currentCSVRow, currentTargetRow)) {
			// skipped
			return false;
		}
		// update with new one
		item.clear();
		item.addAllData(globalFields); // add global level data
		item.addAllData(currentTargetRow);
		return true; // successfully processed
	}
	
	/**
	 * Generates target item
	 * @param csv CSV row data item
	 * @param target target data item
	 * @return returns true if process successful otherwise false
	 * @throws Exception throws error in case of transformation error
	 */
	protected abstract boolean generateTargetItem(RowData csv, RowData target) throws Exception;
	
	// gets field data from csv row
	Collection<String> getData(String field) {
		if(field.contains(":")) {
			if(!currentCSVRow.headerExists(field)) {
				// field does not exist
				return null;
			}
			// JSON field
			String tokens[] = field.split(":");
			if(StringUtils.isBlank(tokens[1])) {
				// no JSON field mentioned
				return currentCSVRow.getData(field);
			}
			
			Collection<String> values = currentCSVRow.getData(field);
			Collection<String> modifiedValues = new LinkedList<String>();
			for(String value : values) {
				// suppress error
				Map<String, String> map = NDLDataUtils.mapFromJson(value, true);
				if(map.containsKey(tokens[1])) {
					modifiedValues.add(map.get(tokens[1]));
				}
			}
			return modifiedValues;
		} else {
			// normal case
			return currentCSVRow.getData(field);
		}
	}
	
	/**
	 * Copies field1 to field2 from old to current row-data
	 * @param field1 CSV field name (CSV)
	 * @param field2 to field (NDL target item)
	 * @param excludes which values to exclude
	 * @param fullDataCopy this flag enables whether full data copy or not
	 * @throws IOException throws error in case logging fails
	 */
	protected void copy(String field1, String field2, Set<String> excludes, boolean fullDataCopy) throws IOException {
		Collection<String> csvvalues = getData(field1);
		if(csvvalues == null || csvvalues.isEmpty()) {
			// values not found
			log("CSV field: " + field1 + " is blank.");
		} else {
			// available
			for(String value : csvvalues) {
				// separator exists
				if(!excludes.contains(value)) {
					// filter
					currentTargetRow.addData(field2, fullDataCopy ? NDLDataUtils.createNewList(value)
							: getModifiedValue(field2, multipleValueSeparator, value));
				}
			}
		}
	}
	
	/**
	 * Copies field1 to field2 from old to current row-data
	 * @param field1 CSV field name (CSV)
	 * @param field2 to field (NDL target item)
	 * @param excludes which values to exclude
	 * @throws IOException throws error in case logging fails
	 */
	protected void copy(String field1, String field2, Set<String> excludes)
			throws IOException {
		copy(field1, field2, excludes, fullDataCopyFlag);
	}
	
	/**
	 * Copies field1 to field2 from old to current row-data
	 * @param field1 CSV field name (CSV)
	 * @param field2 to field (NDL target item)
	 * @param includes which values to include
	 * @param fullDataCopy this flag enables whether full data copy or not
	 * @throws IOException throws error in case logging fails
	 */
	protected void copySelected(String field1, String field2, Set<String> includes, boolean fullDataCopy)
			throws IOException {
		Collection<String> csvvalues = getData(field1);
		if(csvvalues == null || csvvalues.isEmpty()) {
			// values not found
			log("CSV field: " + field1 + " is blank.");
		} else {
			// available
			for(String value : csvvalues) {
				// separator exists
				if(includes.contains(value)) {
					// filter
					currentTargetRow.addData(field2, fullDataCopy ? NDLDataUtils.createNewList(value)
							: getModifiedValue(field2, multipleValueSeparator, value));
				}
			}
		}
	}
	
	/**
	 * Copies field1 to field2 from old to current row-data
	 * @param field1 CSV field name (CSV)
	 * @param field2 to field (NDL target item)
	 * @param includes which values to include
	 * @throws IOException throws error in case logging fails
	 */
	protected void copySelected(String field1, String field2, Set<String> includes)
			throws IOException {
		copySelected(field1, field2, includes, fullDataCopyFlag);
	}
	
	/**
	 * Copies field1 to field2 from old to current row-data
	 * @param field1 CSV field name (CSV)
	 * @param field2 to field (NDL target item)
	 * @param fullDataCopy this flag enables whether full data copy or not
	 * @param excludes which values to exclude
	 * @throws IOException throws error in case logging fails
	 */
	protected void copy(String field1, String field2, boolean fullDataCopy, String... excludes) throws IOException {
		Set<String> newExcludes = new HashSet<String>(2);
		for(String exclude : excludes) {
			newExcludes.add(exclude);
		}
		copy(field1, field2, newExcludes, fullDataCopy);
	}
	
	/**
	 * Copies field1 to field2 from old to current row-data
	 * @param field1 CSV field name (CSV)
	 * @param field2 to field (NDL target item)
	 * @param excludes which values to exclude
	 * @throws IOException throws error in case logging fails
	 */
	protected void copy(String field1, String field2, String... excludes) throws IOException {
		copy(field1, field2, fullDataCopyFlag, excludes);
	}
	
	/**
	 * Copies field1 to field2 from old to current row-data
	 * @param field1 CSV field name (CSV)
	 * @param field2 to field (NDL target item)
	 * @param fullDataCopy this flag enables whether full data copy or not
	 * @param includes which values to include
	 * @throws IOException throws error in case logging fails
	 */
	protected void copySelected(String field1, String field2, boolean fullDataCopy, String... includes)
			throws IOException {
		Set<String> newIncludes = new HashSet<String>(2);
		for(String include : includes) {
			newIncludes.add(include);
		}
		copySelected(field1, field2, newIncludes, fullDataCopy);
	}
	
	/**
	 * Copies field1 to field2 from old to current row-data
	 * @param field1 CSV field name (CSV)
	 * @param field2 to field (NDL target item)
	 * @param includes which values to include
	 * @throws IOException throws error in case logging fails
	 */
	protected void copySelected(String field1, String field2, String... includes) throws IOException {
		copySelected(field1, field2, fullDataCopyFlag, includes);
	}
	
	/**
	 * Adds value to given field
	 * @param field target field to add
	 * @param value value to be added
	 */
	protected void add(String field, String value) {
		// separator exists
		add(field, value, fullDataCopyFlag);
	}
	
	/**
	 * Adds value to given field
	 * @param field target field to add
	 * @param value value to be added
	 * @param fullDataCopy this flag enables whether full data copy or not
	 */
	protected void add(String field, String value, boolean fullDataCopy) {
		// separator exists
		currentTargetRow.addData(field, fullDataCopy ? NDLDataUtils.createNewList(value)
				: getModifiedValue(field, multipleValueSeparator, value));
	}
	
	/**
	 * Adds value to given field
	 * @param field target field to add
	 * @param values values to be added
	 */
	protected void add(String field, String ... values) {
		for(String value : values) {
			add(field, value);
		}
	}
	
	/**
	 * Adds value to given field
	 * @param field target field to add
	 * @param fullDataCopy this flag enables whether full data copy or not
	 * @param values values to be added
	 */
	protected void add(String field, boolean fullDataCopy, String ... values) {
		for(String value : values) {
			add(field, value, fullDataCopy);
		}
	}
	
	/**
	 * Adds value to given field
	 * @param field target field to add
	 * @param values values to be added
	 */
	protected void add(String field, Collection<String> values) {
		Iterator<String> i = values.iterator();
		while(i.hasNext()) {
			add(field, i.next());
		}
	}
	
	/**
	 * Adds value to given field
	 * @param field target field to add
	 * @param fullDataCopy this flag enables whether full data copy or not
	 * @param values values to be added
	 */
	protected void add(String field, boolean fullDataCopy, Collection<String> values) {
		Iterator<String> i = values.iterator();
		while(i.hasNext()) {
			add(field, fullDataCopy, i.next());
		}
	}
	
	/**
	 * Copies field1 to field2 from old to current row-data
	 * @param field1 CSV field name (CSV)
	 * @param field2 to field (NDL target item)
	 * @param fullDataCopy this flag enables whether full data copy or not
	 * @throws IOException throws exception in case of data copy
	 * @see #copyCSVColumns(NDLDataPair...)
	 * @see #copyCSVColumns(String...)
	 */
	protected void copy(String field1, String field2, boolean fullDataCopy) throws IOException {
		Set<String> excludes = new HashSet<String>(2); // empty
		copy(field1, field2, excludes, fullDataCopy);
	}
	
	/**
	 * Copies field1 to field2 from old to current row-data
	 * @param field1 CSV field name (CSV)
	 * @param field2 to field (NDL target item)
	 * @throws IOException throws exception in case of data copy
	 * @see #copyCSVColumns(NDLDataPair...)
	 * @see #copyCSVColumns(String...)
	 */
	protected void copy(String field1, String field2) throws IOException {
		copy(field1, field2, fullDataCopyFlag);
	}
	
	/**
	 * Copy same as CSV column and it's value.
	 * Make sure column names are same as NDL registered field
	 * <b>Note: Don't add JSON keyed field here, for this use {@link #copy(String, String)} method</b>
	 * @param fullDataCopy this flag enables whether full data copy or not
	 * @param fields fields to copy
	 * @throws IOException throws exception in case of data copy
	 * @see #copyCSVColumns(NDLDataPair...)
	 */
	protected void copyCSVColumns(boolean fullDataCopy, String ... fields) throws IOException {
		for(String field : fields) {
			copy(field, field, fullDataCopy);
		}
	}
	
	/**
	 * Copy same as CSV column and it's value.
	 * Make sure column names are same as NDL registered field
	 * <b>Note: Don't add JSON keyed field here, for this use {@link #copy(String, String)} method</b>
	 * @param fields fields to copy
	 * @throws IOException throws exception in case of data copy
	 * @see #copyCSVColumns(NDLDataPair...)
	 */
	protected void copyCSVColumns(String ... fields) throws IOException {
		copyCSVColumns(fullDataCopyFlag, fields);
	}
	
	/**
	 * Copy same as CSV column and it's value.
	 * @param fullDataCopy this flag enables whether full data copy or not
	 * @param fields field details, first value will be CSV column name and second one will  be NDL field name
	 * @throws IOException throws exception in case of data copy
	 * @see #copyCSVColumns(String...)
	 */
	protected void copyCSVColumns(boolean fullDataCopy, NDLDataPair<String>... fields) throws IOException {
		for(NDLDataPair<String> field : fields) {
			copy(field.first(), field.second(), fullDataCopy);
		}
	}
	
	/**
	 * Copy same as CSV column and it's value.
	 * @param fields field details, first value will be CSV column name and second one will  be NDL field name
	 * @throws IOException throws exception in case of data copy
	 * @see #copyCSVColumns(String...)
	 */
	protected void copyCSVColumns(NDLDataPair<String>... fields) throws IOException {
		copyCSVColumns(fullDataCopyFlag, fields);
	}
	
	/**
	 * gets primary column value
	 * @param item row data to find primary value
	 * @return returns primary value
	 * @throws PrimaryIndexNotFoundException throws error if primary index value not found
	 */
	protected String getPrimaryValue(RowData item) throws PrimaryIndexNotFoundException{
		if(primaryColumnAvailable) {
			Collection<String> values = item.getData(primaryIDColumn);
			if(values == null || values.isEmpty()) {
				// no value available
				throw new PrimaryIndexNotFoundException(primaryIDColumn + " is blank");
			} else {
				return values.iterator().next();
			}
		} else {
			 return null;
		}
	}
	
	/**
	 * Corrects whole set of data
	 * @throws Exception throws Exception in case data correction error occurs
	 */
	public void genrateData() throws Exception {
		processData(); // process data
	}
	
	/**
	 * Sets multiple value separator character by which multiple values splitted and placed accordingly
	 * @param multipleValueSeparator multiple value separator character
	 */
	public void setMultipleValueSeparator(char multipleValueSeparator) {
		this.multipleValueSeparator = multipleValueSeparator;
	}
	
	/**
	 * Gets primary ID column
	 * @param primaryIDColumn primary ID column
	 */
	public void setPrimaryIDColumn(String primaryIDColumn) {
		this.primaryIDColumn = primaryIDColumn;
		primaryColumnAvailable = true;
		primaryColumnTracker = new HashSet<String>(); // primary column track to avoid duplicate value
	}
}