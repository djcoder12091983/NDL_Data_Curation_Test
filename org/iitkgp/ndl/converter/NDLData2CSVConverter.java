package org.iitkgp.ndl.converter;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.context.NDLDataValidationContext;
import org.iitkgp.ndl.data.Filter;
import org.iitkgp.ndl.data.NDLDataItem;
import org.iitkgp.ndl.data.RowData;
import org.iitkgp.ndl.data.RowDataList;
import org.iitkgp.ndl.data.Transformer;
import org.iitkgp.ndl.data.container.AbstractNDLDataContainer;
import org.iitkgp.ndl.data.container.DataContainerNULLConfiguration;
import org.iitkgp.ndl.data.iterator.DataIterator;
import org.iitkgp.ndl.data.iterator.DataSourceNULLConfiguration;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * <pre>Abstract class encapsulates NDL data to CSV conversion for all available data in data.
 * It takes input file (compressed and AIP folder) and generates CSV</pre>
 * Note: <ul>
 * <li>
 * Currently it supports 'tar.gz' compressed file, but other files can be supported,
 * for more details see {@link NDLDataUtils#getCompressedDataReader(java.io.File)}
 * </li>
 * <li>
 * See /conf/default.global.configuration.properties#csv.data.write.multiple.value.separator multivalue separator in CSV cell
 * </li>
 * <li>
 * See /conf/default.global.configuration.properties#csv.log.write.line.threshold.limit for CSV file rolling
 * </li>
 * </ul>
 * Here column means NDL metadata schema related fields/attributes
 * @param <D> data item for source type (AIP/SIP etc.)
 * @param <R> data reader for source type (AIP/SIP etc.)
 * @see NDLSIP2CSVConverter
 * @see NDLAIP2CSVConverter
 * @see NDLDataItem
 * @see DataIterator
 * @author Debasis
 */
public abstract class NDLData2CSVConverter<D extends NDLDataItem, R extends DataIterator<D, DataSourceNULLConfiguration>>
		extends AbstractNDLDataContainer<D, R, DataSourceNULLConfiguration> {
	
	// configurations loaded from /conf/default.global.configuration.properties
	char multivalueSeparator = NDLConfigurationContext.getConfiguration("csv.data.write.multiple.value.separator")
			.charAt(0);
	long csvThresholdLimit = Long.parseLong(NDLConfigurationContext.getConfiguration("csv.log.write.line.threshold.limit"));
	
	long dataSelectorCounter = 0; // how many data selected (it counts lesser when applies filter on data)
	int fileIndex = 1; // file index suffix for CSV file rolling
	// data writer(s)
	RowDataList dataList = new RowDataList();
	RowData currentRow = null;
	List<String> orderedColumns = new LinkedList<String>(); // if any column to appear first
	Set<String> excludeColumns = new HashSet<String>(2); // column to be excluded
	// column selection (if any)
	Map<String, String> columnSelectors = new HashMap<String, String>();
	Map<String, Transformer<String, String>> columnTransformers = new HashMap<String, Transformer<String, String>>(2);
	Map<String, Filter<String>> columnFilters = new HashMap<String, Filter<String>>(2);
	
	// flag which indicates allow all blank fields
	boolean allowAllBlankFields = false;
	
	static {
		// validation context startup
		NDLDataValidationContext.init();
	}
	
	/**
	 * Constructor
	 * @param input Input file (compressed and AIP folder) and generates CSV
	 * @param logLocation Log location where CSV data will be written
	 */
	public NDLData2CSVConverter(String input, String logLocation) {
		super(input, logLocation);
		turnOffGlobalLoggingFlag();
	}
	
	/**
	 * Constructor
	 * @param input Input file (compressed and AIP folder) and generates CSV
	 * @param logLocation Log location where CSV data will be written
	 * @param containerConfifuration Container configuration to initialize
	 */
	public NDLData2CSVConverter(String input, String logLocation,
			DataContainerNULLConfiguration<DataSourceNULLConfiguration> containerConfifuration) {
		super(input, logLocation, containerConfifuration);
		turnOffGlobalLoggingFlag();
	}
	
	/**
	 * Constructor
	 * @param input Input file (compressed and AIP folder) and generates CSV
	 * @param logLocation Log location where CSV data will be written
	 * @param name Logical name typically source name which adds prefix to CSV file(s)
	 */
	public NDLData2CSVConverter(String input, String logLocation, String name) {
		super(input, logLocation, name);
		turnOffGlobalLoggingFlag();
	}
	
	/**
	 * Constructor
	 * @param input Input file (compressed and AIP folder) and generates CSV
	 * @param logLocation Log location where CSV data will be written
	 * @param name Logical name typically source name which adds prefix to CSV file(s)
	 * @param validationFlag validation flag for data validation
	 */
	public NDLData2CSVConverter(String input, String logLocation, String name, boolean validationFlag) {
		super(input, logLocation, name, validationFlag);
		turnOffGlobalLoggingFlag();
	}
	
	/**
	 * Constructor
	 * @param input Input file (compressed and AIP folder) and generates CSV
	 * @param logLocation Log location where CSV data will be written
	 * @param name Logical name typically source name which adds prefix to CSV file(s)
	 * @param containerConfifuration Container configuration to initialize
	 */
	public NDLData2CSVConverter(String input, String logLocation, String name,
			DataContainerNULLConfiguration<DataSourceNULLConfiguration> containerConfifuration) {
		super(input, logLocation, name, containerConfifuration);
		turnOffGlobalLoggingFlag();
	}
	
	/**
	 * Constructor
	 * @param input Input file (compressed and AIP folder) and generates CSV
	 * @param logLocation Log location where CSV data will be written
	 * @param name Logical name typically source name which adds prefix to CSV file(s)
	 * @param containerConfifuration Container configuration to initialize
	 * @param validationFlag validation flag for data validation
	 */
	public NDLData2CSVConverter(String input, String logLocation, String name,
			DataContainerNULLConfiguration<DataSourceNULLConfiguration> containerConfifuration,
			boolean validationFlag) {
		super(input, logLocation, name, containerConfifuration, validationFlag);
		turnOffGlobalLoggingFlag();
	}
	
	/**
	 * Turns on flag which indicates all blank fields
	 */
	public void turnOnAllowAllBlankFields() {
		allowAllBlankFields = true;
	}
	
	/**
	 * Adds column details with actual field name and it's logical name.
	 * For example column is <b>dc.contributor.author</b> and it's logical name is <b>Author</b> 
	 * @param column Column name
	 * @param displayName Column display name
	 * @param filter filter to select data from multiple values
	 * @param transformer transformer logic of column value
	 */
	public void addColumnSelector(String column, String displayName, Filter<String> filter,
			Transformer<String, String> transformer) {
		displayName = displayName.trim();
		column = column.trim();
		columnSelectors.put(column, displayName);
		if(transformer != null) {
			columnTransformers.put(column, transformer);
		}
		if(filter != null) {
			columnFilters.put(column, filter);
		}
		// add ordered columns
		orderedColumns.add(displayName);
	}
	
	/**
	 * Adds column details with actual field name and it's logical name.
	 * For example column is <b>dc.contributor.author</b> and it's logical name is <b>Author</b> 
	 * @param column Column name
	 * @param displayName Column display name
	 * @param filter filter to select data from multiple values
	 */
	public void addColumnSelector(String column, String displayName, Filter<String> filter) {
		addColumnSelector(column, displayName, filter, null);
	}
	
	/**
	 * Adds column details with actual field name and it's logical name.
	 * For example column is <b>dc.contributor.author</b> and it's logical name is <b>Author</b> 
	 * @param column Column name
	 * @param displayName Column display name
	 * @param transformer transformer logic of column value
	 */
	public void addColumnSelector(String column, String displayName, Transformer<String, String> transformer) {
		addColumnSelector(column, displayName, null, transformer);
	}
	
	/**
	 * Adds column details with actual field name and it's logical name.
	 * For example column is <b>dc.contributor.author</b> and it's logical name is <b>Author</b> 
	 * @param column Column name
	 * @param displayName Column display name
	 */
	public void addColumnSelector(String column, String displayName) {
		addColumnSelector(column, displayName, null, null);
	}
	
	/**
	 * Adds columns which will appear on first in CSV
	 * @param column Column name
	 */
	public void addOrderedColumn(String column) {
		orderedColumns.add(column);
	}
	
	/**
	 * Adds column exclusion information, which will not appear in CSV
	 * @param columns Column names
	 */
	public void addExcludeColumn(String ... columns) {
		for(String column : columns) {
			excludeColumns.add(column);
		}
	}
	
	/**
	 * Sets custom CSV row threshold limit
	 * @param csvThresholdLImit row limit
	 */
	public void setCsvThresholdLimit(long csvThresholdLImit) {
		this.csvThresholdLimit = csvThresholdLImit;
	}
	
	/**
	 * Sets multiple-value separator in CSV cell
	 * @param multivalueSeparator multiple-value separator in CSV cell
	 */
	public void setMultivalueSeparator(char multivalueSeparator) {
		this.multivalueSeparator = multivalueSeparator;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void preProcessData() throws Exception {
		super.preProcessData(); // super call
		// data reader, asset loading flag is OFF
		dataReader.turnOffAssetLoadingFlag();
	}
	
	/**
	 * Encapsulates conversion logic
	 * @throws Exception Throws Exception in case of error occurs
	 */
	public void convert() throws Exception {
		orderedColumns.add(0, "Handle_ID"); // first column will be handle_ID
		turnOffGlobalLoggingFlag(); // off global writer flag
		processData(); // process whole data
		// last data write
		if(!dataList.isEmpty()) {
			// time to CSV flush
			dataList.orderColumnHeaders(orderedColumns);
			dataList.flush2CSV(new File(logLocation, getFileName(fileIndex++ + ".csv")),
					multivalueSeparator);
		}
		// log
		System.out.println("Total: " + dataSelectorCounter + " data written out of: " + processedCounter);
	}
	
	/**
	 * Adds extra data for each item
	 * @param item current working item
	 */
	void addExtraData(D item) {
		// blank
	}
	
	/**
	 * Processes each row and writes the data into CSV
	 */
	@Override
	public boolean processItem(D item) throws Exception {
		// values of item
		Map<String, Collection<String>> values = item.getAllValues(excludeColumns);
		if(values.isEmpty()) {
			// empty data set
			System.err.println("WARN: " + item.getId() + " has no fields.");
			return false;
		}
		currentRow = new RowData(); // new data row for new columns
		addExtraData(item);
		if(columnSelectors.isEmpty()) {
			// no column selectors available, all columns
			currentRow.addAllData(values);
		} else {
			// consider column selection logic
			Set<String> keys = columnSelectors.keySet();
			for(String key : keys) {
				String originalKey = key;
				String k = columnSelectors.get(originalKey);
				String split[] = originalKey.split(":");
				if(split.length == 2) {
					// JSON
					key = split[0].trim();
					String jsonKey = split[1].trim();
					Collection<String> selectedValues = values.get(key);
					if(selectedValues != null) {
						// data available
						List<String> selectedJsonValues = new LinkedList<String>();
						for(String selectedValue : selectedValues) {
							// filter by key
							Map<String, String> map = NDLDataUtils.mapFromJson(selectedValue, true);
							if(map.containsKey(jsonKey)) {
								// JSON key match found
								selectedJsonValues.add(map.get(jsonKey));
							}
						}
						if(allowAllBlankFields || !selectedJsonValues.isEmpty()) {
							// keyed by display name
							currentRow.addData(k, getTransformedValues(originalKey, selectedJsonValues),
									allowAllBlankFields);
						}
					} else if(allowAllBlankFields) {
						currentRow.addData(k, allowAllBlankFields, StringUtils.EMPTY);
					}
				} else {
					// normal field
					key = split[0].trim();
					Collection<String> selectedValues = values.get(key);
					if(selectedValues != null) {
						// keyed by display name
						currentRow.addData(k, getTransformedValues(originalKey, selectedValues), allowAllBlankFields);
					} else if(allowAllBlankFields) {
						currentRow.addData(k, allowAllBlankFields, StringUtils.EMPTY);
					}
				}
			}
		}
		if(!currentRow.isEmpty()) {
			// data available
			currentRow.addData("Handle_ID", item.getId()); // handle ID add to row
			dataList.addRowData(currentRow); // add to list
			if (++dataSelectorCounter % csvThresholdLimit == 0) {
				// time to CSV flush
				dataList.orderColumnHeaders(orderedColumns);
				dataList.flush2CSV(new File(logLocation, getFileName(fileIndex++ + ".csv")),
						multivalueSeparator);
				// log
				System.out.println(dataSelectorCounter + " data written out of: " + (processedCounter+1));
			}
		}
		return true;
	}
	
	// gets transformed values
	Collection<String> getTransformedValues(String key, Collection<String> selectedValues) {
		Collection<String> transformedvalues = new LinkedList<String>();
		Transformer<String, String> t = columnTransformers.get(key);
		Filter<String> f = columnFilters.get(key);
		for(String selectedValue : selectedValues) {
			if(f != null && !f.filter(selectedValue)) {
				// skip if filter discards
				continue;
			}
			if(t != null) {
				// transformer
				transformedvalues.addAll(t.transform(selectedValue));
			} else {
				// normal
				transformedvalues.add(selectedValue);
			}
		}
		return transformedvalues;
	}
}