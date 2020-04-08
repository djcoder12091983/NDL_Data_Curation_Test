package org.iitkgp.ndl.data.duplicate.validator;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.iitkgp.ndl.data.BaseNDLDataItem;
import org.iitkgp.ndl.data.Transformer;
import org.iitkgp.ndl.data.container.AbstractDataContainer;
import org.iitkgp.ndl.data.container.DataContainerNULLConfiguration;
import org.iitkgp.ndl.data.iterator.DataIterator;

/**
 * NDL Data duplicate checker, compares between source and destination to check duplicates in source
 * @param <S> Source data type
 * @param <D> Destination data type
 * @param <SR> Source data reader
 * @param <DR> Destination data reader
 * @param <SC> Source data reader configuration
 * @param <DC> Destination data reader configuration
 * @author Debasis
 */
public abstract class AbstractNDLDataDuplicateValidator<S extends BaseNDLDataItem, D extends BaseNDLDataItem, SR extends DataIterator<S, SC>, DR extends DataIterator<D, DC>, SC, DC>
		extends AbstractDataContainer<DataContainerNULLConfiguration<SC>> {
	
	// log file name
	static final String COMPARISON_DUPLICATES_FILENAME = "comparison.duplicate";
	
	// csv column details
	Map<String, String> csvHeaders = new LinkedHashMap<String, String>(4);
	// source column to compare for duplicacy check
	List<String> sourceColumnFilters = new LinkedList<String>();
	// normalizer for source data if needed
	Map<String, Transformer<String, String>> normalizers = new HashMap<String, Transformer<String,String>>(2);
	String destination;
	// source data indexing for faster comparison
	NDLDataFieldMap index = new NDLDataFieldMap();
	
	/**
	 * Constructor
	 * @param source source data location
	 * @param destination data location
	 * @param logLocation logging location
	 * @param name logical name which differentiates log file(s), output file(s) from other source
	 */
	public AbstractNDLDataDuplicateValidator(String source, String destination, String logLocation, String name) {
		super(source, logLocation, name);
		this.destination = destination;
		turnOffGlobalLoggingFlag();
	}
	
	/**
	 * Adds CSV header &lt;field name, display name&gt;
	 * <pre>Note: Selected columns/fields should be a subset of added columns/fields using </pre> {@link #addSourceColumnSelector(String)}
	 * @param fieldName field/column name of source database
	 * @param displayName how CSV header name will look
	 * @see AbstractNDLDataDuplicateValidator#addSourceColumnSelector(String)
	 */
	public void addCSVHeader(String fieldName, String displayName) {
		csvHeaders.put(fieldName, displayName);
	}
	
	/**
	 * Adds source column/field for comparison with destination
	 * <pre>Note: This should be super-set of added columns/fields using </pre>{@link #addCSVHeader(String, String)}
	 * @param fieldName specific field name
	 * @param normalizer associated normalizer if needed otherwise null
	 */
	public void addSourceColumnSelector(String fieldName, Transformer<String, String> normalizer) {
		sourceColumnFilters.add(fieldName);
		if(normalizer != null) {
			normalizers.put(fieldName, normalizer);
		}
	}
	
	/**
	 * Adds source column/field for comparison with destination
	 * <pre>Note: This should be super-set of added columns/fields using </pre>{@link #addCSVHeader(String, String)}
	 * @param fieldName specific field name
	 * @see #addSourceColumnSelector(String, Transformer)
	 */
	public void addSourceColumnSelector(String fieldName) {
		addSourceColumnSelector(fieldName, null);
	}
	
	/**
	 * This method compares source and destination and find duplicates in source
	 */
	@Override
	public void processData() throws Exception {
		if(csvHeaders.isEmpty()) {
			// invalid condition
			throw new IllegalStateException("CSV headers should be provided");
		}
		try {
			// add CSV logger for duplicates detail
			addCSVLogger(COMPARISON_DUPLICATES_FILENAME);
			// headers
			Collection<String> columns = csvHeaders.values();
			int cs = columns.size() * 2 + 2;
			String headers[] = new String[cs];
			int c = 0;
			headers[c++] = "Source ID";
			headers[c++] = "Destination ID";
			for(String column : columns) {
				headers[c++] = column + "(Source)";
				headers[c++] = column + "(Destination)";
			}
			
			// validations
			File sfile = new File(input);
			if(!sfile.isDirectory()) {
				throw new IllegalStateException(input + " Should be directory");
			}
			File dfile = new File(destination);
			if(!dfile.isDirectory()) {
				throw new IllegalStateException(destination + " Should be directory");
			}
			
			// source data reader and indexing for faster comparison
			for(File file : sfile.listFiles()) {
				SR sr = getSoureReader(file);
				while(sr.hasNext()) {
					S sdata = sr.next();
					index.add(getSourceID(sdata), sdata, sourceColumnFilters, normalizers);
				}
			}
			
			// destination data traversal
			// find duplicates if exists
			for(File file : dfile.listFiles()) {
				DR dr = getDestinationReader(file);
				while(dr.hasNext()) {
					D ddata = dr.next();
					// finds duplicate
					boolean dup = duplicate(index, ddata);
					if(dup) {
						// duplicate found
						c = 0;
						String cvalues[] = new String[cs];
						cvalues[c++] = getDestinationID(ddata);
						for(String column : columns) {
							// TODO
							//cvalues[c++] = 
						}
						// data write
						log(COMPARISON_DUPLICATES_FILENAME, cvalues);
					}
				}
			}
		} finally {
			// close resources
			close();
		}
	}

	/**
	 * Comparison logic to check duplicates in source from destination
	 * <pre>Note: This method should take care of destination duplicates and discard items</pre>
	 * @param index source index detail
	 * @param destination destination data
	 * @return returns true if source data is duplicate with destination
	 */
	public abstract boolean duplicate(NDLDataFieldMap index, BaseNDLDataItem destination);
	
	/**
	 * gets source data reader for a given source
	 * @param source given source
	 * @return returns source data reader
	 */
	public abstract SR getSoureReader(File source);
	
	/**
	 * gets destination data reader for a given destination
	 * @param destination given destination
	 * @return returns destination data reader
	 */
	public abstract DR getDestinationReader(File destination);
	
	/**
	 * Gets source ID
	 * @param data data to find ID
	 * @return returns ID
	 */
	public abstract String getSourceID(S data);
	
	/**
	 * Gets destination ID
	 * @param data data to find ID
	 * @return returns ID
	 */
	public abstract String getDestinationID(D data);
}