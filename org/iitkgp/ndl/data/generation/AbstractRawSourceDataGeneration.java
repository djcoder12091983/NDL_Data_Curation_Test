package org.iitkgp.ndl.data.generation;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.BaseNDLDataItem;
import org.iitkgp.ndl.data.NDLAssetType;
import org.iitkgp.ndl.data.NDLDataItem;
import org.iitkgp.ndl.data.NDLDataPair;
import org.iitkgp.ndl.data.RowData;
import org.iitkgp.ndl.data.container.AbstractNDLDataContainer;
import org.iitkgp.ndl.data.container.DataContainerNULLConfiguration;
import org.iitkgp.ndl.data.container.NDLDuplicateFieldCorrectionContainerAdapter;
import org.iitkgp.ndl.data.iterator.DataIterator;
import org.iitkgp.ndl.data.normalizer.NDLDataNormalizationPool;
import org.iitkgp.ndl.data.normalizer.NDLDataNormalizer;
import org.iitkgp.ndl.data.writer.DataWriter;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * This abstract class encapsulates some logic for data generation from raw source
 * @param <D> required data item
 * @param <R> required data reader
 * @param <W> Data writer for post process of each item
 * @param <O> Target data type
 * @param <C> required data reader configuration
 * @author Debasis
 */
public abstract class AbstractRawSourceDataGeneration<D extends BaseNDLDataItem, R extends DataIterator<D, C>, W extends DataWriter<O>, O extends NDLDataItem, C>
		extends AbstractNDLDataContainer<D, DataIterator<D, C>, C> {
	
	String outputLocation; // output location
	String outputFileName; // if compression mode is ON
	W writer = null; // data writer 
	// asset tracker
	Map<String, String> assetLocations = new HashMap<String, String>(4);
	Map<String, String> assetDefaultLocations = new HashMap<String, String>(4);
	Map<String, Integer> assetStatistics = new HashMap<String, Integer>(4);
	// global normalizers
	NDLDataNormalizationPool normalizers = new NDLDataNormalizationPool();
	boolean normalizationFlag = false;
	
	String currentAssetID = null;
	
	// duplicate fixation
	Map<String, NDLDuplicateFieldCorrectionContainerAdapter<D>> duplicateFixations
		= new HashMap<String, NDLDuplicateFieldCorrectionContainerAdapter<D>>(2);
	
	/**
	 * Turn on normalization flag
	 */
	public void turnOnNormalizationFlag() {
		normalizationFlag = false;
	}
	
	/**
	 * gets modified value, whether to normalize or not
	 * @param field field to normalize or not
	 * @param multipleValueSeparator separator for multiple values
	 * @param value value to modify
	 * @return returns modified values
	 */
	protected Collection<String> getModifiedValue(String field, char multipleValueSeparator, String value) {
		if(normalizationFlag) {
			// apply normalization
			return normalizers.normalize(field, multipleValueSeparator, value);
		} else {
			// simply split values
			Collection<String> list = NDLDataUtils.createEmptyList();
			StringTokenizer tokens = new StringTokenizer(value, String.valueOf(multipleValueSeparator));
			while(tokens.hasMoreTokens()) {
				String v = tokens.nextToken();
				if(StringUtils.isNotBlank(v)) {
					// not blank
					list.add(v);
				}
			}
			return list;
		}
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
	 * @param input source input, from where data to be processed
	 * @param logLocation logging location, where logging takes place during data processing
	 */
	public AbstractRawSourceDataGeneration(String input, String logLocation) {
		super(input, logLocation);
	}
	
	/**
	 * Constructor
	 * @param input source input, from where data to be processed
	 * @param logLocation logging location, where logging takes place during data processing
	 * @param containerConfiguration Container configuration to initialize
	 */
	public AbstractRawSourceDataGeneration(String input, String logLocation,
			DataContainerNULLConfiguration<C> containerConfiguration) {
		super(input, logLocation, containerConfiguration);
	}
	
	/**
	 * Constructor
	 * @param input source input, from where data to be processed
	 * @param logLocation logging location, where logging takes place during data processing
	 * @param name logical data source name, by which logging file(s) etc. prefixed
	 */
	public AbstractRawSourceDataGeneration(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}
	
	/**
	 * Constructor
	 * @param input source input, from where data to be processed
	 * @param logLocation logging location, where logging takes place during data processing
	 * @param name logical data source name, by which logging file(s) etc. prefixed
	 * @param validationFlag validation flag for data validation
	 */
	public AbstractRawSourceDataGeneration(String input, String logLocation, String name, boolean validationFlag) {
		super(input, logLocation, name, validationFlag);
	}
	
	/**
	 * Constructor
	 * @param input source input, from where data to be processed
	 * @param logLocation logging location, where logging takes place during data processing
	 * @param name logical data source name, by which logging file(s) etc. prefixed
	 * @param containerConfifuration Container configuration to initialize
	 */
	public AbstractRawSourceDataGeneration(String input, String logLocation, String name,
			DataContainerNULLConfiguration<C> containerConfifuration) {
		super(input, logLocation, name, containerConfifuration);
	}
	
	/**
	 * {@inheritDoc}
	 */
	public boolean processItem(D item) throws Exception {
		boolean processed = processRawSourceItem(item);
		if(processed) {
			// successfully processed
			// duplicate fixation
			for(String field : duplicateFixations.keySet()) {
				duplicateFixations.get(field).fix(item);
			}
		}
		return processed;
	}
	
	/**
	 * This abstract method encapsulates each raw-source item processing logic
	 * @param item each data item read by system
	 * @return <pre>returns true data processing successfully done, if fails returns false</pre>
	 * Note: if any data item processing skipped then it should return false. It's required when filter logic is added.
	 * @throws Exception throws error if any error occurs during processing each item
	 */
	public abstract boolean processRawSourceItem(D item) throws Exception;
	
	/**
	 * Creates target item from current raw source
	 * @param item current raw source
	 * @return returns created target item
	 * @throws Exception throws exception in case of creation fails
	 */
	public abstract O createTargetItem(D item) throws Exception;
	
	/**
	 * Handles more post process operations
	 * @param row current processing row
	 * @param targetItem current target item
	 * @throws Exception throws exception in case any process error occurs
	 */
	public void postProcessItem(D row, O targetItem) throws Exception {
		// blank implementation
	}
	
	/**
	 * This method actually stores corrected data and assets if any 
	 */
	@Override
	public void postProcessItem(D item) throws Exception {
		// super call
		super.postProcessItem(item);
		// custom method
		// item to SIP conversion
		String assetID = currentAssetID;
		O target = createTargetItem(item);
		List<String> missing = NDLDataUtils.addAsset(target, assetLocations, assetID, assetDefaultLocations); // adds assets
		if(!missing.isEmpty()) {
			// logging missing details
			for(String miss : missing) {
				String message = miss + " asset not found.";
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
		postProcessItem(item, target); // more post operations
		// write the corrected data
		writer.write(target);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void postProcessData() throws Exception {
		// super call
		super.postProcessData();
		
		// duplicate fixation counts
		for(String field : duplicateFixations.keySet()) {
			System.out.println(
					"[" + field + "] still duplicate count: " + duplicateFixations.get(field).getDuplicateCount());
		}
		
		// asset statistics
		for(String asset : assetStatistics.keySet()) {
			System.out.println(asset + " missing: " + assetStatistics.get(asset));
		}
	}
	
	/**
	 * Adds normalizer against a field, if additionally normalizer needs to be loaded which is not defined
	 * in <b>/conf/default.data.normalization.conf.properties</b>
	 * @param field field with which normalizer is associated
	 * @param normalizer normalizer class, it should be type of {@link NDLDataNormalizer}
	 * @see NDLDataNormalizationPool#addNormalizer(String, String)
	 */
	public void addNormalizer(String field, String normalizer) {
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
	 * Deregisters all normalizer
	 */
	public void deregisterAllNormalizers() {
		// remove all normalizers
		normalizers.deregisterAllNormalizers();
	}
	
	/**
	 * Gets ID to identify asset from asset location (thumbnails/licenses/fulltexts etc.).
	 * @param item from which the ID will be extracted
	 * @return returns associated ID
	 */
	protected abstract String getAssetID(RowData item);
	
	/**
	 * Adds NDL asset location from which assets to be loaded by some identifier
	 * @param type NDL asset type
	 * @param location NDL asset location
	 * @see #getAssetID(RowData)
	 */
	public void addAssetLocation(NDLAssetType type, String location) {
		assetLocations.put(type.getType(), location);
	}
	
	/**
	 * Adds NDL asset default/fallback location from which assets to be loaded by some identifier
	 * @param type NDL asset type
	 * @param location NDL asset location
	 * @see #getAssetID(RowData)
	 */
	public void addAssetDefaultLocation(NDLAssetType type, String location) {
		assetDefaultLocations.put(type.getType(), location);
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
}