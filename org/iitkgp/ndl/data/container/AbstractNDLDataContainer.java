package org.iitkgp.ndl.data.container;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.context.NDLDataValidationContext;
import org.iitkgp.ndl.data.Filter;
import org.iitkgp.ndl.data.NDLField;
import org.iitkgp.ndl.data.iterator.DataIterator;
import org.iitkgp.ndl.data.validator.NDLDataValidationBox;
import org.iitkgp.ndl.data.validator.NDLSchemaDetail;
import org.iitkgp.ndl.util.CommonUtilities;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * This data container specific to NDL data processing.
 * @param <D> required data item
 * @param <R> required data reader
 * @param <C> required data reader configuration
 * @author Debasis
 */
public abstract class AbstractNDLDataContainer<D, R extends DataIterator<D, C>, C>
		extends AbstractDataContainer<DataContainerNULLConfiguration<C>> {
	
	// data processing display limit
	long displayThresholdLimit = Long
			.parseLong(NDLConfigurationContext.getConfiguration("process.display.threshold.limit"));
	protected long processedCounter = 0; // how many data processed
	protected long skippedCounter = 0; // skip counter
	protected R dataReader; // required data reader
	
	// initial schema detail
	protected NDLSchemaDetail schemaDetail;
	// NDL data validator
	protected NDLDataValidationBox validator = null;
	boolean firstFailOnvalidation = true; // by default it's true
	// controlled key validation flags
	protected boolean validationFlag = true; // validation flag
	// more validation rules
	// TODO handle mandate fields
	Set<NDLField> validationMandateFields = new HashSet<NDLField>(2);
	
	List<Filter<D>> filters = new LinkedList<Filter<D>>(); // filters
	
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
	 */
	public AbstractNDLDataContainer(String input, String logLocation) {
		super(input, logLocation);
		contextSetup(); // context setup
		this.validator = new NDLDataValidationBox();
	}
	
	/**
	 * Constructor
	 * @param input source input, from where data to be processed
	 * @param logLocation logging location, where logging takes place during data processing
	 * @param validationFlag validation flag for data validation
	 */
	public AbstractNDLDataContainer(String input, String logLocation, boolean validationFlag) {
		super(input, logLocation);
		contextSetup(); // context setup
		this.validationFlag = validationFlag;
		if(validationFlag) {
			this.validator = new NDLDataValidationBox();
		}
	}
	
	/**
	 * Constructor
	 * @param input source input, from where data to be processed
	 * @param logLocation logging location, where logging takes place during data processing
	 * @param containerConfiguration Container configuration to initialize
	 */
	public AbstractNDLDataContainer(String input, String logLocation,
			DataContainerNULLConfiguration<C> containerConfiguration) {
		super(input, logLocation, containerConfiguration);
		contextSetup(); // context setup
		this.validator = new NDLDataValidationBox();
	}
	
	/**
	 * Constructor
	 * @param input source input, from where data to be processed
	 * @param logLocation logging location, where logging takes place during data processing
	 * @param containerConfiguration Container configuration to initialize
	 * @param validationFlag validation flag for data validation
	 */
	public AbstractNDLDataContainer(String input, String logLocation,
			DataContainerNULLConfiguration<C> containerConfiguration, boolean validationFlag) {
		super(input, logLocation, containerConfiguration);
		contextSetup(); // context setup
		this.validationFlag = validationFlag;
		if(validationFlag) {
			this.validator = new NDLDataValidationBox();
		}
	}
	
	/**
	 * Constructor
	 * @param input source input, from where data to be processed
	 * @param logLocation logging location, where logging takes place during data processing
	 * @param name logical data source name, by which logging file(s) etc. prefixed
	 */
	public AbstractNDLDataContainer(String input, String logLocation, String name) {
		super(input, logLocation, name);
		contextSetup(); // context setup
		this.validator = new NDLDataValidationBox();
	}
	
	/**
	 * Constructor
	 * @param input source input, from where data to be processed
	 * @param logLocation logging location, where logging takes place during data processing
	 * @param name logical data source name, by which logging file(s) etc. prefixed
	 * @param validationFlag validation flag to indicate whether validation takes place or not
	 */
	public AbstractNDLDataContainer(String input, String logLocation, String name, boolean validationFlag) {
		super(input, logLocation, name);
		contextSetup(); // context setup
		this.validationFlag = validationFlag;
		if(validationFlag) {
			this.validator = new NDLDataValidationBox();
		}
	}
	
	/**
	 * Constructor
	 * @param input source input, from where data to be processed
	 * @param logLocation logging location, where logging takes place during data processing
	 * @param name logical data source name, by which logging file(s) etc. prefixed
	 * @param containerConfifuration Container configuration to initialize
	 */
	public AbstractNDLDataContainer(String input, String logLocation, String name,
			DataContainerNULLConfiguration<C> containerConfifuration) {
		super(input, logLocation, name, containerConfifuration);
		contextSetup(); // context setup
		this.validator = new NDLDataValidationBox();
	}
	
	/**
	 * Constructor
	 * @param input source input, from where data to be processed
	 * @param logLocation logging location, where logging takes place during data processing
	 * @param name logical data source name, by which logging file(s) etc. prefixed
	 * @param containerConfifuration Container configuration to initialize
	 * @param validationFlag validation flag to indicate whether validation takes place or not
	 */
	public AbstractNDLDataContainer(String input, String logLocation, String name,
			DataContainerNULLConfiguration<C> containerConfifuration, boolean validationFlag) {
		super(input, logLocation, name, containerConfifuration);
		contextSetup(); // context setup
		this.validationFlag = validationFlag;
		if(validationFlag) {
			this.validator = new NDLDataValidationBox();
		}
	}
	
	/**
	 * Turns off control fields validation flag
	 */
	public void turnOffControlFieldsValidationFlag() {
		if(validationFlag) {
			this.validator.turnOffControlFieldsValidationFlag();
		}
	}
	
	/**
	 * Turns off JSON key validation flag
	 */
	public void turnOffJsonKeyValidationFlag() {
		if(validationFlag) {
			this.validator.turnOffJsonKeyValidationFlag();
		}
	}
	
	/**
	 * Turns off first fail validation flag
	 * <pre>Make sure basic errors somehow will be entertained.</pre>
	 * @see NDLDataValidationBox#turnOnFirstFailOnValidation()
	 */
	public void turnOffFirstFailOnValidation() {
		firstFailOnvalidation = false;
	}
	
	/**
	 * This flag stops showing warning messages
	 */
	public void dontShowWarnings() {
		if(this.validator != null) {
			this.validator.dontShowWarnings();
		}
	}
	
	/**
	 * Adds to exclude controlled field
	 * <pre>No JSON Key should be provided</pre>
	 * @param field field to exclude
	 */
	public void addExcludeControlledField(String field) {
		if(validationFlag) {
			this.validator.addExcludeControlledField(field);
		}
	}
	
	/**
	 * Adds to exclude controlled fields
	 * <pre>No JSON Key should be provided</pre>
	 * @param fields fields to exclude
	 */
	public void addExcludeControlledFields(String ...fields) {
		if(validationFlag) {
			this.addExcludeControlledFields(fields);
		}
	}
	
	/**
	 * Adds to exclude controlled fields
	 * <pre>No JSON Key should be provided</pre>
	 * @param fields fields to exclude
	 */
	public void addExcludeControlledFields(Collection<String> fields) {
		if(validationFlag) {
			this.addExcludeControlledFields(fields);
		}
	}
	
	/**
	 * Adds to mandate fields
	 * @param field field to make it mandate
	 */
	public void addValidationMandateField(String field) {
		validationMandateFields.add(new NDLField(field));
	}
	
	/**
	 * Adds to mandate fields
	 * @param fields fields to make it mandate
	 */
	public void addValidationMandateFields(String ...fields) {
		for(String field : fields) {
			validationMandateFields.add(new NDLField(field));
		}
	}
	
	/**
	 * Adds to mandate fields
	 * @param fields fields to make it mandate
	 */
	public void addValidationMandateFields(Collection<String> fields) {
		for(String field : fields) {
			validationMandateFields.add(new NDLField(field));
		}
	}
	
	/**
	 * This flag handles whether validation error/warning messages tracked handle-id wise
	 * <pre>Note: Less logging still exists.</pre> 
	 * @throws IOException throws error in case logger initialization error occurs
	 */
	public void turnOnDetailValidationLogging() throws IOException {
		// detail logging flag is ON
		if(this.validator != null) {
			this.validator.setDetailLogger(addTextLogger(NDLDataUtils.DETAIL_VALIDATION_LOGGER));
		}
	}
	
	/**
	 * Turns on logging error/warning into CSV files
	 * <pre>
	 * If this flag is ON then all fields related logging happens.
	 * If {@link #addFieldwiseLogger(String)} is called then this flag has no meaning
	 * </pre>
	 * @see #addFieldwiseLogger(String)
	 */
	public void turnOnFieldWiseDetailValidation() {
		// detail logging flag is ON
		if(this.validator != null) {
			this.validator.turnOnFieldWiseDetailValidation();
			this.validator.setContainer(this);
		}
	}
	
	/**
	 * Excludes field wise logging for a given filed name
	 * <pre>This API is valid if {@link #turnOnFieldWiseDetailValidation()} is called</pre>
	 * @param field given field name
	 */
	public void addExcludeFieldwiseLogger(String field) {
		// detail logging flag is ON
		if(this.validator != null) {
			this.validator.addExcludeFieldwiseLogger(field);
		}
	}
	
	/**
	 * Adds fields wise logger
	 * <pre>If this method is called then this flag has no meaning</pre>
	 * @param fieldName given field name (xx.yy.zz etc.)
	 * @throws IOException throws exception in case of logging add error occurs
	 */
	public void addFieldwiseLogger(String fieldName) throws IOException {
		// detail logging flag is ON
		if(this.validator != null) {
			this.validator.addFieldwiseLogger(fieldName, addCSVLogger(fieldName, new String[]{"Handle", "Value"}));
		}
	}
	
	/**
	 * Sets display threshold limit
	 * @param displayThresholdLimit display threshold limit 
	 */
	public void setDisplayThresholdLimit(long displayThresholdLimit) {
		this.displayThresholdLimit = displayThresholdLimit;
	}
	
	/**
	 * Adds filter to filter data. Filter(s) work as ORed,
	 * means any filter returns true then that row included.
	 * @param filter Filter logic
	 * @see Filter#filter(Object)
	 */
	public void addDataFilter(Filter<D> filter) {
		filters.add(filter);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void init(DataContainerNULLConfiguration<C> configuration) throws IOException {
		// super call
		super.init(configuration);
		// validation logger
		if(validationFlag) {
			schemaDetail = NDLDataValidationContext.getSchemaDetail();
			this.validator.addExcludeControlledFields(NDLDataValidationContext
					.getConfiguration("ndl.data.validation.exclude.controlled.fields").split(","));
			this.validator.setSchemaDetail(schemaDetail);
			this.validator.setLessLogger(addTextLogger(NDLDataUtils.LESS_VALIDATION_LOGGER));
			if(firstFailOnvalidation) {
				this.validator.turnOnFirstFailOnValidation();
			}
		}
		// custom initialization
		dataReader.init(configuration.getDataSourceConfig());
	}
	
	/**
	 * Returns true/false whether compression mode is on for reading
	 * @return if compression mode is ON then returns true otherwise false
	 */
	public boolean isCompressed() {
		return dataReader.isCompressed();
	}
	
	/**
	 * Closes opened resources, like logging files and input source if required
	 */
	@Override
	public void close() throws IOException {
		// super call
		super.close();
		// custom code
		dataReader.close();
	}
	
	/**
	 * Pre process data method to executes some pre-operations (some initializations if any)
	 * before starting actual process on whole data-set
	 * @throws Exception throws error in case of any error occurs during pre-operations 
	 */
	public void preProcessData() throws Exception {
		// blank implementation
	}
	
	/**
	 * Encapsulates post process of each skipped data-item if any.
	 * It's required typically when data is persisted after processing or any type of post processing required.
	 * @param skippedItem skipped item to do post processing if any
	 * @throws Exception throws exception if post processing fails
	 */
	public void postProcessSkippedItem(D skippedItem) throws Exception {
		// blank implementation
	}
	
	/**
	 * This process data encapsulates the logic of input data source reading/parsing and processing.
	 * After data processing closes the opened resources. 
	 */
	@Override
	final public void processData() throws Exception {
		
		System.out.println("Starts processing data.");
		
		turnOffGlobalLoggingFlag(); // turn off global logging flag
		long start = System.currentTimeMillis(); // start time
		try {
			init(containerConfiguration); // initialization
			preProcessData(); // pre-operations
			while(dataReader.hasNext()) {
				D item = dataReader.next();
				// pre-operation
				preProcessItem(item);
				// custom code
				boolean processed = filter(item);
				if(processed) {
					// filter pass
					processed = processItem(item);
				}
				if(processed) {
					// successfully processed
					postProcessItem(item);
				} else {
					// post skip item operation
					postProcessSkippedItem(item);
					// skipped
					skippedCounter++;
				}
				if(++processedCounter % displayThresholdLimit == 0) {
					// display processed item
					System.out.println("Processed: " + processedCounter + " items.");
					long intermediate = System.currentTimeMillis(); // intermediate time
					System.out.println(CommonUtilities.durationMessage(intermediate-start)); // duration message
					
					intermediateProcessHandler(); // let programmer to add custom handler
				}
			}
			intermediateProcessHandler(); // let programmer to add custom handler
			System.out.println("Total: " + processedCounter + " items processed.");
			System.out.println("Skipped: " + skippedCounter + " Post processed: "
					+ (processedCounter - skippedCounter));
			
			postProcessData(); // post-operations
		} finally {
			
			// validation messages
			if(validator != null && validator.errorsWarningsExists()) {
				System.err.println(validator.validationMessage());
			}
			
			close(); // close resources
		}
		
		long end = System.currentTimeMillis(); // end time
		System.out.println("Process completed."); // process complete
		System.out.println(CommonUtilities.durationMessage(end-start)); // duration message
	}
	
	/**
	 * This handler called when a data chunk has been processed
	 */
	protected void intermediateProcessHandler() {
		// blank
	}
	
	/**
	 * Total processed counter
	 * @return returns processed counter
	 */
	public long getProcessedCounter() {
		return processedCounter;
	}
	
	/**
	 * Gets skipped counter
	 * @return returns skipped counter
	 */
	public long getSkippedCounter() {
		return skippedCounter;
	}
	
	/**
	 * Returns effective counter, it differs from {@link #processedCounter} if filter logic applied
	 * @return returns effective counter
	 */
	public long getEffectiveCounter() {
		return processedCounter - skippedCounter;
	}
	
	/**
	 * This abstract method encapsulates each item processing logic
	 * @param item each data item read by system
	 * @return <pre>returns true data processing successfully done, if fails returns false</pre>
	 * Note: if any data item processing skipped then it should return false. It's required when filter logic is added.
	 * @throws Exception throws error if any error occurs during processing each item
	 */
	public abstract boolean processItem(D item) throws Exception;
	
	/**
	 * Encapsulates filter logic
	 * @param item item to check whether to filter or not
	 * @return returns true if data to be filtered otherwise false
	 * @throws Exception throws error if any error occurs during processing each item
	 */
	protected boolean filter(D item) throws Exception {
		// filter logic
		if(!filters.isEmpty()) {
			boolean skip = true;
			// apply filter logic
			for(Filter<D> filter : filters) {
				if(filter.filter(item)) {
					// if any filter returns true
					// then skip flag sets to false 
					skip = false;
					break;
				}
			}
			if(skip) {
				// skip data-row
				return false;
			}
		}
		return true; // default allow all
	}
	
	/**
	 * Post process data method to executes some post-operations (some destroy if any)
	 * after process on whole data-set
	 * @throws Exception throws error in case of any error occurs during post-operations 
	 */
	public void postProcessData() throws Exception {
		// blank implementation
	}
	
	/**
	 * Encapsulates pre-process of each data-item.
	 * @param item item to do pre-processing if any
	 * @throws Exception throws exception if pre-processing fails
	 */
	public void preProcessItem(D item) throws Exception {
		// blank implementation
	}

	/**
	 * Encapsulates post process of each data-item.
	 * It's required typically when data is persisted after processing or any type of post processing required.
	 * @param item item to do post processing if any
	 * @throws Exception throws exception if post processing fails
	 */
	public void postProcessItem(D item) throws Exception {
		// blank implementation
		// it should be overridden, when processed-item needs to persist
		// or any type of post processing required.
	}
}