package org.iitkgp.ndl.data.validator;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.context.NDLDataValidationContext;
import org.iitkgp.ndl.core.NDLFieldTokenSplitter;
import org.iitkgp.ndl.core.NDLFieldTokenSplittingLoader;
import org.iitkgp.ndl.core.NDLMap;
import org.iitkgp.ndl.data.NDLDataItem;
import org.iitkgp.ndl.data.container.AbstractNDLDataContainer;
import org.iitkgp.ndl.data.container.DataContainerNULLConfiguration;
import org.iitkgp.ndl.data.correction.AbstractNDLDataCorrectionContainer;
import org.iitkgp.ndl.data.generation.AbstractCSVRawSourceDataGeneration;
import org.iitkgp.ndl.data.iterator.DataIterator;
import org.iitkgp.ndl.data.iterator.DataSourceNULLConfiguration;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.iitkgp.ndl.util.NDLDataValidationUtils;
import org.iitkgp.ndl.validator.exception.NDLSchemaDetailLoadException;

/**
 * <pre>This class responsible for validating generated data (SIP/AIP etc.) against some constraints.</pre>
 * The constraints are like,
 * <ul>
 * <li>invalid NDL fields</li>
 * <li>single/multiple value check</li>
 * <li>control vocabulary check</li>
 * <li>control keys check</li>
 * <li>unique fields check</li>
 * <li>JSON format error</li>
 * </ul>  etc.
 * Some configurations defined in <b>/conf/default.data.validation.conf.properties</b> file.
 * <pre>Note: Don't use following methods for validations, these have only effect on
 * {@link AbstractNDLDataCorrectionContainer} and {@link AbstractCSVRawSourceDataGeneration}</pre>
 * <ul>
 * <li>{@link AbstractNDLDataContainer#turnOffControlFieldsValidationFlag()}</li>
 * <li>{@link AbstractNDLDataContainer#turnOffFirstFailOnValidation()}</li>
 * <li>{@link AbstractNDLDataContainer#turnOffJsonKeyValidationFlag()}</li>
 * <li>{@link AbstractNDLDataContainer#addExcludeControlledField(String)}</li>
 * <li>{@link AbstractNDLDataContainer#addExcludeControlledFields(Collection)}</li>
 * <li>{@link AbstractNDLDataContainer#addExcludeControlledFields(String...)}</li>
 * </ul>
 * @param <D> target data item (SIP/AIP etc.)
 * @param <R> target data reader (SIP/AIP etc.)
 * @see NDLFieldType
 * @see NDLFieldDetail
 * @see NDLFieldSchemaDetail
 * @see NDLSchemaDetail
 * @author Debasis
 */
public class AbstractNDLDataValidator<D extends NDLDataItem, R extends DataIterator<D, DataSourceNULLConfiguration>>
		extends AbstractNDLDataContainer<D, R, DataSourceNULLConfiguration> {
	
	// error loggers
	static String GENERAL_ERROR_LOGGER = "global.general.validation.error.log";
	static String UNIQUE_FIELD_ERROR_LOGGER = "unique.field.validation.error.log";
	static String MULTIVLAUE_ERROR_LOGGER = "multi.value.validation.error.log";
	static String REQUIRED_FIELD_ERROR_LOGGER = "mandate.field.validation.error.log";
	static String CTRL_VOCAB_ERROR_LOGGER = "control.vocabulary.validation.error.log";
	
	// counter variables
	long errorCounter = 0, warnCounter = 0, errorItemsCounter = 0;
	
	// unique field tracking
	Map<String, NDLMap<List<String>>> uniqueFieldTracker = new HashMap<String, NDLMap<List<String>>>(2);
	Set<String> excludeUniqueFields = new HashSet<String>(2);
	Set<String> excludeFields = new HashSet<String>(2);
	Set<String> handleIDTracker = new HashSet<String>(); // handle ID tracker to avoid duplicate
	NDLFieldTokenSplittingLoader tokenSplittingLoader = new NDLFieldTokenSplittingLoader();
	boolean excludeAllUniqueFieldValidationFlag = false;
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @throws NDLSchemaDetailLoadException throws exception when constrains loading error occurs
	 */
	public AbstractNDLDataValidator(String input, String logLocation) throws NDLSchemaDetailLoadException {
		super(input, logLocation, false);
		turnOffGlobalLoggingFlag();
		schemaDetail = NDLDataValidationContext.getSchemaDetail();
		// load details
		loadLoggers();
	}
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param containerConfiguration configuration to initialize
	 * @throws NDLSchemaDetailLoadException throws exception when constrains loading error occurs
	 */
	public AbstractNDLDataValidator(String input, String logLocation,
			DataContainerNULLConfiguration<DataSourceNULLConfiguration> containerConfiguration)
			throws NDLSchemaDetailLoadException {
		super(input, logLocation, containerConfiguration, false);
		turnOffGlobalLoggingFlag();
		schemaDetail = NDLDataValidationContext.getSchemaDetail();
		// load details
		loadLoggers();
	}
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param name logical name to add prefix to log files 
	 * @throws NDLSchemaDetailLoadException throws exception when constrains loading error occurs
	 */
	public AbstractNDLDataValidator(String input, String logLocation, String name) throws NDLSchemaDetailLoadException {
		super(input, logLocation, name, false);
		turnOffGlobalLoggingFlag();
		schemaDetail = NDLDataValidationContext.getSchemaDetail();
		// load details
		loadLoggers();
	}
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param name logical name to add prefix to log files
	 * @param containerConfiguration configuration to initialize
	 * @throws NDLSchemaDetailLoadException throws exception when constrains loading error occurs
	 */
	public AbstractNDLDataValidator(String input, String logLocation, String name,
			DataContainerNULLConfiguration<DataSourceNULLConfiguration> containerConfiguration)
			throws NDLSchemaDetailLoadException {
		super(input, logLocation, name, containerConfiguration, false);
		turnOffGlobalLoggingFlag();
		schemaDetail = NDLDataValidationContext.getSchemaDetail();
		// load details
		loadLoggers();
	}
	
	/**
	 * Initializes validation container
	 */
	@Override
	public void init(DataContainerNULLConfiguration<DataSourceNULLConfiguration> configuration) throws IOException {
		// super call
		super.init(configuration);
		// if want to change the configuration list then see "/conf/default.data.validation.conf.properties"
		// stop unique fields check
		excludeUniqueFields(NDLDataValidationUtils.EXCLUDE_UNIQUE_FIELDS_4_CHECK);
		// exclude fields check
		excludeFields(NDLDataValidationUtils.EXCLUDE_FIELDS_4_CHECK);
	}
	
	/**
	 * Adds field name to exclude unique field list 
	 * @param field field to exclude
	 */
	public void excludeUniqueField(String field) {
		excludeUniqueFields.add(field);
	}
	
	/**
	 * Adds fields to exclude unique field list 
	 * @param fields fields to exclude
	 */
	public void excludeUniqueFields(Collection<String> fields) {
		excludeUniqueFields.addAll(fields);
	}
	
	/**
	 * Exclude field for validation
	 * @param field field to exclude
	 */
	public void excludeField(String field) {
		excludeFields.add(field);
	}
	
	/**
	 * Exclude fields for validation
	 * @param fields fields to exclude
	 */
	public void excludeFields(Collection<String> fields) {
		excludeFields.addAll(fields);
	}
	
	/**
	 * This indicates all unique fields validation stop
	 */
	public void turnOffUniqueFieldValidations() {
		excludeAllUniqueFieldValidationFlag = true;
	}
	
	// load loggers for validation report  generation
	void loadLoggers() throws NDLSchemaDetailLoadException {
		try {
			// add loggers
			addTextLogger(GENERAL_ERROR_LOGGER);
			addTextLogger(UNIQUE_FIELD_ERROR_LOGGER);
			addTextLogger(MULTIVLAUE_ERROR_LOGGER);
			addTextLogger(REQUIRED_FIELD_ERROR_LOGGER);
			addTextLogger(CTRL_VOCAB_ERROR_LOGGER);
		} catch(IOException ex) {
			// error
			throw new NDLSchemaDetailLoadException(ex.getMessage());
		}
	}
	
	// logging errors 
	void logMultiValueError(String error) throws IOException {
		log(MULTIVLAUE_ERROR_LOGGER, error);
	}
	
	void logUniqueFieldError(String error) throws IOException {
		log(UNIQUE_FIELD_ERROR_LOGGER, error);
	}
	
	void logGeneralError(String error) throws IOException {
		log(GENERAL_ERROR_LOGGER, error);
	}
	
	void logRequiredFieldError(String error) throws IOException {
		log(REQUIRED_FIELD_ERROR_LOGGER, error);
	}
	
	void logCtrlVocabError(String error) throws IOException {
		log(CTRL_VOCAB_ERROR_LOGGER, error);
	}
	
	/**
	 * validates each item
	 */
	@Override
	public boolean processItem(NDLDataItem item) throws Exception {
		// each item validation
		boolean flag = validate(item);
		if(flag) {
			errorItemsCounter++;
		}
		return flag;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void postProcessData() throws Exception {
		// super call
		super.postProcessData();
		// now write unique fields error (if any)
		Set<String> uniqueFields = uniqueFieldTracker.keySet();
		for(String uniqueField : uniqueFields) {
			// for each unique field
			NDLMap<List<String>> fieldDetail = uniqueFieldTracker.get(uniqueField);
			// multiple entry handle for unique fields
			NDLUniqueFieldDuplicateEntryHandler handler = new NDLUniqueFieldDuplicateEntryHandler(uniqueField);
			fieldDetail.setKeyProcessor(handler);
			// collect multiple values
			fieldDetail.iterate();
			List<List<String>> duplicateValues = handler.getDuplicateValues();
			if(!duplicateValues.isEmpty()) {
				// duplicate entry exists
				warnCounter++;
				long count = 0;
				for(List<String> handles: duplicateValues) {
					count += handles.size();
					String values = NDLDataUtils.join(handles, ' ');
					logUniqueFieldError(values);
				}
				String message = "[WARN] Duplicate values exists for unique field: " + uniqueField + " (" + count + ")";
				System.err.println(message);
				logUniqueFieldError(message);
				logUniqueFieldError(""); // separator
			}
		}
		// report
		System.out.println("Total error counter: " + errorCounter + ", Warning counter: " + warnCounter);
		System.out.println("Total error items: " + errorCounter);
	}
	
	/**
	 * Validates whole source
	 * @throws Exception throws Exception in case any validation process throws error
	 */
	public void validate() throws Exception {
		// whole data processing
		processData();
	}
	
	/**
	 * Custom unique field user wants to check uniqueness
	 * @param field field name
	 */
	public void setUniqueField(String field) {
		schemaDetail.setUniqueField(field);
	}
	
	// validate each item
	boolean validate(NDLDataItem item) throws IOException {
		// item validation code, unique tracking logic
		String handle = item.getId();
		if(handleIDTracker.contains(handle)) {
			// duplicate handle ID
			String message = "[WARN] Duplicate handle ID: " + handle;
			System.err.println(message);
			logGeneralError(message);
		} else {
			// adds handle ID
			handleIDTracker.add(handle);
		}
		Map<String, Collection<String>> values = item.getAllValues(excludeFields);
		Set<String> availableFields = values.keySet();
		boolean error = false;
		for(String field : availableFields) {
			// iterate all fields
			if(!schemaDetail.containsField(field)) {
				// invalid field
				logGeneralError("[ERROR][" + handle + "] field: " + field + " is not registered.");
				errorCounter++;
				error = true;
			}
			
			Collection<String> fieldValues = values.get(field);
			// handle some general errors and CTRL/JSON related errors
			if(handleSomeErrors(field, fieldValues, handle)) {
				error = true;
			}
			
			// multi value check
			if(!schemaDetail.isMulti(field) && fieldValues.size() > 1) {
				// violates (single value field contains multiple values)
				logMultiValueError("[ERROR][" + handle + "] Field: " + field
						+ " is single valued but it contains multiple values.");
				errorCounter++;
				error = true;
			}
			if (!excludeAllUniqueFieldValidationFlag && schemaDetail.isUnique(field)
					&& !excludeUniqueFields.contains(field)) {
				// unique field tracking
				handleUniqueField(field, fieldValues, handle);
			}
		}
		// mandate field check
		for(String field : schemaDetail.getMandateFields()) {
			if(!availableFields.contains(field)) {
				// missing
				logRequiredFieldError(
						"[ERROR][" + handle + "] Field : " + field + " is mandatory but missing.");
				errorCounter++;
				error = true;
			}
		}
		return error;
	}
	
	// handle some general errors and CTRL/JSON related errors
	boolean handleSomeErrors(String field, Collection<String> fieldValues, String handle) throws IOException {
		boolean ctrl = schemaDetail.isCtrl(field);
		boolean ctrlKey = schemaDetail.isCtrlKey(field);
		NDLFieldSchemaDetail moreSchemaDetail = null;
		boolean error = false;
		if(ctrl || ctrlKey) {
			// load individual schema detail (on demand)
			moreSchemaDetail = NDLDataValidationContext.getSchemaDetail(field);
		}
		Set<String> duplicateTrack = new HashSet<String>();
		for(String fieldValue : fieldValues) {
			// iterate values
			if(StringUtils.isNotBlank(fieldValue)) {
				// valid value
				String value = null;
				 if(ctrlKey && NDLDataValidationUtils.requiredJSONCheck(field)) {
					 // JSON (CTRL key), JSON check is enabled
					 try {
						Map<String, String> map = NDLDataUtils.mapFromJson(fieldValue);
						String jsonKey = map.keySet().iterator().next();
						if(!moreSchemaDetail.getControlledKeys(field).contains(jsonKey)) {
							// JOSN key missing
							logCtrlVocabError("[WARN][" + handle + "] field: " + field + " JSON key: " + jsonKey
									+ " is not registered.");
							warnCounter++;
						}
						String jsonKeyValue = map.get(jsonKey);
						value = jsonKey + ":" + jsonKeyValue;
					 } catch(Exception ex) {
						 // error (JSON parsing)
						 String message = "[ERROR][" + handle + "] field: " + field + " JSON error: " + fieldValue;
						 System.err.println(message);
						 logGeneralError(message);
						 errorCounter++;
						 error = true;
					 }
				 } else if(ctrl) {
					 // CTRL value
					 value = fieldValue;
					 if(!moreSchemaDetail.getControlledValues(field).contains(value)) {
						 // control value missing
						 logCtrlVocabError(
								"[WARN][" + handle + "] field: " + field + " value: " + value + " is not controlled.");
						 warnCounter++;
					 }
				 }
				 if(StringUtils.isNotBlank(value)) {
					 // value will be blank if JSON parsing error happens
					 boolean append = duplicateTrack.add(value);
					 if(!append) {
						 // duplicate value
						 logGeneralError("[WARN][" + handle + "] field: " + field + " duplicate value: " + value);
						 warnCounter++;
					 }
				 }
			} else {
				// blank value
				logGeneralError("[WARN][" + handle + "] field: " + field + " blank value.");
				warnCounter++;
			}
		}
		return error;
	}
	
	// unique field tracking
	void handleUniqueField(String field, Collection<String> fieldValues, String handle) {
		NDLMap<List<String>> uniqueFieldDetail = uniqueFieldTracker.get(field);
		if(uniqueFieldDetail == null) {
			uniqueFieldDetail = new NDLMap<List<String>>();
			uniqueFieldTracker.put(field, uniqueFieldDetail);
		}
		NDLFieldTokenSplitter<String, String[]> splitter = tokenSplittingLoader.loadTokenSplitter(field);
		for(String fieldValue : fieldValues) {
			// fir all values iterate
			String splittingTokens[] = null;
			if(splitter == null) {
				// default splitter (splitted by space)
				splittingTokens = fieldValue.split("( | )+");
			} else {
				// custom splitting logic
				splittingTokens = splitter.split(fieldValue);
			}
			// keep on adding handle ID for duplicate values for unique fields
			List<String> handles = uniqueFieldDetail.get(splittingTokens);
			if(handles == null) {
				// first time
				handles = new LinkedList<String>();
				uniqueFieldDetail.add(splittingTokens, handles);
			}
			handles.add(handle);
		}
	}
}