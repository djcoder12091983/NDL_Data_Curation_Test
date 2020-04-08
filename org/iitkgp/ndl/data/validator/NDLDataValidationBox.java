package org.iitkgp.ndl.data.validator;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.context.NDLDataValidationContext;
import org.iitkgp.ndl.data.NDLField;
import org.iitkgp.ndl.data.container.AbstractDataContainer;
import org.iitkgp.ndl.data.log.CSVLogger;
import org.iitkgp.ndl.data.log.TextLogger;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.iitkgp.ndl.util.NDLDataValidationUtils;
import org.iitkgp.ndl.validator.exception.NDLDataValidationException;

/**
 * NDL data validation controller.
 * It offers control fields(including JSON key), data singularity etc. validations.
 * By default all validation flags are ON. To off call 'turnOffXXX' methods.
 * <pre>{@link #turnOnFirstFailOnValidation()} method has special flag to control process and logging</pre>
 * @author Debasis
 */
public class NDLDataValidationBox implements AbstractNDLDataValidationBox {
	
	// validation flags
	boolean controlFieldsValidationFlag = true;
	boolean jsonKeyValidationFlag = true;
	boolean singleValueValidationFlag = true;
	boolean detailValidationLoggingFlag = false;
	boolean firstFailOnvalidation = false;
	boolean showWarningMessage = true;
	boolean fieldWiseDetailValidationLoggingFlag = false;
	AbstractDataContainer<?> container = null; // associated container if any

	// multiple value separator
	char multipleValueSeparator = NDLConfigurationContext.getConfiguration("csv.data.write.multiple.value.separator")
			.charAt(0);
	
	// statistics
	long warningCounter = 0;
	long errorCounter = 0;
	
	// error controllers
	UniqueErrorTracker errorTracker = new UniqueErrorTracker();
	TextLogger detailLogger = null;
	TextLogger lessLogger = null;
	NDLSchemaDetail schemaDetail = null;
	
	// source specific simple validation rules
	Set<String> excludeControlledFields = new HashSet<String>(2);
	// TODO add more validation rules which will be particular context specific
	
	// field wise error/warning CSV generation
	Map<String, CSVLogger> fieldwiseLoggers = new HashMap<String, CSVLogger>(2);
	Set<String> excludeFieldwiseLoggers = new HashSet<String>();
	
	/**
	 * Sets associated container if any
	 * @param container associated container if any
	 */
	public void setContainer(AbstractDataContainer<?> container) {
		this.container = container;
	}
	
	/**
	 * Sets multiple value separator
	 * @param multipleValueSeparator multiple value separator
	 */
	public void setMultipleValueSeparator(char multipleValueSeparator) {
		this.multipleValueSeparator = multipleValueSeparator;
	}
	
	/**
	 * Turns off control fields validation flag
	 */
	public void turnOffControlFieldsValidationFlag() {
		controlFieldsValidationFlag = false;
	}
	
	/**
	 * Turns off JSON key validation flag
	 */
	public void turnOffJsonKeyValidationFlag() {
		jsonKeyValidationFlag = false;
	}
	
	/**
	 * Turns off data singularity validation flag
	 */
	public void turnOffSingleValueValidationFlag() {
		singleValueValidationFlag = false;
	}
	
	/**
	 * Turns on first fail validation flag, once it's called then container stops
	 *  if validation error occurs but continues on warning
	 */
	public void turnOnFirstFailOnValidation() {
		firstFailOnvalidation = true;
	}
	
	/**
	 * Turns on logging error/warning into CSV files
	 * <pre>
	 * If this flag is ON then all fields related logging happens.
	 * If {@link #addFieldwiseLogger(String, CSVLogger)} is called then this flag has no meaning
	 * </pre>
	 * @see #addFieldwiseLogger(String, CSVLogger)
	 */
	public void turnOnFieldWiseDetailValidation() {
		fieldWiseDetailValidationLoggingFlag = true;
	}
	
	/**
	 * Excludes field wise logging for a given filed name
	 * <pre>This API is valid if {@link #turnOnFieldWiseDetailValidation()} is called</pre>
	 * @param field given field name
	 */
	public void addExcludeFieldwiseLogger(String field) {
		excludeFieldwiseLoggers.add(field);
	}
	
	/**
	 * Adds fields wise logger
	 * <pre>If this method is called then this flag has no meaning</pre>
	 * @param fieldName given field name (xx.yy.zz etc.)
	 * @param logger corresponding logger
	 */
	public void addFieldwiseLogger(String fieldName, CSVLogger logger) {
		fieldwiseLoggers.put(fieldName, logger);
	}
	
	/**
	 * This flag stops showing warning messages
	 */
	public void dontShowWarnings() {
		showWarningMessage = false;
	}
	
	/**
	 * Adds to exclude controlled fields
	 * <pre>No JSON Key should be provided</pre>
	 * @param field field to exclude
	 */
	public void addExcludeControlledField(String field) {
		excludeControlledFields.add(field);
	}
	
	/**
	 * Adds to exclude controlled fields
	 * <pre>No JSON Key should be provided</pre>
	 * @param fields fields to exclude
	 */
	public void addExcludeControlledFields(String ...fields) {
		for(String field : fields) {
			excludeControlledFields.add(field);
		}
	}
	
	/**
	 * Adds to exclude controlled fields
	 * <pre>No JSON Key should be provided</pre>
	 * @param fields fields to exclude
	 */
	public void addExcludeControlledFields(Collection<String> fields) {
		for(String field : fields) {
			excludeControlledFields.add(field);
		}
	}
	
	/**
	 * Sets detail (handle id wise errors) text logger to put validation messages
	 * @param logger associated logger
	 */
	public void setDetailLogger(TextLogger logger) {
		this.detailLogger = logger;
	}
	
	/**
	 * Sets less (a brief) text logger to put validation messages
	 * @param lessLogger associated logger
	 */
	public void setLessLogger(TextLogger lessLogger) {
		this.lessLogger = lessLogger;
	}
	
	/**
	 * Sets schema detail
	 * @param schemaDetail schema detail
	 */
	public void setSchemaDetail(NDLSchemaDetail schemaDetail) {
		this.schemaDetail = schemaDetail;
	}
	
	/**
	 * Validates and logs validation message if any
	 * @param field NDL field for validation error
	 * @param values field values to validate
	 * @param handle handle id for current item (it's required to track validation error by handle ID)
	 * @return returns returns false if any error/warning occurs otherwise true
	 * @throws IOException throws error if logging fails
	 * @throws NDLDataValidationException throws error if 'firstFailOnvalidation' is ON and an error occurs
	 * @see #turnOnFirstFailOnValidation()
	 */
	public boolean validate(String field, Collection<String> values, String handle)
			throws IOException, NDLDataValidationException {
		if(schemaDetail == null) {
			// if schema detail not loaded then simply skip validation process
			return true;
		}
		ValidationStatus result = validate(field, values);
		boolean f = result.flag;
		if(!f) {
			// validation fails
			boolean error = result.error;
			String value = result.value;
			// put brief message to less logger
			String messaget = null;
			if(error || showWarningMessage) {
				// error/warning message show
				messaget = errorTracker.displayError(new NDLField(field), value, handle, lessLogger);
			}
			if(detailLogger != null && messaget != null) {
				// logger is set
				StringBuilder message = new StringBuilder();
				message.append(error ? "[ERROR]" : "[WARN]").append("@").append(handle).append(" ").append(messaget);
				detailLogger.log(message.toString());
			}
			// field wise logger
			if(fieldwiseLoggers.containsKey(field)) {
				// specific fields
				CSVLogger logger = fieldwiseLoggers.get(field);
				logger.log(new String[]{handle, value.substring(1, value.length() - 1)}); // handle id and it's value
			} else if(fieldWiseDetailValidationLoggingFlag) {
				// all fields
				if(!excludeFieldwiseLoggers.contains(field)) {
					// excluding specific fields (if any)
					if(container == null) {
						throw new IllegalStateException(
								"Associated container is not set, set before doing field wise error/warning logging");
					}
					CSVLogger logger = fieldwiseLoggers.get(field);
					if(logger == null) {
						// not yet
						logger = container.addCSVLogger(field, new String[]{"Handle", "Value"});
						fieldwiseLoggers.put(field, logger);
					}
					logger.log(new String[]{handle, value.substring(1, value.length() - 1)}); // handle id and it's value
				}
			}
			// statistics
			if(error) {
				errorCounter++;
			} else {
				warningCounter++;
			}
			if(error && firstFailOnvalidation) {
				// if on error process should stop
				throw new NDLDataValidationException("NDL data validation exception@" + handle + " " + messaget);
			}
		}
		return f;
	}
	
	// validation status
	class ValidationStatus {
		boolean flag = true;
		String value = null;
		boolean error = false;
		
		// validation status detail
		public ValidationStatus(boolean flag, String value, boolean error) {
			this.flag = flag;
			this.value = value;
			this.error = error;
		}
	}
	
	// gets error value
	String getErrorValue(Collection<String> wrongValues, boolean ctrlKey) {
		StringBuilder value = new StringBuilder();
		if(ctrlKey) {
			value.append("JsonKey@");
		}
		value.append("[").append(NDLDataUtils.join(wrongValues, multipleValueSeparator)).append("]");
		return value.toString();
	}
	
	/**
	 * Validates field with associated values
	 * @param field field name
	 * @param values associated values
	 * @return returns validation status (status true/false along with field value)
	 */
	ValidationStatus validate(String field, Collection<String> values) {
		if(schemaDetail == null) {
			throw new IllegalStateException("SchemaDetail not loaded, it's required to do validations");
		}
		if(!schemaDetail.isValidField(field)) {
			// wrong field
			return new ValidationStatus(false, null, true); // error flag TRUE
		}
		
		// validations
		boolean ctrl = schemaDetail.isCtrl(field);
		boolean ctrlKey = schemaDetail.isCtrlKey(field) && NDLDataValidationUtils.requiredJSONCheck(field);
		NDLFieldSchemaDetail detail = null;
		if(ctrl || ctrlKey) {
			detail = NDLDataValidationContext.getSchemaDetail(field);
		}
		boolean ctrlValidationRequired = ctrl && controlFieldsValidationFlag
				&& !excludeControlledFields.contains(field);
		Set<String> wrongValues = new HashSet<String>(2);
		for(String value : values) {
			String key = null;
			if(ctrlKey) {
				Map<String, String> map = NDLDataUtils.mapFromJson(value, true);
				if(!map.isEmpty()) {
					key = map.keySet().iterator().next();
				}
			}
			if(ctrlValidationRequired) {
				// controlled dictionary validation is allowed
				if(!detail.getControlledValues(field).contains(value)) {
					// wrong controlled value
					wrongValues.add(value);
				}
			} else if(schemaDetail.isBoolean(field)) {
				// boolean check
				if(!StringUtils.equals(value, "true") && !StringUtils.equals(value, "false")) {
					// invalid value
					wrongValues.add(value);
				}
			}
			if(jsonKeyValidationFlag && ctrlKey) {
				// controlled JSON key validation
				if(!detail.getControlledKeys(field).contains(key)) {
					// wrong controlled value
					wrongValues.add(key);
				}
			}
		}
		if(!wrongValues.isEmpty()) {
			return new ValidationStatus(false, getErrorValue(wrongValues, ctrlKey), false);
		}
		if(singleValueValidationFlag) {
			// data singularity validation
			if(schemaDetail.isSingleValued(field) && values.size() > 1) {
				// data singularity violation
				return new ValidationStatus(false, "Multivalued", true); // error flag TRUE
			}
		}
		
		// success
		return new ValidationStatus(true, null, false);
	}
	
	/**
	 * gets error counter
	 * @return returns error counter
	 */
	public long getErrorCounter() {
		return errorCounter;
	}
	
	/**
	 * gets warning counter
	 * @return returns warning counter
	 */
	public long getWarningCounter() {
		return warningCounter;
	}
	
	/**
	 * String representation, mainly accumulated/collaborative error messages
	 */
	@Override
	public String toString() {
		StringBuilder message = new StringBuilder();
		message.append("Data validation statistics.....").append(NDLDataUtils.NEW_LINE);
		message.append("Warning counter: ").append(warningCounter).append(NDLDataUtils.NEW_LINE);
		message.append("Error counter: ").append(errorCounter).append(NDLDataUtils.NEW_LINE);
		return message.toString();
	}
	
	/**
	 * Returns validation message
	 * @see #toString()
	 * @return returns string represenation
	 */
	public String validationMessage() {
		return toString();
	}
	
	/**
	 * Returns true whether errors/warnings exists
	 * @return Returns true whether errors/warnings exists otherwise false
	 */
	public boolean errorsWarningsExists() {
		return errorCounter > 0 || warningCounter > 0;
	}
}