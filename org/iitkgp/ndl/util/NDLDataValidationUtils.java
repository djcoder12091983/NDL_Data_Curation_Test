package org.iitkgp.ndl.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.context.DefaultNDLDataValidationContextLoader;
import org.iitkgp.ndl.context.NDLDataValidationContext;
import org.iitkgp.ndl.data.validator.AbstractNDLDataValidator;
import org.iitkgp.ndl.data.validator.NDLFieldSchemaDetail;
import org.iitkgp.ndl.data.validator.NDLSchemaDetail;
import org.iitkgp.ndl.validator.exception.NDLSchemaDetailLoadException;

/**
 * <pre>NDL data validation related utilities.</pre>
 * <pre>Some configurations defined in <b>/conf/default.data.validation.conf.properties</b></pre>
 * See {@link AbstractNDLDataValidator}
 * @author Debasis
 */
public class NDLDataValidationUtils {
	
	/**
	 * Only JSON format check fields, example ndl.sourceMeta.uniqueInfo, dc.relation.haspart
	 * but dc.identifier.other needs key-value pair check
	 */
	public static Set<String> PLAIN_JSON_CHECK_FIELDS = new HashSet<String>(2);
	/**
	 * Unique fields check not required field list
	 */
	public static Set<String> EXCLUDE_UNIQUE_FIELDS_4_CHECK = new HashSet<String>(2);
	/**
	 * Excludes for field validation
	 */
	public static Set<String> EXCLUDE_FIELDS_4_CHECK = new HashSet<String>(2);
	
	static {
		// context startup
		NDLDataValidationContext.init();
		
		// load context configuration
		loadConfiguration();
	}
	
	/**
	 * resets and reloads configuration from context
	 */
	public synchronized static void reset() {
		// resets
		PLAIN_JSON_CHECK_FIELDS.clear();
		EXCLUDE_UNIQUE_FIELDS_4_CHECK.clear();
		EXCLUDE_FIELDS_4_CHECK.clear();
		
		// reloads
		loadConfiguration();
	}
	
	/**
	 * Loads validation context configuration
	 */
	public static void loadConfiguration() {
		// load
		String value = NDLDataValidationContext.getConfiguration("ndl.data.validation.plain.JSON.chek");
		if(StringUtils.isNotBlank(value)) {
			PLAIN_JSON_CHECK_FIELDS.addAll(Arrays.asList(value.split("\\|")));
		}
		value = NDLDataValidationContext.getConfiguration("ndl.data.validation.exclude.unique.fields");
		if(StringUtils.isNotBlank(value)) {
			EXCLUDE_UNIQUE_FIELDS_4_CHECK.addAll(Arrays.asList(value.split("\\|")));
		}
		value = NDLDataValidationContext.getConfiguration("ndl.data.validation.load.exclude.fields");
		if(StringUtils.isNotBlank(value)) {
			EXCLUDE_FIELDS_4_CHECK.addAll(Arrays.asList(value.split("\\|")));
		}
	}
	
	/**
	 * Loads NDL schema constraint details
	 * @return returns schema detail
	 * @throws NDLSchemaDetailLoadException throws error if schema detail loading error occurs
	 * @see NDLSchemaDetail
	 * @see NDLFieldSchemaDetail
	 * @deprecated move to {@link DefaultNDLDataValidationContextLoader}
	 */
	public static NDLSchemaDetail loadSchemaDetail() throws NDLSchemaDetailLoadException {
		// move to 'DefaultNDLDataValidationContextLoader'
		throw new UnsupportedOperationException("Use instead DefaultNDLDataValidationContextLoader methods.");
	}
	
	/**
	 * Loads individual NDL schema constraint in details for a given field
	 * @param field given NDL field
	 * @return returns NDL field wise schema detail
	 * @throws NDLSchemaDetailLoadException throws error if schema field more detail loading error occurs
	 * @deprecated move to {@link DefaultNDLDataValidationContextLoader}
	 */
	public static NDLFieldSchemaDetail loadSchemaDetail(String field) throws NDLSchemaDetailLoadException {
		// move to 'DefaultNDLDataValidationContextLoader'
		throw new UnsupportedOperationException("Use instead DefaultNDLDataValidationContextLoader methods.");
	}
	
	/**
	 * validates existence field
	 * @param field field to check
	 * @return returns validation status
	 */
	public static boolean validate(String field) {
		return NDLDataValidationContext.isValidField(field);
	}
	
	/**
	 * validates controlled field (controlled value/key controlled keyed)
	 * @param field field to check
	 * @param checkpoint checkpoint (value/key) to check
	 * @return returns validation status 
	 */
	public static boolean validate(String field, String checkpoint) {
		return NDLDataValidationContext.isValidControlledFieldValue(field, checkpoint);
	}
	
	/**
	 * Returns whether a field requires JSON check
	 * @param field field to check
	 * @return returns true if required otherwise false
	 */
	public static boolean requiredJSONCheck(String field) {
		return !PLAIN_JSON_CHECK_FIELDS.contains(field);
	}
}