package org.iitkgp.ndl.context;

import org.iitkgp.ndl.data.validator.NDLFieldSchemaDetail;
import org.iitkgp.ndl.data.validator.NDLSchemaDetail;
import org.iitkgp.ndl.validator.exception.NDLSchemaDetailLoadException;

/**
 * NDL data validation context loader specification
 * It's required when custom validation context loader comes into scenario
 * @author Debasis
 */
public interface NDLDataValidationContextLoader {
	
	
	/**
	 * Loads NDL schema constraint details
	 * @return returns schema detail
	 * @throws NDLSchemaDetailLoadException throws error if schema detail loading error occurs
	 * @see NDLSchemaDetail
	 * @see NDLFieldSchemaDetail
	 */
	NDLSchemaDetail loadSchemaDetail() throws NDLSchemaDetailLoadException;
	
	/**
	 * Loads individual NDL schema constraint in details for a given field
	 * @param field given NDL field
	 * @return returns NDL field wise schema detail
	 * @throws NDLSchemaDetailLoadException throws error if schema field more detail loading error occurs
	 */
	NDLFieldSchemaDetail loadSchemaDetail(String field) throws NDLSchemaDetailLoadException;
}