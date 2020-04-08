package org.iitkgp.ndl.data.validator;

import java.util.Collection;

/**
 * Abstract NDL data validation controller.
 * @author Debasis
 */
public interface AbstractNDLDataValidationBox {
	
	/**
	 * Validates and logs validation message if any
	 * @param field NDL field for validation error
	 * @param values Values to validate
	 * @param handle current working handle ID
	 * @return returns returns false if any error/warning occurs otherwise true
	 * @throws Exception throws error if logging fails
	 */
	public boolean validate(String field, Collection<String> values, String handle) throws Exception;
}