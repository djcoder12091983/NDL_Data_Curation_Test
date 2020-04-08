package org.iitkgp.ndl.data.validator;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.iitkgp.ndl.validator.exception.NDLFieldNotFoundException;

// NDL schema response detail
class NDLSchemaResponseDetail {
	Map<String, NDLFieldDetail> general;
	
	public void setGeneral(Map<String, NDLFieldDetail> general) {
		this.general = general;
	}
}

/**
 * NDL abbreviated schema detail for all NDL fields
 * @see NDLFieldType 
 * @author Debasis
 */
public class NDLSchemaDetail {
	
	NDLSchemaResponseDetail response;
	Set<String> mandateFields = new HashSet<String>();
	Set<String> uniqueFields = new HashSet<String>();
	// all fields
	List<NDLFieldDetail> fieldDetails = new LinkedList<NDLFieldDetail>();
	Set<String> fields = null;
	// control fields
	Set<String> jsonKeyedFields = null; // NDL json-keyed fields
	Set<String> ctrlFields = null;
	
	/**
	 * Creates blank instance
	 * @return returns blank instance
	 */
	public static NDLSchemaDetail createBlankInstance() {
		NDLSchemaDetail detail = new NDLSchemaDetail();
		detail.fields = new HashSet<String>(2);
		detail.jsonKeyedFields = new HashSet<String>(2);
		detail.ctrlFields = new HashSet<String>(2);
		return detail;
	}
	
	/**
	 * This method loads fields set for unique and mandate fields
	 */
	public void loadMoreDetails() {
		jsonKeyedFields = new HashSet<String>();
		ctrlFields = new HashSet<String>();
		fields = new HashSet<String>();
		for(String key : response.general.keySet()) {
			NDLFieldDetail detail = getDetail(key);
			if(detail.isRequired()) {
				// mandate
				mandateFields.add(key);
			}
			if(detail.isUnique()) {
				// unique
				uniqueFields.add(key);
			}
			// add field
			fieldDetails.add(detail);
			fields.add(key);
			// some more details
			// control vocabulary
			if(isCtrlKey(key)) {
				jsonKeyedFields.add(key);
			} else if(isCtrl(key)) {
				ctrlFields.add(key);
			}
		}
	}
	
	/**
	 * JSON response available for schema detail
	 * @return returns true if so otherwise false
	 */
	public boolean available() {
		return response != null;
	}
	
	/**
	 * returns JSON response text
	 * @param response JSON response text
	 */
	public void setResponse(NDLSchemaResponseDetail response) {
		this.response = response;
	}
	
	/**
	 * Gets NDL field detail for a given NDL field
	 * @param field NDL field
	 * @return returns NDL field detail
	 */
	public NDLFieldDetail getDetail(String field) {
		return response.general.get(field);
	}
	
	/**
	 * Gets NDL mandate fields
	 * @return returns NDL mandate fields
	 */
	public Set<String> getMandateFields() {
		return mandateFields;
	}
	
	/**
	 * Gets unique NDL fields
	 * @return returns NDL unique fields
	 */
	public Set<String> getUniqueFields() {
		return uniqueFields;
	}
	
	/**
	 * Checks whether a field is multiple or not
	 * @param field field name
	 * @return returns true if so otherwise false
	 * @throws NDLFieldNotFoundException throws error when field name is not registered yet
	 */
	public boolean isMulti(String field) throws NDLFieldNotFoundException {
		NDLFieldDetail detail = getDetail(field);
		if(detail == null) {
			throw new NDLFieldNotFoundException("Field: " + field + " not found");
		}
		return detail.isMulti();
	}
	
	/**
	 * Checks whether a field is single valued or not
	 * @param field field name
	 * @return returns true if so otherwise false
	 * @throws NDLFieldNotFoundException throws error when field name is not registered yet
	 */
	public boolean isSingleValued(String field) throws NDLFieldNotFoundException {
		return !isMulti(field);
	}
	
	/**
	 * Checks whether a field is mandate or not
	 * @param field field name
	 * @return returns true if so otherwise false
	 * @throws NDLFieldNotFoundException throws error when field name is not registered yet
	 */
	public boolean isRequired(String field) throws NDLFieldNotFoundException {
		NDLFieldDetail detail = getDetail(field);
		if(detail == null) {
			throw new NDLFieldNotFoundException("Field: " + field + " not found");
		}
		return detail.isRequired();
	}
	
	/**
	 * Checks whether a field is unique across source or not
	 * @param field field name
	 * @return returns true if so otherwise false
	 * @throws NDLFieldNotFoundException throws error when field name is not registered yet
	 */
	public boolean isUnique(String field) throws NDLFieldNotFoundException {
		NDLFieldDetail detail = getDetail(field);
		if(detail == null) {
			throw new NDLFieldNotFoundException("Field: " + field + " not found");
		}
		return detail.isUnique();
	}
	
	/**
	 * Custom unique field user wants to check uniqueness
	 * @param field field name
	 */
	public void setUniqueField(String field) {
		if(!isUnique(field)) {
			// if not then set
			NDLFieldDetail detail = getDetail(field);
			detail.setUnique(true);
		}
	}
	
	/**
	 * Checks whether a field is controlled or not
	 * @param field field name
	 * @return returns true if so otherwise false
	 * @throws NDLFieldNotFoundException throws error when field name is not registered yet
	 */
	public boolean isCtrl(String field) throws NDLFieldNotFoundException {
		NDLFieldDetail detail = getDetail(field);
		if(detail == null) {
			throw new NDLFieldNotFoundException("Field: " + field + " not found");
		}
		return detail.type.equals(NDLFieldType.CTRL.getType());
	}
	
	/**
	 * Checks whether a field is boolean or not
	 * @param field field name
	 * @return returns true if so otherwise false
	 * @throws NDLFieldNotFoundException throws error when field name is not registered yet
	 */
	public boolean isBoolean(String field) {
		NDLFieldDetail detail = getDetail(field);
		if(detail == null) {
			throw new NDLFieldNotFoundException("Field: " + field + " not found");
		}
		return detail.type.equals(NDLFieldType.BOOL.getType());
	}
	
	/**
	 * Checks whether a field is control-keyed or not
	 * @param field field name
	 * @return returns true if so otherwise false
	 * @throws NDLFieldNotFoundException throws error when field name is not registered yet
	 */
	public boolean isCtrlKey(String field) throws NDLFieldNotFoundException {
		NDLFieldDetail detail = getDetail(field);
		if(detail == null) {
			throw new NDLFieldNotFoundException("Field: " + field + " not found");
		}
		return detail.type.equals(NDLFieldType.CTRL_KEY.getType());
	}
	
	/**
	 * Whether the fields exists in NDL Schema or not
	 * @param field field name to check
	 * @return returns true if so otherwise false
	 */
	public boolean containsField(String field) {
		return response.general.containsKey(field);
	}
	
	/**
	 * Gets NDL JSON-keyed fields
	 * @return returns NDL JSON-keyed fields
	 */
	public Set<String> getNDLJSONKeyedFields(){
		return jsonKeyedFields;
	}
	
	/**
	 * Gets NDL fields which managed by control vocabulary
	 * @return returns NDL fields which managed by control vocabulary
	 */
	public Set<String> getControlFields() {
		return ctrlFields;
	}
	
	/**
	 * Gets all field details
	 * @return returns field details
	 */
	public List<NDLFieldDetail> getFieldDetails() {
		return fieldDetails;
	}
	
	/**
	 * Returns whether field is valid field or not
	 * @param field field name to check
	 * @return returns true if valid, otherwise false 
	 */
	public  boolean isValidField(String field) {
		return fields.contains(field);
	}
}