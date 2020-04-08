package org.iitkgp.ndl.json;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.iitkgp.ndl.json.exception.InvalidJSONExpressionException;
import org.iitkgp.ndl.json.exception.JSONParsingException;
import org.iitkgp.ndl.util.NDLJSONUtils;
import org.json.simple.parser.JSONParser;

/**
 * Json parser and it facilitates accessing the data using some json-path (javascript object access mechanism)
 * @author Debasis, Vishal
 */
public class NDLJSONParser {
	
	// NDLJson root;
	Object jsonRoot;
	
	/**
	 * Loads json by input stream
	 * @param stream input stream
	 * @throws JSONParsingException throws exception if invalid json stream is provided
	 */
	public NDLJSONParser(InputStream stream) throws JSONParsingException {
		try {
			parse(new String(IOUtils.toByteArray(stream)));
		} catch(IOException ex) {
			// error
			throw new JSONParsingException(ex.getMessage(), ex.getCause());
		}
	}
	
	/**
	 * Loads json by input byte array
	 * @param json json byte array
	 * @throws JSONParsingException throws exception if invalid json byte array is provided
	 */
	public NDLJSONParser(byte[] json) throws JSONParsingException {
		parse(new String(json));
	}
	
	/**
	 * Loads json by input text
	 * @param json json text
	 * @throws JSONParsingException throws exception if invalid json text is provided
	 */
	public NDLJSONParser(String json) throws JSONParsingException {
		parse(json);
	}
	
	// JOSN parser
	void parse(String json) throws JSONParsingException {
		try {
			jsonRoot = new JSONParser().parse(json);
		} catch(Exception ex) {
			// error
			throw new JSONParsingException(ex.getMessage(), ex.getCause());
		}
	}
	
	/**
	 * Checks whether json-path expression exists in loaded json 
	 * @param expression json-path expression
	 * @return returns true if found otherwise false
	 * @throws InvalidJSONExpressionException throws exception if expression is invalid, like non-array is accessed as array etc.
	 */
	public boolean hasValue(String expression) throws InvalidJSONExpressionException {
		return NDLJSONUtils.hasValue(expression, jsonRoot);
	}
	
	/**
	 * Checks whether json-path expression is array or not 
	 * @param expression json-path expression
	 * @return returns true if found otherwise false
	 * @throws InvalidJSONExpressionException throws exception if expression is invalid, like non-array is accessed as array etc.
	 */
	public boolean isJsonArray(String expression) throws InvalidJSONExpressionException {
		return NDLJSONUtils.isJsonArray(expression, jsonRoot);
	}
	
	/**
	 * Checks whether json-path expression is text or not 
	 * @param expression json-path expression
	 * @return returns true if found otherwise false
	 * @throws InvalidJSONExpressionException throws exception if expression is invalid, like non-array is accessed as array etc.
	 */
	public boolean isText(String expression) throws InvalidJSONExpressionException {
		return NDLJSONUtils.isText(expression, jsonRoot);
	}
	
	/**
	 * Checks whether json-path expression is numeric or not 
	 * @param expression json-path expression
	 * @return returns true if found otherwise false
	 * @throws InvalidJSONExpressionException throws exception if expression is invalid, like non-array is accessed as array etc.
	 */
	public boolean isNumeric(String expression) throws InvalidJSONExpressionException {
		return NDLJSONUtils.isNumeric(expression, jsonRoot);
	}
	
	/**
	 * gets array items by expression in loaded json object
	 * @param expression expression to load array items
	 * @return returns list of objects
	 * @throws InvalidJSONExpressionException throws exception if expression is invalid, like non-array is accessed as array etc.
	 */
	public List<Object> getItems(String expression) throws InvalidJSONExpressionException {
		return NDLJSONUtils.getItems(expression, jsonRoot);
	}
	
	/**
	 * gets array text values by expression in loaded json object
	 * @param expression expression to load array text values
	 * @return returns list of texts
	 * @throws InvalidJSONExpressionException throws exception if expression is invalid, like non-array is accessed as array etc.
	 */
	public List<String> getTextValues(String expression) throws InvalidJSONExpressionException {
		return NDLJSONUtils.getTextValues(expression, jsonRoot);
	}
	
	/**
	 * gets array numeric values by expression in loaded json object
	 * @param expression expression to load array numeric values
	 * @return returns list of numbers
	 * @throws InvalidJSONExpressionException throws exception if expression is invalid, like non-array is accessed as array etc.
	 */
	public List<Number> getNumericValues(String expression) throws InvalidJSONExpressionException {
		return NDLJSONUtils.getNumericValues(expression, jsonRoot);
	}
	
	/**
	 * gets text value by expression in loaded json object 
	 * @param expression expression to load value
	 * @return returns text value
	 * @throws InvalidJSONExpressionException throws exception if expression is invalid, like non-array is accessed as array etc.
	 */
	public String getText(String expression) throws InvalidJSONExpressionException {
		return NDLJSONUtils.getText(expression, jsonRoot);
	}
	
	/**
	 * gets text value by expression in loaded json object 
	 * @param expression expression to load value
	 * @param alteValue if value does not exist then returns alternate value
	 * @return returns text value
	 */
	public String getText(String expression, String alteValue) throws InvalidJSONExpressionException {
		if(hasValue(expression)) {
			return NDLJSONUtils.getText(expression, jsonRoot);
		} else {
			return alteValue;
		}
	}
	
	/**
	 * gets numeric value by expression in loaded json object 
	 * @param expression expression to load value
	 * @return returns numeric value if found otherwise returns null
	 * @throws InvalidJSONExpressionException throws exception if expression is invalid, like non-array is accessed as array etc.
	 */
	public Number getNumber(String expression) throws InvalidJSONExpressionException {
		if(hasValue(expression)) {
			return NDLJSONUtils.getNumber(expression, jsonRoot);
		} else {
			// no value found
			return null;
		}
	}
	
	/**
	 * gets boolean value by expression in loaded json object 
	 * @param expression expression to load value
	 * @param root given root to start scanning
	 * @return returns boolean value
	 * @throws InvalidJSONExpressionException throws exception if expression is invalid, like non-array is accessed as array etc.
	 */
	public Boolean getBoolean(String expression, Object root) throws InvalidJSONExpressionException {
		return NDLJSONUtils.getBoolean(expression, jsonRoot);
	}
}