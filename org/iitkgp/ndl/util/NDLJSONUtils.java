package org.iitkgp.ndl.util;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.json.NDLJSONParser;
import org.iitkgp.ndl.json.exception.InvalidJSONExpressionException;
import org.iitkgp.ndl.json.exception.JSONArrayIndexOutOfBoundException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * NDL JSON parsing utilities
 * @author Debasis
 */
public class NDLJSONUtils {
	
	/**
	 * evaluates json-path and returns value, NULL if invalid json-path provided
	 * @param expression expression to evaluate, see {@link NDLJSONParser}
	 * @param root root from where to start scanning
	 * @return returns found object if any, otehrwise NULL
	 * @throws InvalidJSONExpressionException throws error if expression is wrong formatted, see {@link NDLJSONParser}
	 */
	public static Object evaluate(String expression, Object root)
			throws InvalidJSONExpressionException {
		return evaluate(expression, root, true);
	}
	
	/**
	 * evaluates json-path and returns value, NULL if invalid json-path provided
	 * @param expression expression to evaluate, see {@link NDLJSONParser}
	 * @param root root from where to start scanning
	 * @param checkExists this flag is for checking existence
	 * @return returns found object if any, otehrwise NULL
	 * @throws InvalidJSONExpressionException throws error if expression is wrong formatted, see {@link NDLJSONParser}
	 */
	public static Object evaluate(String expression, Object root, boolean checkExists)
			throws InvalidJSONExpressionException {
		if(StringUtils.isBlank(expression)) {
			// blank expression
			return null;
		}
		String tokens[] = expression.split("\\.");
		Object jsonNode = null;
		int pathIndex = 0;
		if(isJsonArray(root)) {
			String detail[] = NDLDataUtils.getArrayIndex(tokens[pathIndex++]);
			if(detail == null) {
				// array type expected
				throw new InvalidJSONExpressionException("Array expected.");
			}
			JSONArray node = (JSONArray)root;
			int index = Integer.parseInt(detail[1]); // ignore variable name
			if(index >= node.size()) {
				// invalid index
				throw new JSONArrayIndexOutOfBoundException("JSON array index out of bound: " + index);
			}
			jsonNode = node.get(index);
		} else {
			jsonNode = root;
		}
		int l = tokens.length;
		for(int i = pathIndex; i < l; i++) {
			if(jsonNode == null) {
				// invalid expression
				throw new InvalidJSONExpressionException("Invalid expression: " + expression);
			}
			if(isJsonObject(jsonNode)) {
				// json object node
				JSONObject node = (JSONObject)jsonNode;
				String detail[] = NDLDataUtils.getArrayIndex(tokens[i]);
				Object targetNode = null;
				if(detail != null) {
					// expected array
					targetNode = node.get(detail[0]);
					if(!isJsonArray(targetNode)) {
						// array type expected
						throw new InvalidJSONExpressionException("Array(" + detail[0] + ") expected.");
					}
					int index = Integer.parseInt(detail[1]);
					if(index >= ((JSONArray)targetNode).size()) {
						// invalid index
						throw new JSONArrayIndexOutOfBoundException(
								"JSON array(" + detail[0] + ") index out of bound: " + index);
					}
					targetNode = ((JSONArray)targetNode).get(index);
				} else {
					targetNode = node.get(tokens[i]);
				}
				jsonNode = targetNode; // next node to process
				if(isItemObject(jsonNode) && i < l-1) {
					// invalid expression (still tokens to process but it's a leaf)
					throw new InvalidJSONExpressionException("Invalid expression: " + expression);
				}
			}
		}
		if(jsonNode == null && checkExists) {
			// not found yet
			throw new InvalidJSONExpressionException("Invalid expression: " + expression);
		}
		return jsonNode;
	}
	
	/**
	 * check whether object is JSON array
	 * @param object object to check
	 * @return returns true if array type otherwise false
	 */
	public static boolean isJsonArray(Object object) {
		if(object == null) {
			// does not exist
			return false;
		}
		// json aray 
		return object instanceof JSONArray;
	}
	
	/**
	 * check whether object is intermediate JSON object
	 * @param object object to check
	 * @return returns true if so otherwise false
	 */
	public static boolean isJsonObject(Object object) {
		if(object == null) {
			// does not exist
			return false;
		}
		// json object
		return object instanceof JSONObject;
	}
	
	/**
	 * check whether object is leaf/item JSON object
	 * @param object object to check
	 * @return returns true if so otherwise false
	 */
	public static boolean isItemObject(Object object) {
		if(object == null) {
			// does not exist
			return false;
		}
		// leaf
		return !isJsonArray(object) && !isJsonObject(object);
	}
	
	/**
	 * check whether object is JSON text object
	 * @param object object to check
	 * @return returns true if so otherwise false
	 */
	public static boolean isText(Object object) {
		if(object == null) {
			// does not exist
			return false;
		}
		return object instanceof String;
	}
	
	/**
	 * check whether object is JSON numeric object
	 * @param object object to check
	 * @return returns true if so otherwise false
	 */
	public static boolean isNumeric(Object object) {
		if(object == null) {
			// does not exist
			return false;
		}
		return object instanceof Number;
	}
	
	/**
	 * check whether object is intermediate JSON boolean object
	 * @param object object to check
	 * @return returns true if so otherwise false
	 */
	public static boolean isBoolean(Object object) {
		if(object == null) {
			// does not exist
			return false;
		}
		return object instanceof Boolean;
	}
	
	/**
	 * Checks whether json-path expression exists in loaded json 
	 * @param expression json-path expression
	 * @param root given root to start scanning
	 * @return returns true if found otherwise false
	 * @throws InvalidJSONExpressionException throws exception if expression is invalid, like non-array is accessed as array etc.
	 */
	public static boolean hasValue(String expression, Object root) throws InvalidJSONExpressionException {
		Object node = evaluate(expression, root, false);
		if(node == null) {
			// node does not exist
			return false;
		}
		return isItemObject(node);
	}
	
	/**
	 * Checks whether json-path expression is array or not 
	 * @param expression json-path expression
	 * @param root given root to start scanning
	 * @return returns true if found otherwise false
	 * @throws InvalidJSONExpressionException throws exception if expression is invalid, like non-array is accessed as array etc.
	 */
	public static boolean isJsonArray(String expression, Object root) throws InvalidJSONExpressionException {
		return isJsonArray(evaluate(expression, root));
	}
	
	/**
	 * Checks whether json-path expression is text or not 
	 * @param expression json-path expression
	 * @param root given root to start scanning
	 * @return returns true if found otherwise false
	 * @throws InvalidJSONExpressionException throws exception if expression is invalid, like non-array is accessed as array etc.
	 */
	public static boolean isText(String expression, Object root) throws InvalidJSONExpressionException {
		Object object = evaluate(expression, root);
		if(isItemObject(object)) {
			return isText(object);
		} else {
			return false;
		}
	}
	
	/**
	 * Checks whether json-path expression is numeric or not 
	 * @param expression json-path expression
	 * @param root given root to start scanning
	 * @return returns true if found otherwise false
	 * @throws InvalidJSONExpressionException throws exception if expression is invalid, like non-array is accessed as array etc.
	 */
	public static boolean isNumeric(String expression, Object root) throws InvalidJSONExpressionException {
		Object object = evaluate(expression, root);
		if(isItemObject(object)) {
			return isNumeric(object);
		} else {
			return false;
		}
	}
	
	/**
	 * gets array items by expression in loaded json object
	 * @param expression expression to load array items
	 * @param root given root to start scanning
	 * @return returns list of objects
	 * @throws InvalidJSONExpressionException throws exception if expression is invalid, like non-array is accessed as array etc.
	 */
	public static List<Object> getItems(String expression, Object root) throws InvalidJSONExpressionException {
		Object object = evaluate(expression, root);
		if(isJsonArray(object)) {
			JSONArray array = (JSONArray)object;
			return array.subList(0, array.size());
		} else {
			// not an array type
			throw new InvalidJSONExpressionException("Not an array type");
		}
	}
	
	/**
	 * gets array text values by expression in loaded json object
	 * @param expression expression to load array text values
	 * @param root given root to start scanning
	 * @return returns list of texts
	 * @throws InvalidJSONExpressionException throws exception if expression is invalid, like non-array is accessed as array etc.
	 */
	public static List<String> getTextValues(String expression, Object root) throws InvalidJSONExpressionException {
		Object object = evaluate(expression, root);
		if(isJsonArray(object)) {
			JSONArray array = (JSONArray)object;
			List<String> texts = new LinkedList<String>();
			for(Object o : array) {
				if(isText(object)) {
					texts.add(o.toString());
				}
			}
			return texts;
		} else {
			// not an array type
			throw new InvalidJSONExpressionException("Not an array type");
		}
	}
	
	/**
	 * gets array numeric values by expression in loaded json object
	 * @param expression expression to load array numeric values
	 * @param root given root to start scanning
	 * @return returns list of numbers
	 * @throws InvalidJSONExpressionException throws exception if expression is invalid, like non-array is accessed as array etc.
	 */
	public static List<Number> getNumericValues(String expression, Object root) throws InvalidJSONExpressionException {
		Object object = evaluate(expression, root);
		if(isJsonArray(object)) {
			JSONArray array = (JSONArray)object;
			List<Number> texts = new LinkedList<Number>();
			for(Object o : array) {
				if(isNumeric(object)) {
					texts.add((Number)o);
				}
			}
			return texts;
		} else {
			// not an array type
			throw new InvalidJSONExpressionException("Not an array type");
		}
	}
	
	/**
	 * gets text value by expression in loaded json object 
	 * @param expression expression to load value
	 * @param root given root to start scanning
	 * @return returns text value
	 * @throws InvalidJSONExpressionException throws exception if expression is invalid, like non-array is accessed as array etc.
	 */
	public static String getText(String expression, Object root) throws InvalidJSONExpressionException {
		Object object = evaluate(expression, root);
		if(isText(object)) {
			return object.toString();
		} else {
			return null;
		}
	}
	
	/**
	 * gets numeric value by expression in loaded json object 
	 * @param expression expression to load value
	 * @param root given root to start scanning
	 * @return returns numeric value
	 * @throws InvalidJSONExpressionException throws exception if expression is invalid, like non-array is accessed as array etc.
	 */
	public static Number getNumber(String expression, Object root) throws InvalidJSONExpressionException {
		Object object = evaluate(expression, root);
		if(isNumeric(object)) {
			return (Number)object;
		} else {
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
	public static Boolean getBoolean(String expression, Object root) throws InvalidJSONExpressionException {
		Object object = evaluate(expression, root);
		if(isBoolean(object)) {
			return (Boolean)object;
		} else {
			return null;
		}
	}
}