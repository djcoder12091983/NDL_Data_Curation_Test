package org.iitkgp.ndl.json;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.iitkgp.ndl.json.exception.JSONArrayIndexOutOfBoundException;

// TODO currently no being used
public class NDLJSON {
	
	Map<String, NDLJSON> attributes = null;
	Object value;
	List<NDLJSON> items = null;
	
	public NDLJSON(Object value) {
		this.value = value;
	}
	
	public void addAttribute(String attribute, NDLJSON next) {
		if(attributes == null) {
			attributes = new HashMap<String, NDLJSON>(2);
		}
		attributes.put(attribute, next);
	}
	
	public void addAttribute(String attribute, String value) {
		if(attributes == null) {
			attributes = new HashMap<String, NDLJSON>(2);
		}
		attributes.put(attribute, new NDLJSON(value));
	}
	
	public void addAttribute(String attribute, Number value) {
		if(attributes == null) {
			attributes = new HashMap<String, NDLJSON>(2);
		}
		attributes.put(attribute, new NDLJSON(value));
	}
	
	public void addItem(NDLJSON item) {
		if(items == null) {
			items = new LinkedList<NDLJSON>();
		}
		items.add(item);
	}
	
	public void addAllItems(List<NDLJSON> items) {
		if(this.items == null) {
			this.items = new LinkedList<NDLJSON>();
		}
		this.items.addAll(items);
	}
	
	public boolean hasValue() {
		return value != null;
	}
	
	public boolean isArray() {
		return items != null;
	}
	
	public boolean hasAttributes() {
		return attributes != null;
	}
	
	public NDLJSON itemsAt(int index) throws JSONArrayIndexOutOfBoundException {
		if(isArray() && index < items.size()) {
			return items.get(index);
		} else {
			// error
			throw new JSONArrayIndexOutOfBoundException("Invalid index accessed/may be it's not an array");
		}
	}
	
	public boolean hasAttribute(String attribute) {
		return hasAttributes() && attributes.containsKey(attribute);
	}
	
	public NDLJSON getAttribute(String attribute) {
		if(hasAttributes()) {
			return attributes.get(attribute);
		} else {
			return null;
		}
	}
	
	public boolean isNumber() {
		return hasValue() && value instanceof Number;
	}
	
	public boolean isText() {
		return hasValue() && value instanceof String;
	}
	
	public boolean isBoolean() {
		return hasValue() && value instanceof Boolean;
	}
	
	public String getText() {
		if(isText()) {
			return value.toString();
		} else {
			return null;
		}
	}
	
	public Number getNumber() {
		if(isNumber()) {
			return (Number)value;
		} else {
			return null;
		}
	}
	
	public Boolean getBoolean() {
		if(isBoolean()) {
			return (Boolean)value;
		} else {
			return null;
		}
	}

}