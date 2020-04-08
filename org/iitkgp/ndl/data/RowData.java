package org.iitkgp.ndl.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.exception.NDLMultivaluedException;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * Encapsulates row data with <b>Map&lt;String, Collection&lt;String&gt;&gt;</b> form,
 * key is column/attribute name and value is associated list of values
 * @author Debasis, Aurghya
 */
public class RowData implements BaseNDLDataItem {

	int rowIndex;
	String sourceName;
	// data-row
    Map<String, Collection<String>> data = new HashMap<String, Collection<String>>(2);
    
    /**
     * Checks whether key/header/column exists or not
     * @param key given key to check for existence
     * @return true if exists otherwise false
     * @see #headerExists(String)
     */
    public boolean containsKey(String key) {
    	return data.containsKey(key);
    }
    
    /**
     * Adds data with key and associated value, calling on same key values are added to associated list
     * @param key key
     * @param value value
     * @param allowBlankValue this flag indicates whether to allow blank value or not
     */
    void add(String key, String value, boolean allowBlankValue) {
    	if(!allowBlankValue && StringUtils.isBlank(value)) {
    		// skips blank data
    		return;
    	}
        Collection<String> values = data.get(key);
        if(values == null) {
            // new entry
            values = new LinkedList<String>();
            data.put(key, values);
        }
        
        values.add(value);
    }
    
    /**
     * Sets row detail
     * @param rowIndex row index (1 based)
     * @param sourceName source name from where data gets read
     */
    public void setRowDetail(int rowIndex, String sourceName) {
    	this.rowIndex = rowIndex;
    	this.sourceName = sourceName;
    }
    
    /**
     * Gets current row index
     * @return current row index
     */
    public int getRowIndex() {
		return rowIndex;
	}
    
    /**
     * Gets associated source name
     * @return returns associated source name
     */
    public String getSourceName() {
		return sourceName;
	}
    
    /**
     * Adds data with key and associated value, calling on same key values are added to associated list
     * @param key key
     * @param value value
     */
    public void add(String key, String value) {
    	add(key, value, false);
    }
    
    /**
     * Sets data for a given key, remove old existence
     * @param key given key
     * @param values given values
     */
    public void setData(String key, String ... values) {
    	if(values.length == 0) {
    		throw new IllegalArgumentException("Values must be at least one");
    	}
    	Collection<String> t = new LinkedList<String>();
    	for(String v : values) {
    		t.add(v);
    	}
    	data.put(key, t);
    }
    
    /**
     * Sets data for a given key, remove old existence
     * @param key given key
     * @param values given values
     */
    public void setData(String key, Collection<String> values) {
    	if(values.isEmpty()) {
    		throw new IllegalArgumentException("Values must be at least one");
    	}
    	data.put(key, values);
    }
    
    /**
     * Adds data with key and associated values, calling on same key values are added to associated list
     * @param key key
     * @param values values
     */
    public void addData(String key, String ... values) {
    	addData(key, false, values);
    }
    
    /**
     * Adds data with key and associated values, calling on same key values are added to associated list
     * @param key key
     * @param allowBlankValue this flag indicates whether to allow blank value or not
     * @param values values
     */
    public void addData(String key, boolean allowBlankValue, String ... values) {
    	if(values.length == 0) {
    		throw new IllegalArgumentException("Values must be at least one");
    	}
    	for(String value : values) {
    		// for each value
    		if(allowBlankValue || StringUtils.isNotBlank(value)) {
    			add(key, value, allowBlankValue);
    		}
    	}
    }
    
    /**
     * Adds data with key and associated collection values
     * @param key key
     * @param values associated values
     */
    public void addData(String key, Collection<String> values) {
    	addData(key, values, false);
    }
    
    /**
     * Adds data with key and associated collection values
     * @param key key
     * @param values associated values
     * @param allowBlankValue this flag indicates whether to allow blank value or not
     */
    public void addData(String key, Collection<String> values, boolean allowBlankValue) {
    	for(String value : values) {
    		// for each value
    		if(allowBlankValue || StringUtils.isNotBlank(value)) {
    			// only add non-blank value
    			addData(key, allowBlankValue, value);
    		}
    	}
    }
    
    /**
     * Gets list of data for a given key
     * @param key key
     * @return returns list of data, NULL if not found
     */
    public Collection<String> getData(String key) {
        return data.get(key);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getValue(String field) {
    	Collection<String> d = getData(field);
    	List<String> data = new ArrayList<String>(d.size()); 
    	data.addAll(d);
    	return data;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Collection<String>> getAllValues() {
    	return data;
    }
    
    /**
     * Gets single data value for a given key
     * @param key key
     * @return <pre>returns single data, empty string if not found.</pre>
     * <b>Note: For multiple values it returns first one.</b> 
     * @throws NDLMultivaluedException throws error when more than one values exist
     */
    public String getSingleData(String key) throws NDLMultivaluedException {
    	Collection<String> values = data.get(key);
    	if(values == null || values.isEmpty()) {
    		// value not found
    		return StringUtils.EMPTY;
    	} else {
    		// gets first value
    		if(values.size() > 1) {
    			throw new NDLMultivaluedException(key + " contains multiple values.");
    		}
    		return values.iterator().next();
    	}
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String getSingleValue(String field) throws NDLMultivaluedException {
    	return getSingleData(field);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean updateSingleValue(String field, String value) {
    	data.put(field, NDLDataUtils.createNewList(value));
    	return true; // updated successfully
    }
    
    /**
     * Gets entry attribute set details, see {@link Map#entrySet()} 
     * @return returns entry set details for all attributes
     */
    public Set<Entry<String, Collection<String>>> entrySet() {
    	return data.entrySet();
    }
    
    /**
     * gets all headers
     * @return returns all headers
     */
    public Set<String> getAllHeaders() {
    	return data.keySet();
    }
    
    /**
     * Returns whether header exists or not for a given header
     * @param header given header name
     * @return returns true if exists otherwise false
     */
    public boolean headerExists(String header) {
    	return data.containsKey(header);
    }
    
    /**
     * Add all data with key and associated list of values
     * @param data data details
     */
    public void addAllData(Map<String, Collection<String>> data) {
    	this.data.putAll(data);
    }
    
    /**
     * Add all data with key and associated list of values
     * @param data data details
     */
    public void addAllData(RowData data) {
    	this.data.putAll(data.data);
    }
    
    /**
     * Clears all details
     */
    public void clear() {
    	this.data.clear();
    }
    
    /**
     * Checks whether data set is empty
     * @return returns true if empty otherwise false
     */
    public boolean isEmpty() {
    	return this.data.isEmpty();
    }
}