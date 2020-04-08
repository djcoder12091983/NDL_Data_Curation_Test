package org.iitkgp.ndl.data.hierarchy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Item level stitching requirements, which is used for stitching
 * @author Debasis
 */
public class NDLStitchingDetail {
	
	String handle;
	String title;
	String collection;
	// other properties
	Map<String, Collection<String>> others = new HashMap<String, Collection<String>>(2);
	// handle ID(s)
	List<String> ids;
	
	/**
	 * Constructor
	 * @param maxLevel maximum level in stitching
	 * @param handle item handle id
	 * @param title item title
	 * @param collection item collection
	 */
	public NDLStitchingDetail(int maxLevel, String handle, String title, String collection) {
		ids = new ArrayList<String>(maxLevel);
		this.handle = handle;
		this.title = title;
		this.collection = collection;
	}
	
	/**
	 * Adds other values
	 * @param key associated key
	 * @param values associated value
	 */
	public void addOther(String key, String ... values) {
		Collection<String> vals = others.get(key);
		if(vals == null) {
			vals = new LinkedList<String>();
			others.put(key, vals);
		}
		for(String value : values) {
			vals.add(value);
		}
	}
	
	/**
	 * Gets others first value by given key
	 * @param key given key
	 * @return returns associated value if present otherwise NULL
	 */
	public String getValue(String key) {
		Collection<String> vals = others.get(key);
		if(vals == null) {
			return null;
		} else {
			return vals.iterator().next();
		}
	}
	
	/**
	 * Gets others values by given key
	 * @param key given key
	 * @return returns associated values if present otherwise NULL
	 */
	public Collection<String> getValues(String key) {
		return others.get(key);
	}
	
	/**
	 * Gets item handle ID
	 * @return returns handle ID
	 */
	public String getHandle() {
		return handle;
	}
	
	/**
	 * Gets item collection name
	 * @return returns collection name
	 */
	public String getCollection() {
		return collection;
	}
	
	/**
	 * Gets item title
	 * @return returns title
	 */
	public String getTitle() {
		return title;
	}
	
	/**
	 * Adds stitching key details (virtual nodes)
	 * @param level this stitching level starts from 1, 2 .. so on
	 * @param handleId associated handle id
	 */
	public void addStitchingKey(int level, String handleId) {
		ids.add(level - 1, handleId);
	}
}