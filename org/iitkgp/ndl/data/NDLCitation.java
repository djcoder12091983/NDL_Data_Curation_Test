package org.iitkgp.ndl.data;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * NDL citation detail
 * An example &#64;type{id,key={value1 and value2 .... }...}
 * To get string representation call {@link #toString()} or {@link #getCitationText()} method
 * @author Debasis
 */
public class NDLCitation {

	String type;
	String id;
	Map<String, Collection<String>> detail = new HashMap<String, Collection<String>>(2);
	
	/**
	 * Constructor
	 * @param type citation type
	 * @param id string id
	 */
	public NDLCitation(String type, String id) {
		this.type = type;
		this.id = id;
	}
	
	/**
	 * Adds detail with key and value, a key can have multiple associated values
	 * @param key key name
	 * @param value associated value
	 */
	public void addDetail(String key, String value) {
		if(StringUtils.isBlank(value)) {
			// avoid blank value
			return;
		}
		Collection<String> values = detail.get(key);
		if(values == null) {
			values = new LinkedList<String>();
			detail.put(key, values);
		}
		values.add(value);
	}
	
	/**
	 * Adds detail with key and associated values
	 * @param key key name
	 * @param values associated values
	 */
	public void addDetail(String key, String ... values) {
		for(String value : values) {
			addDetail(key, value);
		}
	}
	
	/**
	 * Adds detail with key and associated values
	 * @param key key name
	 * @param values associated values
	 */
	public void addDetail(String key, Collection<String> values) {
		for(String value : values) {
			addDetail(key, value);
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return getCitationText();
	}
	
	/**
	 * Gets citation text
	 * @return returns citation text
	 */
	public String getCitationText() {
		StringBuilder citation = new StringBuilder();
		citation.append('@').append(type).append('{');
		List<String> details = new LinkedList<String>();
		details.add(id);
		for(String key : detail.keySet()) {
			Collection<String> values = detail.get(key);
			details.add(key + "={" + NDLDataUtils.join(values, " and ") + "}");
		}
		citation.append(NDLDataUtils.join(details, ','));
		citation.append('}');
		
		return citation.toString();
	}
}