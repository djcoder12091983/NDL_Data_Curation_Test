package org.iitkgp.ndl.data.container;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.iitkgp.ndl.data.exception.ConfigurationMergeException;

/**
 * <pre>This configuration data is required to configure {@link ConfigurationPool}.</pre>
 * <pre>This class encapsulates each configuration detail by configuration logical name. For more details see table in database</pre>
 * See {@link AbstractDataContainer#addMappingResource(java.io.File, String)}
 * {@link AbstractDataContainer#addMappingResource(java.io.File, String, String)}
 * @author Debasis, Aurghya
 */
public class ConfigurationData {
	
	static final String DOT = "__DOT__";
	
	boolean ignoreCase = false; // case type of primary-key
	// column mapping
	Map<String, Integer> columnIndex = new HashMap<String, Integer>(2);
	// data detail
	Map<String, Map<Integer, String>> data = new HashMap<String, Map<Integer, String>>();
	
	/**
	 * Constructor
	 * @param columnIndex CSV/table column details
	 * @param ignoreCase this flag determines whether to ignore case or not for primary key
	 */
	public ConfigurationData(List<String> columnIndex, boolean ignoreCase) {
		int l = columnIndex.size();
		for(int i=0; i<l; i++) {
			this.columnIndex.put(columnIndex.get(i), i);
		}
		this.ignoreCase = ignoreCase;
	}
	
	/**
	 * Constructor
	 * @param columnIndex CSV/table column details 
	 */
	public ConfigurationData(List<String> columnIndex) {
		this(columnIndex, false);
	}
	
	/**
	 * Add all configuration data
	 * @param data all configuration data
	 */
	void merge(ConfigurationData data) {
		// cross check
		if(this.ignoreCase != data.ignoreCase) {
			// illegal state
			throw new ConfigurationMergeException("Configuration merging error: ignore-case mismatch");
		} else {
			// illegal state
			Set<String> columns = data.columnIndex.keySet();
			for(String c : this.columnIndex.keySet()) {
				if(!columns.contains(c)) {
					// columns mismatch
					throw new ConfigurationMergeException("Configuration merging error: columns mismatch");
				}
			}
		}
		// merge
		this.data.putAll(data.data);
	}
	
	/**
	 * Adds row detail for a given key
	 * @param key given key (primary key)
	 * @param rowdata row-data detail
	 */
	void add(String key, List<String> rowdata) {
		if(ignoreCase) {
			// normalize the key
			key = key.toLowerCase();
		}
		Map<Integer, String> values = data.get(key);
		if(values == null) {
			values = new HashMap<Integer, String>();
			data.put(key, values);
		}
		int l = rowdata.size();
		for(int i=0; i<l; i++) {
			values.put(i, rowdata.get(i));
		}
	}
	
	/**
	 * Extracts the value by a given key. This key is different from primary key.
	 * This key is given as form of <b>&lt;primary-key&gt;.</b>[&lt;column-name&gt;].
	 * See column name is optional, if only column is available then it's not required.
	 * <pre>If token contains then use {@link #escapeDot(String)}</pre>
	 * @param key given key
	 * @return returns value for given key, NULL if data not found
	 */
	String get(String key) {
		String tokens[] = key.split("\\.");
		String pk = denormalizeToken(tokens[0]);
		if(ignoreCase) {
			// normalize the PK
			pk = pk.toLowerCase();
		}
		Map<Integer, String> values = data.get(pk);
		if(values == null) {
			return null;
		} else {
			String subkey = null;
			if(tokens.length == 1) {
				// single value
				subkey = columnIndex.keySet().iterator().next();
			} else {
				subkey = tokens[1];
			}
			Integer index = columnIndex.get(denormalizeToken(subkey));
			if(index == null) {
				return null;
			} else {
				return values.get(index);
			}
		}
	}
	
	// columns key-set
	// internal usage
	Set<String> columnKeySet() {
		return columnIndex.keySet();
	}
	
	// gets column values for given key and multiple-value character separator
	Map<String, List<String>> getColumnValues(String key, char multivalueSeparator) {
		key = denormalizeToken(key);
		if(ignoreCase) {
			// normalize the PK
			key = key.toLowerCase();
		}
		Map<String, List<String>> all = new HashMap<String, List<String>>();
		Map<Integer, String> values = data.get(key);
		if(values == null) {
			return null;
		} else {
			Set<String> keys = columnIndex.keySet();
			for(String k : keys) {
				String value = values.get(columnIndex.get(k));
				StringTokenizer tokens = new StringTokenizer(value, String.valueOf(multivalueSeparator));
				List<String> list = new LinkedList<String>();
				while(tokens.hasMoreTokens()) {
					list.add(tokens.nextToken());
				}
				all.put(key, list);
			}
		}
		return all;
	}
	
	// checks whether given key exists or not
	boolean contains(String key) {
		return data.containsKey(denormalizeToken(key));
	}
	
	// checks whether given key and sub-key exists or not
	boolean contains(String key, String subkey) {
		key = denormalizeToken(key);
		subkey = denormalizeToken(subkey);
		if(data.containsKey(key)) {
			// checks sub-key exists
			return data.get(key).containsKey(columnIndex.get(subkey));
		} else {
			// sub-key not found
			return false;
		}
	}
	
	// normalize token
	/*static String normalizeToken(String token) {
		return token.replaceAll("\\\\.", DOT);
	}*/
	
	// denormalize individual token
	static String denormalizeToken(String token) {
		return token.replaceAll(DOT, ".");
	}
	
	/**
	 * Escapes dot if token contains dot because dot means special character for expression
	 * @param token token to escape
	 * @return returns escaped dot value
	 */
	public static String escapeDot(String token) {
		return token.replaceAll("\\.", DOT);
	}

}