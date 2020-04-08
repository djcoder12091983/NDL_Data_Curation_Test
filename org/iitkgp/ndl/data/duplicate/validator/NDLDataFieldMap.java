package org.iitkgp.ndl.data.duplicate.validator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.iitkgp.ndl.data.BaseNDLDataItem;
import org.iitkgp.ndl.data.Transformer;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * This field map encapsulates field wise details
 * @author debasis
 */
public class NDLDataFieldMap {
	
	// field data detail
	// map field details (value to handle_id mapping)
	Map<String, Map<String, List<String>>> map = new HashMap<String, Map<String, List<String>>>();
	// handle_id to data
	Map<String, Map<String, Collection<String>>> data = new HashMap<String, Map<String, Collection<String>>>();
	
	/**
	 * Adds field details for a particular item
	 * @param id id of the data item
	 * @param item particular data item
	 * @param fields given set of fields
	 * @param normalizers field specific normalizers if exists
	 */
	public void add(String id, BaseNDLDataItem item, List<String> fields,
			Map<String, Transformer<String, String>> normalizers) {
		id = NDLDataUtils.getHandleSuffixID(id);
		Map<String, Collection<String>> values = item.getAllValues();
		Map<String, Collection<String>> modified = new HashMap<String, Collection<String>>(2);
		for(String field : fields) {
			Collection<String> value = values.get(field);
			modified.put(field, value);
		}
		// handle_id to value details
		data.put(id, modified);
		// field value indexing
		for(String field : fields) {
			Transformer<String, String> normalizer = normalizers.get(field);
			Map<String, List<String>> index = map.get(field);
			if(index == null) {
				index = new HashMap<String, List<String>>(2);
				map.put(field, index);
			}
			Collection<String> value = values.get(field);
			for(String v : value) {
				Collection<String> nvalues = null;
				if(normalizer != null) {
					// normalization if needed
					Collection<String> tvalues = normalizer.transform(v);
					nvalues = new ArrayList<String>(tvalues.size());
					nvalues.addAll(tvalues);
				}
				for(String nvalue : nvalues) {
					List<String> ids = index.get(nvalue);
					if(ids == null) {
						ids = new ArrayList<String>(2);
						index.put(nvalue, ids);
					}
					ids.add(id);
				}
			}
		}
	}
	
	/**
	 * Checks whether given value contained in given field
	 * @param fieldName given field name
	 * @param value value to check for existence
	 * @return returns list of handles if found otherwise empty list
	 */
	public List<String> contains(String fieldName, String value) {
		if(!map.containsKey(fieldName)) {
			// field name not found
			return NDLDataUtils.createEmptyList();
		} else {
			Map<String, List<String>> detail = map.get(fieldName);
			if(detail.containsKey(value)) {
				// found
				return detail.get(value);
			} else {
				// otherwise empty list
				return NDLDataUtils.createEmptyList();
			}
		}
	}
}