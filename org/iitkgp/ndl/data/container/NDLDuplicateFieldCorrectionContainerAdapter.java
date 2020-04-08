package org.iitkgp.ndl.data.container;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.BaseNDLDataItem;
import org.iitkgp.ndl.data.NDLDataPair;
import org.iitkgp.ndl.data.Try;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * This class eases to configure duplicate field handle
 * @param D Data to be corrected in terms of duplicates
 * @see NDLDuplicateFieldCorrectionContainer
 * @author Debasis
 */
public class NDLDuplicateFieldCorrectionContainerAdapter<D extends BaseNDLDataItem> {
	
	NDLDuplicateFieldCorrectionContainer<D> duplicateContainer = null;
	
	// internal usage
	void construct(String field, char braceStart, char braceEnd, NDLDataPair<String> ... pairs) {
		duplicateContainer = new NDLDuplicateFieldCorrectionContainer<D>(field);
		int l = pairs.length;
		for(int i = 0; i < l; i++) {
			// define each trial logic
			final int counter = i;
			duplicateContainer.addTrial(new Try<D, String>() {
				@Override
				public String trial(D input) throws Exception {
					// trial logic
					StringBuilder newvalue = new StringBuilder(input.getSingleValue(field));
					List<String> values = new LinkedList<String>();
					for(int j = 0; j <= counter; j++) {
						NDLDataPair<String> pair = pairs[j];
						String value = input.getSingleValue(pair.first());
						if(StringUtils.isNotBlank(value)) {
							String name = pair.second();
							values.add(StringUtils.isNotBlank(name) ? (name + ": ") : "" + value);
						}
					}
					String suffix = NDLDataUtils.join(values, ',');
					if(StringUtils.isNotBlank(suffix)) {
						newvalue.append(braceStart).append(suffix).append(braceEnd);
					}
					return newvalue.toString();
				}
			});
		}
	}
	
	/**
	 * Constructor
	 * @param field which field to handle duplicates
	 * @param braceStart this field determines which character wraps the suffix part, default value is '('
	 * @param braceEnd this field determines which character wraps the suffix part, default value is ')'
	 * @param pairs field configuration by which duplicates to be fixed, fields must be single valued
	 *              each pair contains field name and display name, if display name not required then leave it blank
	 */
	public NDLDuplicateFieldCorrectionContainerAdapter(String field, char braceStart, char braceEnd, NDLDataPair<String> ... pairs) {
		construct(field, braceStart, braceEnd, pairs);
	}
	
	/**
	 * Constructor
	 * @param field which field to handle duplicates
	 * @param pairs field configuration by which duplicates to be fixed, fields must be single valued
	 *              each pair contains field name and display name, if display name not required then leave it blank
	 * @see #NDLDuplicateFieldCorrectionContainerAdapter(String, char, char, NDLDataPair...)
	 */
	public NDLDuplicateFieldCorrectionContainerAdapter(String field, NDLDataPair<String> ... pairs) {
		this(field, '(', ')', pairs);
	}
	
	/**
	 * Constructor
	 * @param field which field to handle duplicates
	 * @param braceStart this field determines which character wraps the suffix part, default value is '('
	 * @param braceEnd this field determines which character wraps the suffix part, default value is ')'
	 * @param fields field configuration by which duplicates to be fixed, fields must be single valued
	 */
	public NDLDuplicateFieldCorrectionContainerAdapter(String field, char braceStart, char braceEnd, String ... fields) {
		NDLDataPair<String> pairs[] = new NDLDataPair[fields.length];
		int c = 0;
		for(String f : fields) {
			pairs[c++] = new NDLDataPair<String>(f, "");
		}
		construct(field, braceStart, braceEnd, pairs);
	}
	
	/**
	 * Constructor
	 * @param field which field to handle duplicates
	 * @param fields field configuration by which duplicates to be fixed, fields must be single valued
	 * @see #NDLDuplicateFieldCorrectionContainerAdapter(String, char, char, String...)
	 */
	public NDLDuplicateFieldCorrectionContainerAdapter(String field, String ... fields) {
		this(field, '(', ')', fields);
	}
	
	/**
	 * Tries to fix the field
	 * @param item current processing item
	 * @return returns true if item is fixed, otherwise false
	 * @throws Exception throws exception in case of processing error occurs
	 * @see NDLDuplicateFieldCorrectionContainer#fix(BaseNDLDataItem)
	 */
	public boolean fix(D item) throws Exception {
		return duplicateContainer.fix(item);
	}
	
	/**
	 * Gets duplicate count
	 * @return returns duplicate count
	 */
	public long getDuplicateCount() {
		return duplicateContainer.getDuplicateCount();
	}
}