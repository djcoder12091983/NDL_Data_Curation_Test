package org.iitkgp.ndl.data.container;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.BaseNDLDataItem;
import org.iitkgp.ndl.data.Try;

/**
 * This class tries to fix the duplicate field across the source with some given trial logic
 * @param <D> Data to be corrected in terms of duplicates
 * @author Debasis
 */
public class NDLDuplicateFieldCorrectionContainer<D extends BaseNDLDataItem> {
	
	String field;
	// TODO instead of using hashing we can use prefix-tree(using token-splitter) to save space
	Set<String> values = new HashSet<String>(2);
	long duplicateCount = 0; // after fixation still duplicate count
	
	// given trial operations
	List<Try<D, String>> trials = new LinkedList<Try<D, String>>();
	
	/**
	 * Constructor
	 * @param field field to handle duplicates, the field should be single valued
	 */
	public NDLDuplicateFieldCorrectionContainer(String field) {
		this.field = field;
	}
	
	/**
	 * Adds trial logic
	 * @param trial trial logic
	 */
	public void addTrial(Try<D, String> trial) {
		trials.add(trial);
	}
	
	/**
	 * Tries to fix the field (try to remove duplicate values)
	 * @param item current processing item
	 * @return whether the item fixed or not
	 * @throws Exception throws exception in case of errors
	 */
	public boolean fix(D item) throws Exception {
		String value = item.getSingleValue(field);
		if(StringUtils.isNotBlank(value)) {
			// value exists
			if(values.contains(value)) {
				// duplicate
				boolean f = false;
				for(Try<D, String> trial : trials) {
					String newvalue = trial.trial(item);
					if(values.contains(newvalue)) {
						// still duplicate
						continue;
					} else {
						// fixed
						values.add(newvalue);
						item.updateSingleValue(field, newvalue);
						f = true;
						break;
					}
				}
				// still duplicate
				duplicateCount++;
				return f;
			} else {
				// normal
				values.add(value);
			}
		}
		return true;
	}
	
	/**
	 * Gets duplicate count
	 * @return returns duplicate count
	 */
	public long getDuplicateCount() {
		return duplicateCount;
	}
}