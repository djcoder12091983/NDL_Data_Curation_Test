package org.iitkgp.ndl.data.validator;

import java.util.LinkedList;
import java.util.List;

import org.iitkgp.ndl.core.NDLMap;
import org.iitkgp.ndl.core.NDLMapKeyProcessor;

/**
 * <pre>NDL unique field handler, see {@link NDLMap#setKeyProcessor(NDLMapKeyProcessor)}.</pre>
 * This handler required to collect the field to track uniqueness, for more details see {@link NDLMap}.
 * @see AbstractNDLDataValidator
 * @author Debasis
 */
public class NDLUniqueFieldDuplicateEntryHandler implements NDLMapKeyProcessor<List<String>> {
	
	String field;
	List<List<String>> duplicateValues = new LinkedList<List<String>>();
	
	/**
	 * Constructor
	 * @param field field name to track uniquely
	 */
	public NDLUniqueFieldDuplicateEntryHandler(String field) {
		this.field = field;
	}
	
	/**
	 * Collect the fields to track uniqueness, this collects values if count is more than one.
	 */
	@Override
	public void process(List<String> data) {
		if(data.size() > 1) {
			// multiple entries
			duplicateValues.add(data); // track
		}
	}
	
	/**
	 * Returns duplicate values detail
	 * @return returns duplicate values detail
	 */
	public List<List<String>> getDuplicateValues() {
		return duplicateValues;
	}

}