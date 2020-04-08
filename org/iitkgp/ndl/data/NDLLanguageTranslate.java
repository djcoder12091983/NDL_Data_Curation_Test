package org.iitkgp.ndl.data;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

/**
 * NDL language translation logic
 * @author Debasis
 */
public class NDLLanguageTranslate {
	
	String field;
	List<List<NDLLanguageDetail>> values = new LinkedList<List<NDLLanguageDetail>>();
	
	/**
	 * Constructor
	 * @param field given field
	 */
	public NDLLanguageTranslate(String field) {
		this.field = field;
	}
	
	/**
	 * Constructor
	 * @param field given field
	 * @param values given detail value
	 * @see NDLLanguageDetail
	 */
	public NDLLanguageTranslate(String field, NDLLanguageDetail ... values) {
		this(field, Arrays.asList(values));
	}
	
	/**
	 * Constructor
	 * @param field given field
	 * @param values given detail value
	 * @see NDLLanguageDetail
	 */
	public NDLLanguageTranslate(String field, Collection<NDLLanguageDetail> values) {
		this.field = field;
		List<NDLLanguageDetail> newvalues = new LinkedList<NDLLanguageDetail>();
		for(NDLLanguageDetail value : values) {
			if(StringUtils.isNotBlank(value.translation)) {
				// valid
				newvalues.add(value);
			}
		}
		if(!newvalues.isEmpty()) {
			this.values.add(newvalues);
		}
	}

	/**
	 * Gets given field name
	 * @return returns filed name
	 */
	public String getField() {
		return field;
	}
	
	/**
	 * Gets field related values detail
	 * @return returns detail
	 * @see NDLLanguageDetail
	 */
	public List<List<NDLLanguageDetail>> getValues() {
		return values;
	}
	
	/**
	 * Adds each value detail
	 * @param values values detail
	 */
	public void addValue(List<NDLLanguageDetail> values) {
		List<NDLLanguageDetail> newvalues = new LinkedList<NDLLanguageDetail>();
		for(NDLLanguageDetail value : values) {
			if(StringUtils.isNotBlank(value.translation)) {
				// valid
				newvalues.add(value);
			}
		}
		if(!newvalues.isEmpty()) {
			this.values.add(newvalues);
		}
	}
	
	/**
	 * Adds each value detail
	 * @param values value detail by array of @link {@link NDLLanguageDetail}
	 */
	public void addValue(NDLLanguageDetail ... values) {
		List<NDLLanguageDetail> newvalues = new LinkedList<NDLLanguageDetail>();
		for(NDLLanguageDetail value : values) {
			if(StringUtils.isNotBlank(value.translation)) {
				// valid
				newvalues.add(value);
			}
		}
		if(!newvalues.isEmpty()) {
			this.values.add(newvalues);
		}
	}
}