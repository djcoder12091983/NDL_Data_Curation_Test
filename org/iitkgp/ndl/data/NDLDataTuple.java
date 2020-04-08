package org.iitkgp.ndl.data;

import java.util.ArrayList;
import java.util.List;

/**
 * Tuple of values
 * @param <T> type t data
 * @author Debasis
 */
public class NDLDataTuple<T> {
	
	List<T> data = null;
	
	/**
	 * Constructor
	 * @param values list of values
	 */
	public NDLDataTuple(T ... values) {
		data = new ArrayList<T>(values.length);
		for(T value : values) {
			data.add(value);
		}
	}
	
	/**
	 * Returns value at index
	 * @param index given index
	 * @return returns value
	 */
	public T at(int index) {
		return data.get(index);
	}
}