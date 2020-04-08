package org.iitkgp.ndl.data;

/**
 * Data pair
 * @param <T> type of data t
 * @see NDLDataTuple
 * @author Debasis
 */
public class NDLDataPair<T> extends NDLDataTuple<T> {
	
	/**
	 * Constructor
	 * @param first first value
	 * @param second second value
	 */
	public NDLDataPair(T first, T second) {
		super(first, second);
	}
	
	/**
	 * Gets first value
	 * @return returns first value
	 */
	public T first() {
		return at(0);
	}
	
	/**
	 * Gets second value
	 * @return returns second value
	 */
	public T second() {
		return at(1);
	}
}