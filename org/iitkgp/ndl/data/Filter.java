package org.iitkgp.ndl.data;

/**
 * Filter logic encapsulation
 * @param <D> data on which filter logic will be applied
 */
public interface Filter<D> {
	/**
	 * Encapsulates filter logic on selected data
	 * @param data selected data/row
	 * @return returns true if data selected otherwise false
	 */
    boolean filter(D data);
}
