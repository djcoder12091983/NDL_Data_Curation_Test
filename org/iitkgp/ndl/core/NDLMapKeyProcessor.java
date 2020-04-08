package org.iitkgp.ndl.core;

/**
 * NDL-map key processor, this required to iterate and post-process on found an entry
 * @see NDLMap
 * @see NDLSet
 * @author Debasis
 */
public interface NDLMapKeyProcessor<T> {
	
	/**
	 * post-process with data
	 * @param data data of end flag enabled data
	 */
	void process(T data);
}