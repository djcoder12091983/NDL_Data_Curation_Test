package org.iitkgp.ndl.core;

import java.util.HashMap;
import java.util.Map;

/**
 * Each node encapsulates information to build NDL-map prefix tree
 * @param T unknown value type associated with ndl-map prefix-tree key
 * @see NDLFieldTokenSplitter
 * @see NDLMap
 * @see NDLSet
 * @author Debasis
 */
public class NDLMapEntryNode<T> {

	String prefix; // node prefix
	boolean end; // end flag to determine whether a node indicate end of entry or not
	T data; // associated data for key/entry
	Map<String, NDLMapEntryNode<T>> keys = null; // children by prefix key

	/**
	 * Constructor
	 * @param prefix NDL-map prefix
	 * @param end end flag to determine whether a node indicate end of entry or not
	 */
	public NDLMapEntryNode(String prefix, boolean end) {
		this.prefix = prefix;
		this.end = end;
	}

	/**
	 * @param prefix NDL-map prefix
	 * @return added node
	 */
	NDLMapEntryNode<T> addKey(String prefix) {
		if (keys == null) {
			keys = new HashMap<String, NDLMapEntryNode<T>>(2);
		}
		NDLMapEntryNode<T> entry = keys.get(prefix);
		if (entry == null) {
			entry = new NDLMapEntryNode<T>(prefix, false);
			keys.put(prefix, entry);
		}
		return entry;
	}

	// by prefix returns added node
	NDLMapEntryNode<T> get(String prefix) {
		return keys != null ? keys.get(prefix) : null;
	}

	// sets data to a node where entry ends
	void setData(T data) {
		this.data = data;
		this.end = true;
	}

	/**
	 * Gets associated data of the node
	 * @return returns data
	 */
	public T get() {
		return data;
	}

	/**
	 * gets prefix of the node
	 * @return returns prefix
	 */
	public String getPrefix() {
		return prefix;
	}
}