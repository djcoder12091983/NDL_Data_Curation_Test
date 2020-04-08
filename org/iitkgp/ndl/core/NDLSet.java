package org.iitkgp.ndl.core;

/**
 * NDL-set backed by NDL-map
 * @see NDLMap
 * @author Debasis
 */
public class NDLSet {

	// map (entry with character-Y)
	NDLMap<Character> map = new NDLMap<Character>();
	
	/**
	 * adds entry
	 * @param tokens splitted tokens
	 */
	public void add(String tokens[]) {
		map.add(tokens, 'Y');
	}
	
	/**
	 * Checks whether key exists in set or not
	 * @param tokens splitted tokens
	 * @return returns true if found otherwise false
	 */
	public boolean containsKey(String tokens[]) {
		return map.containsKey(tokens);
	}
}