package org.iitkgp.ndl.core;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * This a generic group (similar to SQL group by clause).
 * This can be used as group-by key in a hashing data-structure (typically a hash-map)
 * @author Debasis
 */
public class Group {

	List<String> groups = new LinkedList<>();
	
	/**
	 * Adds to group-by clause
	 * @param group group-by key to add
	 */
	public void add(String group) {
		groups.add(group);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {
		if(obj == null) {
			return false;
		} else if(obj == this) {
			return true;
		} else if(obj instanceof Group) {
			Group g = (Group)obj;
			if(groups.size() != g.groups.size()) {
				// different group size
				return false;
			}
			
			Iterator<String> i1 = groups.iterator();
			Iterator<String> i2 = g.groups.iterator();
			while(i1.hasNext()) {
				if(!i1.next().equals(i2.next())) {
					// group key mismatch
					return false;
				}
			}
			
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		int h = 17;
		for(String g : groups) {
			h += h * 13 + g.hashCode();
		}
		return h;
	}
}