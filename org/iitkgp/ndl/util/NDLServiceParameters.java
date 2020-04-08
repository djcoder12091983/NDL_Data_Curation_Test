package org.iitkgp.ndl.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * NDL service request parameter details encapsulation 
 * @author Debasis
 */
public class NDLServiceParameters {
	
	// parameters map
	Map<String, Collection<String>> parameters = new HashMap<String, Collection<String>>();
	
	/**
	 * Adds parameter detail
	 * @param parameter parameter name
	 * @param value parameter value
	 */
	public void addParameter(String parameter, String value) {
		Collection<String> list = parameters.get(parameter);
		if(list == null) {
			list = new LinkedList<String>();
			parameters.put(parameter, list);
		}
		list.add(value);
	}
	
	/**
	 * gets parameter details
	 * @return returns parameter details
	 */
	public Map<String, Collection<String>> getParameters() {
		return parameters;
	}

}