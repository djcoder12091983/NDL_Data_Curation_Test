package org.iitkgp.ndl.data.container;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.iterator.SIPDataIterator;

/**
 * Parsing SIP data but not saved
 * @deprecated Instead use {@link NDLSIPDataContainer}
 * @author Debasis
 */
@Deprecated
public abstract class DefaultNDLSIPDataContainer extends DefaultAbstractNDLDataContainer<SIPDataItem, SIPDataIterator> {
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param name logical data source name, by which logging file(s) etc. prefixed
	 */
	public DefaultNDLSIPDataContainer(String input, String logLocation, String name) {
		super(input, logLocation, name, false);
		dataReader = new SIPDataIterator(input);
	}	
}