package org.iitkgp.ndl.data.container;

import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.iterator.AIPDataIterator;

/**
 * Parsing AIP data but not saved
 * @deprecated Instead use {@link NDLAIPDataContainer}
 * @author Debasis
 */
@Deprecated
public abstract class DefaultNDLAIPDataContainer extends DefaultAbstractNDLDataContainer<AIPDataItem, AIPDataIterator> {
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param name logical data source name, by which logging file(s) etc. prefixed
	 */
	public DefaultNDLAIPDataContainer(String input, String logLocation, String name) {
		super(input, logLocation, name, false);
		dataReader = new AIPDataIterator(input);
	}
}