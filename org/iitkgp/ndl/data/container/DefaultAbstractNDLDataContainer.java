package org.iitkgp.ndl.data.container;

import org.iitkgp.ndl.data.NDLDataItem;
import org.iitkgp.ndl.data.iterator.DataIterator;
import org.iitkgp.ndl.data.iterator.DataSourceNULLConfiguration;

/**
 * Default implementation of {@link AbstractNDLDataContainer}. It's generally used for reading purpose.
 * @author Debasis
 */
public abstract class DefaultAbstractNDLDataContainer<D extends NDLDataItem, R extends DataIterator<D, DataSourceNULLConfiguration>>
		extends AbstractNDLDataContainer<D, R, DataSourceNULLConfiguration> {
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 */
	public DefaultAbstractNDLDataContainer(String input, String logLocation) {
		super(input, logLocation,
				new DataContainerNULLConfiguration<DataSourceNULLConfiguration>(new DataSourceNULLConfiguration()));
	}
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param globalLoggingFlag global logging flag
	 */
	public DefaultAbstractNDLDataContainer(String input, String logLocation, boolean globalLoggingFlag) {
		super(input, logLocation,
				new DataContainerNULLConfiguration<DataSourceNULLConfiguration>(new DataSourceNULLConfiguration()));
		if(!globalLoggingFlag) {
			// turn off
			turnOffGlobalLoggingFlag();
		}
	}
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param name logical name to add prefix to log files
	 */
	public DefaultAbstractNDLDataContainer(String input, String logLocation, String name) {
		super(input, logLocation, name,
				new DataContainerNULLConfiguration<DataSourceNULLConfiguration>(new DataSourceNULLConfiguration()));
	}
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param name logical name to add prefix to log files
	 * @param globalLoggingFlag global logging flag
	 */
	public DefaultAbstractNDLDataContainer(String input, String logLocation, String name, boolean globalLoggingFlag) {
		super(input, logLocation, name,
				new DataContainerNULLConfiguration<DataSourceNULLConfiguration>(new DataSourceNULLConfiguration()));
		if(!globalLoggingFlag) {
			// turn off
			turnOffGlobalLoggingFlag();
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean processItem(D item) throws Exception {
		// custom logic
		return readItem(item);
	}
	
	/**
	 * Reads item and takes some action, but item is not saved
	 * @param item item to read or process
	 * @return returns true if data read successfully happens
	 * @throws Exception throws exception in case of error occurs
	 */
	protected abstract boolean readItem(D item) throws Exception;
}