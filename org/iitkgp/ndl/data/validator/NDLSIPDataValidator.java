package org.iitkgp.ndl.data.validator;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.DataContainerNULLConfiguration;
import org.iitkgp.ndl.data.iterator.DataSourceNULLConfiguration;
import org.iitkgp.ndl.data.iterator.SIPDataIterator;
import org.iitkgp.ndl.validator.exception.NDLSchemaDetailLoadException;

/**
 * NDL SIP data validation process
 * @author Debasis
 */
public class NDLSIPDataValidator extends AbstractNDLDataValidator<SIPDataItem, SIPDataIterator> {
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @throws NDLSchemaDetailLoadException throws exception when constrains loading error occurs
	 */
	public NDLSIPDataValidator(String input, String logLocation) throws NDLSchemaDetailLoadException {
		super(input, logLocation);
		dataReader = new SIPDataIterator(input);
	}
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param containerConfifuration Container configuration to initialize
	 * @throws NDLSchemaDetailLoadException throws exception when constrains loading error occurs
	 */
	public NDLSIPDataValidator(String input, String logLocation,
			DataContainerNULLConfiguration<DataSourceNULLConfiguration> containerConfifuration)
			throws NDLSchemaDetailLoadException {
		super(input, logLocation, containerConfifuration);
		dataReader = new SIPDataIterator(input);
	}
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param name logical name to add prefix to log files 
	 * @throws NDLSchemaDetailLoadException throws exception when constrains loading error occurs
	 */
	public NDLSIPDataValidator(String input, String logLocation, String name){
		super(input, logLocation, name);
		dataReader = new SIPDataIterator(input);
	}
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param name logical name to add prefix to log files
	 * @param containerConfifuration Container configuration to initialize
	 * @throws NDLSchemaDetailLoadException throws exception when constrains loading error occurs
	 */
	public NDLSIPDataValidator(String input, String logLocation, String name,
			DataContainerNULLConfiguration<DataSourceNULLConfiguration> containerConfifuration) {
		super(input, logLocation, name, containerConfifuration);
		dataReader = new SIPDataIterator(input);
	}
}