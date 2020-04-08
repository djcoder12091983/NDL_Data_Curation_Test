package org.iitkgp.ndl.data.validator;

import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.container.DataContainerNULLConfiguration;
import org.iitkgp.ndl.data.iterator.AIPDataIterator;
import org.iitkgp.ndl.data.iterator.DataSourceNULLConfiguration;
import org.iitkgp.ndl.validator.exception.NDLSchemaDetailLoadException;

/**
 * NDL AIP data validation process
 * @author Debasis
 */
public class NDLAIPDataValidator extends AbstractNDLDataValidator<AIPDataItem, AIPDataIterator> {
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @throws NDLSchemaDetailLoadException throws exception when constrains loading error occurs
	 */
	public NDLAIPDataValidator(String input, String logLocation) throws NDLSchemaDetailLoadException {
		super(input, logLocation);
		dataReader = new AIPDataIterator(input);
	}
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param containerConfifuration Container configuration to initialize
	 * @throws NDLSchemaDetailLoadException throws exception when constrains loading error occurs
	 */
	public NDLAIPDataValidator(String input, String logLocation,
			DataContainerNULLConfiguration<DataSourceNULLConfiguration> containerConfifuration)
			throws NDLSchemaDetailLoadException {
		super(input, logLocation, containerConfifuration);
		dataReader = new AIPDataIterator(input);
	}
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param name logical name to add prefix to log files 
	 * @throws NDLSchemaDetailLoadException throws exception when constrains loading error occurs
	 */
	public NDLAIPDataValidator(String input, String logLocation, String name){
		super(input, logLocation, name);
		dataReader = new AIPDataIterator(input);
	}
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param name logical name to add prefix to log files
	 * @param containerConfifuration Container configuration to initialize
	 * @throws NDLSchemaDetailLoadException throws exception when constrains loading error occurs
	 */
	public NDLAIPDataValidator(String input, String logLocation, String name,
			DataContainerNULLConfiguration<DataSourceNULLConfiguration> containerConfifuration) {
		super(input, logLocation, name, containerConfifuration);
		dataReader = new AIPDataIterator(input);
	}
}