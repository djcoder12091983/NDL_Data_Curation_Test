package org.iitkgp.ndl.converter;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.DataContainerNULLConfiguration;
import org.iitkgp.ndl.data.iterator.DataSourceNULLConfiguration;
import org.iitkgp.ndl.data.iterator.SIPDataIterator;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * <pre>NDL SIP data to CSV converter  for all available data in data.
 * It takes input file (compressed and AIP folder) and generates CSV</pre>
 * Note: <ul>
 * <li>
 * Currently it supports 'tar.gz' compressed file, but other files can be supported,
 * for more details see {@link NDLDataUtils#getCompressedDataReader(java.io.File)}
 * </li>
 * <li>
 * See /conf/default.global.configuration.properties#csv.data.write.multiple.value.separator multivalue separator in CSV cell
 * </li>
 * <li>
 * See /conf/default.global.configuration.properties#csv.log.write.line.threshold.limit for CSV file rolling
 * </li>
 * </ul>
 * @see NDLAIP2CSVConverter
 * @author Debasis
 */
public class NDLSIP2CSVConverter extends NDLData2CSVConverter<SIPDataItem, SIPDataIterator> {
	
	/**
	 * Constructor
	 * @param input Input file (compressed and AIP folder) and generates CSV
	 * @param logLocation Log location where CSV data will be written
	 */
	public NDLSIP2CSVConverter(String input, String logLocation) {
		super(input, logLocation,
				new DataContainerNULLConfiguration<DataSourceNULLConfiguration>(new DataSourceNULLConfiguration()));
		dataReader = new SIPDataIterator(input);
	}
	
	/**
	 * Constructor
	 * @param input Input file (compressed and AIP folder) and generates CSV
	 * @param logLocation Log location where CSV data will be written
	 * @param containerConfifuration Container configuration to initialize
	 */
	public NDLSIP2CSVConverter(String input, String logLocation,
			DataContainerNULLConfiguration<DataSourceNULLConfiguration> containerConfifuration) {
		super(input, logLocation, containerConfifuration);
		dataReader = new SIPDataIterator(input);
	}
	
	/**
	 * Constructor
	 * @param input Input file (compressed and AIP folder) and generates CSV
	 * @param logLocation Log location where CSV data will be written
	 * @param name Logical name typically source name which adds prefix to CSV file(s)
	 */
	public NDLSIP2CSVConverter(String input, String logLocation, String name){
		super(input, logLocation, name,
				new DataContainerNULLConfiguration<DataSourceNULLConfiguration>(new DataSourceNULLConfiguration()));
		dataReader = new SIPDataIterator(input);
	}
	
	/**
	 * Constructor
	 * @param input Input file (compressed and AIP folder) and generates CSV
	 * @param logLocation Log location where CSV data will be written
	 * @param name Logical name typically source name which adds prefix to CSV file(s)
	 * @param validationFlag validation flag for data validation
	 */
	public NDLSIP2CSVConverter(String input, String logLocation, String name, boolean validationFlag){
		super(input, logLocation, name,
				new DataContainerNULLConfiguration<DataSourceNULLConfiguration>(new DataSourceNULLConfiguration()),
				validationFlag);
		dataReader = new SIPDataIterator(input);
	}
	
	/**
	 * Constructor
	 * @param input Input file (compressed and AIP folder) and generates CSV
	 * @param logLocation Log location where CSV data will be written
	 * @param name Logical name typically source name which adds prefix to CSV file(s)
	 * @param containerConfifuration Container configuration to initialize
	 */
	public NDLSIP2CSVConverter(String input, String logLocation, String name,
			DataContainerNULLConfiguration<DataSourceNULLConfiguration> containerConfifuration) {
		super(input, logLocation, name, containerConfifuration);
		dataReader = new SIPDataIterator(input);
	}
}