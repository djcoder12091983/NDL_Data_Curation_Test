package org.iitkgp.ndl.converter;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.Filter;
import org.iitkgp.ndl.data.container.DataContainerNULLConfiguration;
import org.iitkgp.ndl.data.iterator.AIPDataIterator;
import org.iitkgp.ndl.data.iterator.DataSourceNULLConfiguration;
import org.iitkgp.ndl.util.NDLDataUtils;

import com.opencsv.CSVWriter;

/**
 * <pre>NDL AIP data to CSV converter  for all available data in data.
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
 * @see NDLSIP2CSVConverter
 * @author Debasis
 */
public class NDLAIP2CSVConverter extends NDLData2CSVConverter<AIPDataItem, AIPDataIterator> {
	
	// for internal usage
	class AIPCollection {
		String name;
		String id;
		
		public AIPCollection(String id, String name) {
			this.id = id;
			this.name = name;
		}
		
		@Override
		public boolean equals(Object obj) {
			if(obj == this) {
				return true;
			}
			AIPCollection c = (AIPCollection)obj;
			return StringUtils.equals(c.id, id) && StringUtils.equals(c.name, name);
		}
		
		@Override
		public int hashCode() {
			int h = 13;
			h += 31 * id.hashCode();
			h += 31 * name.hashCode();
			return h;
		}
	}
	
	// collection titles
	Map<String, String> collectionTitles = new HashMap<String, String>(2);
	Set<AIPCollection> collections = new HashSet<AIPCollection>();
	
	/**
	 * Constructor
	 * @param input Input file (compressed and AIP folder) and generates CSV
	 * @param logLocation Log location where CSV data will be written
	 */
	public NDLAIP2CSVConverter(String input, String logLocation) {
		super(input, logLocation,
				new DataContainerNULLConfiguration<DataSourceNULLConfiguration>(new DataSourceNULLConfiguration()));
		dataReader = new AIPDataIterator(input);
	}
	
	/**
	 * Constructor
	 * @param input Input file (compressed and AIP folder) and generates CSV
	 * @param logLocation Log location where CSV data will be written
	 * @param containerConfifuration Container configuration to initialize
	 */
	public NDLAIP2CSVConverter(String input, String logLocation,
			DataContainerNULLConfiguration<DataSourceNULLConfiguration> containerConfifuration) {
		super(input, logLocation, containerConfifuration);
		dataReader = new AIPDataIterator(input);
	}
	
	/**
	 * Constructor
	 * @param input Input file (compressed and AIP folder) and generates CSV
	 * @param logLocation Log location where CSV data will be written
	 * @param name Logical name typically source name which adds prefix to CSV file(s)
	 */
	public NDLAIP2CSVConverter(String input, String logLocation, String name) {
		super(input, logLocation, name,
				new DataContainerNULLConfiguration<DataSourceNULLConfiguration>(new DataSourceNULLConfiguration()));
		dataReader = new AIPDataIterator(input);
	}
	
	/**
	 * Constructor
	 * @param input Input file (compressed and AIP folder) and generates CSV
	 * @param logLocation Log location where CSV data will be written
	 * @param name Logical name typically source name which adds prefix to CSV file(s)
	 * @param validationFlag validation flag for data validation
	 */
	public NDLAIP2CSVConverter(String input, String logLocation, String name, boolean validationFlag) {
		super(input, logLocation, name, validationFlag);
		dataReader = new AIPDataIterator(input);
	}
	
	/**
	 * Constructor
	 * @param input Input file (compressed and AIP folder) and generates CSV
	 * @param logLocation Log location where CSV data will be written
	 * @param name Logical name typically source name which adds prefix to CSV file(s)
	 * @param containerConfifuration Container configuration to initialize
	 */
	public NDLAIP2CSVConverter(String input, String logLocation, String name,
			DataContainerNULLConfiguration<DataSourceNULLConfiguration> containerConfifuration) {
		super(input, logLocation, name, containerConfifuration);
		dataReader = new AIPDataIterator(input);
	}
	
	@Override
	void addExtraData(AIPDataItem item) {
		// super call
		super.addExtraData(item);
		// add extra data list
		String pid = item.getParentId();
		currentRow.addData("Collection_ID", pid);
		String name = collectionTitles.get(NDLDataUtils.getHandleSuffixID(pid));
		currentRow.addData("Collection_Name", name);
		collections.add(new AIPCollection(pid, name));
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void preProcessData() throws Exception {
		// super call
		super.preProcessData();
		// load hierarchy
		System.out.println("Loading hierarchy infromations ...");
		while(dataReader.hasNext()) {
			AIPDataItem item = dataReader.next();
			if(item.isCollection()) {
				String itemid = NDLDataUtils.getHandleSuffixID(item.getId());
				collectionTitles.put(itemid, item.getSingleValue("dc.title"));
			}
		}
		// reset the reader
		dataReader.reset();
		// add parent ID
		orderedColumns.add(1, "Collection_ID");
		orderedColumns.add(2, "Collection_Name");
		// add filter logic
		addDataFilter(new Filter<AIPDataItem>() {
			// skip collection data
			@Override
			public boolean filter(AIPDataItem data) {
				return data.isItem(); // allow items only
			}
		});
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void postProcessData() throws Exception {
		// super call
		super.postProcessData();
		// write collection CSV
		CSVWriter writer = NDLDataUtils.openCSV(new File(logLocation, getFileName("collections.csv")));
		writer.writeNext(new String[]{"Collection_ID", "Collection_Name"});
		for(AIPCollection c : collections) {
			writer.writeNext(new String[]{c.id, c.name});
		}
		writer.close();
	}
}