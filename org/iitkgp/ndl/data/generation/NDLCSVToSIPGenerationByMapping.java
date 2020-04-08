package org.iitkgp.ndl.data.generation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.RowData;
import org.iitkgp.ndl.data.container.DataContainerNULLConfiguration;
import org.iitkgp.ndl.data.iterator.DataSourceCSVConfiguration;

/**
 * <pre>This class inherits detail from {@link NDLCSVToSIPGeneration}, additionally it creates SIP
 * automatically from a given mapping.</pre>
 * Mapping example: source_field=destination_NDL_field and JSON field can also be copied by
 * source_field:json_key=destination_NDL_field
 * @author Debasis
 */
public class NDLCSVToSIPGenerationByMapping extends NDLCSVToSIPGeneration {
	
	String confFile;
	Map<String, String> mapping = new HashMap<String, String>(2);

	/**
	 * Constructor, default data source configuration is "comma separated values"
	 * @param input source data (either compressed version or folder version)
	 * @param logLocation logging location
	 * @param outputLocation output location
	 * @param confFile configuration file mapping CSV column to NDL field (source_CSV_column=NDL_destination_column)
	 * @param name <pre>logical name which differentiates log file(s), output file(s) from other source</pre>
	 */
	public NDLCSVToSIPGenerationByMapping(String input, String logLocation, String outputLocation, String confFile,
			String name) {
		super(input, logLocation, outputLocation, name);
		this.confFile = confFile;
	}
	
	/**
	 * Constructor
	 * @param input source data (either compressed version or folder version)
	 * @param logLocation logging location
	 * @param outputLocation output location
	 * @param confFile configuration file mapping CSV column to NDL field (source_CSV_column=NDL_destination_column)
	 * @param name <pre>logical name which differentiates log file(s), output file(s) from other source</pre>
	 * @param valueSeparator CSV value separated, if not set then default is comma. 
	 */
	public NDLCSVToSIPGenerationByMapping(String input, String logLocation, String outputLocation, String confFile,
			String name, char valueSeparator) {
		super(input, logLocation, outputLocation, name, valueSeparator);
		this.confFile = confFile;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void init(DataContainerNULLConfiguration<DataSourceCSVConfiguration> configuration) throws IOException {
		super.init(configuration); // super call required
		load();
	}
	
	// loads mapping
	void load() throws IOException {
		System.out.println("Loading mapping file .....");
		
		// load mapping
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(confFile));
			String line = null;
			while((line = reader.readLine()) != null) {
				if(StringUtils.isBlank(line)) {
					// skip blank line
					continue;
				}
				String tokens[] = line.split("=");
				int l = tokens.length;
				if(l == 1 || l > 2) {
					// no destination field
					System.err.println("Found invalid destination field: " + line);
					continue;
				}
				if(StringUtils.isBlank(tokens[1])) {
					// invalid destination field
					System.err.println("Found no destination field: " + line);
					continue;
				}
				mapping.put(tokens[0], tokens[1]);
			}
		} finally {
			// close resource
			IOUtils.closeQuietly(reader);
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean generateTargetItem(RowData csv, RowData target) throws Exception {
		// load data from mapping
		for(String field : mapping.keySet()) {
			copy(field, mapping.get(field));
		}
		
		return true; // allow all rows
	}
}