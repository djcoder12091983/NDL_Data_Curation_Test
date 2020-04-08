package org.iitkgp.ndl.data.container;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.iitkgp.ndl.data.exception.InvalidConfigurationKeyException;
import org.iitkgp.ndl.data.exception.PrimaryIndexNotFoundException;
import org.iitkgp.ndl.util.NDLDataUtils;

import com.opencsv.CSVReader;

/**
 * <pre>This class encapsulates logic of mapping configuration access by key.
 * The data is given in table format, typically a CSV file (comma separated values, quote is double quote).</pre>
 * <ul>
 * <li>Each CSV file/configuration is identified by logical name. This can't contain dot.</li>
 * <li>Each CSV file/configuration is identified by primary index if exists</li>
 * <li>If primary index does not exist, for example a single column then index column is NULL</li>
 * </ul>
 * <pre>Some examples of key by which data is accessed,
 * <b>&lt;logical-name&gt;.[&lt;primary-index&gt;].[&lt;sub-column-name&gt;]</b>, logical name is must but rest two are optional.</pre>
 * <pre>If CSV file/table has two columns then <b>primary index</b> is required but <b>sub-column-name</b>.</pre>
 * <pre>But if CSV file/table has only one column then <b>primary index</b> is not required so <b>sub-column-name</b>.</pre>
 * @author Debasis, Aurghya
 */
public class ConfigurationPool {
	
	// logical name vs. configuration detail
	Map<String, ConfigurationData> configuration = new HashMap<String, ConfigurationData>();
	
	// loads CSV data into configuration pool
	void loadResource(InputStream resource, String logicalName, String primaryIndex, boolean ignoreCase)
			throws PrimaryIndexNotFoundException, IOException {
		CSVReader reader = NDLDataUtils.readCSV(new InputStreamReader(resource), ',', '"');
		String[] tokens = null;
		tokens = reader.readNext();
		int pk = -1;
		List<String> columnIndex = new ArrayList<String>();
		if(primaryIndex == null) {
			pk = 0;
			columnIndex.add("DUMMY");
		} else {
			// multiple columns
			int index = 0;
			for(String token : tokens) {
				if(token.equals(primaryIndex)) {
					// primary index
					pk = index;
				} else {
					// other columns
					columnIndex.add(token);
				}
				index++;
			}
		}
		if(pk == -1) {
			// PK not found
			reader.close();
			throw new PrimaryIndexNotFoundException("primary index: " + primaryIndex + " not found.");
		}
		// load data
		ConfigurationData data = new ConfigurationData(columnIndex, ignoreCase);
		while((tokens = reader.readNext()) != null) {
			String key = tokens[pk];
			int l = tokens.length;
			List<String> row = new ArrayList<String>();
			for(int i=0; i<l; i++) {
				if(i != pk) {
					// other than PK
					row.add(tokens[i]);
				}
			}
			if(row.isEmpty()) {
				// dummy value
				row.add("Y");
			}
			data.add(key, row);
		}
		reader.close();
		// configuration data load
		ConfigurationData existing = configuration.get(logicalName);
		if(existing == null) {
			// first time
			configuration.put(logicalName, data);
		} else {
			// merge
			existing.merge(data);
		}
	}
	
	/**
	 * Adds configuration resource (CSV/table formatted data)
	 * @param resource resource stream (CSV/table formatted data)
	 * @param logicalName logical name by which particular configuration
	 * @throws IOException throws error when loading configuration fails
	 */
	public void addResource(InputStream resource, String logicalName) throws IOException {
		// only single column
		addResource(resource, null, logicalName);
	}
	
	/**
	 * Adds configuration resource (CSV/table formatted data)
	 * @param resource resource stream (CSV/table formatted data)
	 * @param primaryIndex primary index to identify the row (RDBMS table concept)
	 * @param logicalName logical name by which particular configuration
	 * @throws PrimaryIndexNotFoundException throws error if provided primary index not found in configuration file
	 * @throws IOException throws error when loading configuration fails
	 * @see #addResource(File, String)
	 */
	public void addResource(InputStream resource, String primaryIndex, String logicalName)
			throws PrimaryIndexNotFoundException, IOException {
		loadResource(resource, logicalName, primaryIndex, false);
	}
	
	/**
	 * Adds configuration resource (CSV/table formatted data)
	 * @param resource resource stream (CSV/table formatted data)
	 * @param primaryIndex primary index to identify the row (RDBMS table concept)
	 * @param logicalName logical name by which particular configuration
	 * @param ignoreCase this flag determines whether to ignore case or not for primary key
	 * @throws PrimaryIndexNotFoundException throws error if provided primary index not found in configuration file
	 * @throws IOException throws error when loading configuration fails
	 */
	public void addResource(InputStream resource, String primaryIndex, String logicalName, boolean ignoreCase)
			throws PrimaryIndexNotFoundException, IOException {
		loadResource(resource, logicalName, primaryIndex, ignoreCase);
	}
	
	/**
	 * Adds configuration resource (CSV/table formatted data)
	 * @param resourceFile resource file (CSV/table formatted data)
	 * @param logicalName logical name by which particular configuration
	 * @throws IOException throws error when loading configuration fails
	 */
	public void addResource(File resourceFile, String logicalName) throws IOException {
		// only single column
		addResource(resourceFile, null, logicalName, false);
	}
	
	/**
	 * Adds configuration resource (CSV/table formatted data)
	 * @param resourceFile resource file (CSV/table formatted data)
	 * @param logicalName logical name by which particular configuration
	 * @param ignoreCase this flag determines whether to ignore case or not for primary key
	 * @throws IOException throws error when loading configuration fails
	 */
	public void addResource(File resourceFile, String logicalName, boolean ignoreCase) throws IOException {
		// only single column
		addResource(resourceFile, null, logicalName, ignoreCase);
	}
	
	/**
	 * Adds configuration resource (CSV/table formatted data)
	 * @param resourceFile resource file (CSV/table formatted data)
	 * @param primaryIndex primary index to identify the row (RDBMS table concept)
	 * @param logicalName logical name by which particular configuration
	 * @param ignoreCase this flag determines whether to ignore case or not for primary key
	 * @throws PrimaryIndexNotFoundException throws error if provided primary index not found in configuration file
	 * @throws IOException throws error when loading configuration fails
	 */
	public void addResource(File resourceFile, String primaryIndex, String logicalName, boolean ignoreCase)
			throws PrimaryIndexNotFoundException, IOException {
		File resources[] = null;
		if(resourceFile.isDirectory()) {
			resources = resourceFile.listFiles();
		} else {
			resources = new File[1];
			resources[0] = resourceFile;
		}
		// load resources
		for(File resource : resources) {
			// each file
			try {
				loadResource(new FileInputStream(resource), logicalName, primaryIndex, ignoreCase);
			} catch(Exception ex) {
				// error occurs
				System.err.println("Error loading resource: " + resource.getAbsolutePath());
				throw ex;
			}
		}
	}
	
	/**
	 * Adds configuration resource (CSV/table formatted data)
	 * @param resourceFile resource file (CSV/table formatted data)
	 * @param primaryIndex primary index to identify the row (RDBMS table concept)
	 * @param logicalName logical name by which particular configuration
	 * @throws PrimaryIndexNotFoundException throws error if provided primary index not found in configuration file
	 * @throws IOException throws error when loading configuration fails
	 * @see #addResource(File, String)
	 */
	public void addResource(File resourceFile, String primaryIndex, String logicalName)
			throws PrimaryIndexNotFoundException, IOException {
		addResource(resourceFile, primaryIndex, logicalName, false);
	}
	
	/**
	 * Gets mapped for a given key. For key details see class level documentation.
	 * @param key given key
	 * @return returns mapped value for a given key, NULL if no mapped data found
	 * @throws InvalidConfigurationKeyException throws error if configuration is not found (invalid configuration)
	 */
	public String get(String key) throws InvalidConfigurationKeyException {
		//key = ConfigurationData.normalizeToken(key); // escape dot
		String[] tokens = key.split("\\.");
		int l = tokens.length;
		if(l < 2 || l > 3) {
			throw new InvalidConfigurationKeyException("At least two tokens expected: XXX.YYY[.ZZZ] or more than 3 tokens");
		}
		String first = ConfigurationData.denormalizeToken(tokens[0]);
		ConfigurationData data = configuration.get(first);
		if(data == null) {
			throw new InvalidConfigurationKeyException("The key: " + first + " not found");
		} else {
			String subkey = tokens[1] + (tokens.length > 2 ? ("." + tokens[2]) : "");
			String value = data.get(subkey);
			if(value == null) {
				throw new InvalidConfigurationKeyException("The data key: " + subkey + " not found");
			} else {
				return value;
			}
		}
	}
	
	/**
	 * Checks whether key if mapped or not. For key details see class level documentation.
	 * @param key given key
	 * @return returns true if mapped otherwise false
	 * @throws InvalidConfigurationKeyException throws error if expression contains more than 3 tokens,
	 * ex: XXX.YYY.ZZZ.more_token
	 */
	public boolean contains(String key) throws InvalidConfigurationKeyException {
		//key = ConfigurationData.normalizeToken(key); // escape dot
		String[] tokens = key.split("\\.");
		int l = tokens.length;
		if(l > 3) {
			throw new InvalidConfigurationKeyException(
					"At most three tokens expected: XXX[.YYY][.ZZZ]. Last two are optional.");
		}
		String first = ConfigurationData.denormalizeToken(tokens[0]);
		ConfigurationData data = configuration.get(first);
		if(data == null) {
			// logical name not found
			return false;
		} else {
			if(l > 1) {
				// valid key
				return l == 2 ? data.contains(tokens[1]) : data.contains(tokens[1], tokens[2]);
			} else {
				// logical name exists test
				return true;
			}
		}
	}
	
	/**
	 * Get all values for a given configuration <b>&lt;logical-name&gt;.&lt;primary-index&gt;</b>.
	 * @param key given key, key details see class level documentation.
	 * @param multivalueSeparator if a cell contains multiple values then those value should be separated by a character
	 * @return returns all values, NULL if no mapped key found
	 * @throws InvalidConfigurationKeyException throws error if configuration is not found (invalid configuration)
	 */
	public Map<String, List<String>> getAll(String key, char multivalueSeparator)
			throws InvalidConfigurationKeyException {
		//key = ConfigurationData.normalizeToken(key); // escape dot
		String[] tokens = key.split("\\.");
		if(tokens.length != 2) {
			throw new InvalidConfigurationKeyException("Two tokens expected: XXX.YYY");
		}
		String first = ConfigurationData.denormalizeToken(tokens[0]);
		ConfigurationData data = configuration.get(first);
		if(data == null) {
			throw new InvalidConfigurationKeyException("The key: " + first + " not found");
		}
		String subkey = tokens[1];
		return data.getColumnValues(subkey, multivalueSeparator);
	}
}