package org.iitkgp.ndl.data;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.util.NDLDataUtils;

import com.google.common.base.Joiner;
import com.opencsv.CSVWriter;

/**
 * Row data list which holds tabular data list of {@link RowData}
 * @author Debasis, Aurghya
 */
public class RowDataList {
	
	/*static {
		NDLConfigurationContext.init();
	}*/
	
	int lastColumnID = 0;
	int columnSize;
	//long csvFlushLimit = Long.parseLong(NDLConfigurationContext.getConfiguration("csv.log.write.line.threshold.limit"));
	
	// columns are associated with a identifier
	Map<String, Integer> columnMapper = new LinkedHashMap<String, Integer>(columnSize);
	// tabular data
	List<Map<Integer, Collection<String>>> dataList = new ArrayList<Map<Integer, Collection<String>>>();
	
	/**
	 * constructor
	 */
	public RowDataList() {
		columnSize = 16; // default column size
	}
	
	/**
	 * Constructor
	 * @param columnSize Initial column size
	 */
	public RowDataList(int columnSize) {
		this.columnSize = columnSize;
	}
	
	/**
	 * Reset to table to hold new data form scratch
	 */
	public void reset() {
		columnMapper.clear();
		dataList.clear();
		lastColumnID = 0;
	}
	
	/**
	 * Adds row-data into table
	 * @param data row-data to be added
	 */
	public void addRowData(RowData data) {
		
		Map<Integer, Collection<String>> rowData = new HashMap<Integer, Collection<String>>();
		dataList.add(rowData);
		
		for(Entry<String, Collection<String>> entry : data.entrySet()) {
			
			String columnName = entry.getKey();
			Collection<String> value = entry.getValue();
			
			Integer columnID = columnMapper.get(columnName);
			if(columnID == null) {
				// new entry
				columnMapper.put(columnName, ++lastColumnID);
				columnID = lastColumnID;
			}
			
			rowData.put(columnID, value);
		}
	}
	
	/**
	 * Gets column mapping detail
	 * @return mapping detail
	 */
	public Map<String, Integer> getColumnMapper() {
		return columnMapper;
	}
	
	/**
	 * Gets all data list in table
	 * @return returns data list
	 */
	public List<Map<Integer, Collection<String>>> getDataList() {
		return dataList;
	}
	
	/**
	 * Returns table row size
	 * @return returns table row size
	 */
	public int size() {
		return dataList.size();
	}
	
	/**
	 * Checks whether table is empty or not
	 * @return returns true if empty otherwise false
	 */
	public boolean isEmpty() {
		return dataList.size() == 0;
	}
	
	/**
	 * Gets values by a given row index and column name
	 * @param rowIdx row index
	 * @param columnName column name
	 * @return returns list of values by given column and row index,
	 * see {@link ArrayIndexOutOfBoundsException} this error is thrown when invalid row-index provided.
	 * returns NULL when invalid column provided
	 */
	public Collection<String> getValues(int rowIdx, String columnName) {
		
		Integer id = columnMapper.get(columnName);
		if(id == null) {
			// default-value (won't find in column entry)
			id = Integer.MIN_VALUE;
		}
		
		Map<Integer, Collection<String>> row = dataList.get(rowIdx);
		Collection<String> value = row.get(id);
		
		return value;
	}
	
	/**
	 * Gets single value for a given row index and column name
	 * @param rowIdx row index
	 * @param columnName column name
	 * @return returns single for a given column and row index,
	 * see {@link ArrayIndexOutOfBoundsException} this error is thrown when invalid row-index provided.
	 * returns NULL when invalid column provided amd EMPTY when multiple values is access
	 */
	public String getSingleValue(int rowIdx, String columnName) {
		Collection<String> values = getValues(rowIdx, columnName);
		if(values != null && !values.isEmpty()) {
			return values.iterator().next();
		} else {
			return StringUtils.EMPTY;
		}
	}
	
	/**
	 * Sets order columns to appear those column values first
	 * @param prioritizedColumns column names to appear first
	 */
	public void orderColumnHeaders(List<String> prioritizedColumns) {
		Map<String, Integer> newColumnMapper = new LinkedHashMap<String, Integer>();
		for(String prioritizedColumn : prioritizedColumns) {
			Integer id = columnMapper.get(prioritizedColumn);
			if(id != null) {
				// new entry
				newColumnMapper.put(prioritizedColumn, id);
				// remove entry from original map
				columnMapper.remove(prioritizedColumn);
			}
		}
		// remaining columns
		for(String column : columnMapper.keySet()) {
			newColumnMapper.put(column, columnMapper.get(column));
		}
		columnMapper.clear(); // remove all
		// add new column orders
		columnMapper.putAll(newColumnMapper);
	}
	
	/**
	 * Flush the data into CSV
	 * @param out CSV file
	 * @param separator multiple value separator in CSV cell
	 * @throws IOException throws error in case data flush error occurs
	 */
	public void flush2CSV(File out, char separator) throws IOException {
		// TODO this method generic in such a way so that any output stream can  be flushed with table data
		// open CSV file
		CSVWriter writer = NDLDataUtils.openCSV(out, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.DEFAULT_QUOTE_CHARACTER);
		
		try {
			// headers
			Set<String> columns = columnMapper.keySet();
			int s = columns.size();
			String headers[] = new String[s];
			columns.toArray(headers);
			writer.writeNext(headers, true);
			
			Joiner joiner = Joiner.on(separator).skipNulls();
			// actual data
			for(Map<Integer, Collection<String>> row : dataList) {
				
				String dataValues[] = new String[s];
				int c = 0;
				for(String columnName : columns) {
					
					int id = columnMapper.get(columnName);
					Collection<String> value = row.get(id);
					
					String text = value != null ? joiner.join(value) : StringUtils.EMPTY;
					dataValues[c++] = text;
				}
				
				writer.writeNext(dataValues, true);
			}
		} finally {
			// finally block
			reset();
			writer.close();
		}
	}

}