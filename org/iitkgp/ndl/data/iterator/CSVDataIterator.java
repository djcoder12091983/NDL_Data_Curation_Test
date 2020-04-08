package org.iitkgp.ndl.data.iterator;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.iitkgp.ndl.data.RowData;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.xml.sax.SAXException;

import com.opencsv.CSVReader;

/**
 * <pre>CSV data iteration process, it reads CSV file/files row by row and returns {@link RowData}</pre>
 * Note: CSV file should have columns by which mapping to be done, system assumes first row to be columns.
 * @author Debasis
 */
public class CSVDataIterator extends DataIterator<RowData, DataSourceCSVConfiguration> {

	String input;
	CSVReader csvReader;
	File csvFiles[]; // CSV files
	int csvFileIndex = 0;
	String[] columns;
	String[] currentRow = null;
	int currentRowIndex;
	File currentCsvFile;
	Character multipleValueSeparator = null;
	
	/**
	 * Constructor
	 * @param input input source
	 */
	public CSVDataIterator(String input) {
		this.input = input;
	}
	
	/**
	 * Initializes CSV reading process
	 */
	@Override
	public void init(DataSourceCSVConfiguration configuration) throws IOException {
		super.init(configuration); // super call
		
		// csv parser
		if(this.configuration.isMultipleValueSeparatorAvailable()) {
			// multiple value separator available
			this.multipleValueSeparator = this.configuration.getMultipleValueSeparator();
		}
		File inputFile = new File(input);
		if(inputFile.isFile()) {
			// single CSV file
			csvFiles = new File[1];
			csvFiles[0] = inputFile;
		} else if(inputFile.isDirectory()) {
			// multiple CSV files
			csvFiles = inputFile.listFiles();
		}
		// reader initialization with first CSV file
		loadCSVReader();
	}
	
	// loads next CSV file
	void loadCSVReader() throws IOException {
		// reset
		currentRow = null;
		columns = null;
		// reader initialization with first CSV file
		IOUtils.closeQuietly(csvReader); // if already exists
		if(csvFileIndex < csvFiles.length) {
			// next CSV file available
			currentRowIndex = 0;
			File csvFile = csvFiles[csvFileIndex++];
			currentCsvFile = csvFile;
			csvReader = NDLDataUtils.readCSVWithMultiline(csvFile, configuration.getSeparator(),
					configuration.getQuote(), configuration.getMultilineLimit());
			columns = csvReader.readNext(); // columns
			if(columns == null) {
				// loads next one
				loadCSVReader();
			} else {
				// try data
				currentRow = csvReader.readNext(); // current row
				if(currentRow == null) {
					// loads next one
					loadCSVReader();
				}
			}
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isCompressed() {
		// no compressed version
		return false;
	}
	
	/**
	 * Returns true if next row available otherwise false
	 */
	@Override
	public boolean hasNext() throws IOException, SAXException {
		return currentRow != null; // current data available
	}
	
	/**
	 * Returns next row
	 */
	@Override
	public RowData next() throws IOException, SAXException {
		if(currentRow != null) {
			currentRowIndex++;
			// data available
			RowData row = new RowData();
			row.setRowDetail(currentRowIndex, currentCsvFile.getName());
			int colIndex = 0;
			for(String column : columns) {
				// each cell value
				String value = currentRow[colIndex++];
				if(configuration.isMultipleValueSeparatorAvailable()) {
					// multiple value separator available
					StringTokenizer tokens = new StringTokenizer(value, String.valueOf(multipleValueSeparator));
					while(tokens.hasMoreTokens()) {
						row.addData(column, tokens.nextToken()); // add each value
					}
				} else {
					// single value
					row.addData(column, value);
				}
			}
			// prepares for next row
			currentRow = csvReader.readNext();
			if(currentRow == null) {
				// try next file if exists
				loadCSVReader();
			}
			return row;
		} else {
			// no data available
			return null;
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {
		IOUtils.closeQuietly(csvReader); // last CSV reader close if any
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void turnOffAssetLoadingFlag() {
		// nothing to do with this AIP in this context
	}
}