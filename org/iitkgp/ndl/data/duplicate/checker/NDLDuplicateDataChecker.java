package org.iitkgp.ndl.data.duplicate.checker;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.IOUtils;
import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.data.container.AbstractDataContainer;
import org.iitkgp.ndl.data.iterator.DataSourceNULLConfiguration;
import org.iitkgp.ndl.util.CommonUtilities;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.iitkgp.ndl.util.NDLServiceUtils;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

/**
 * NDL data duplicate checker (DOI etc.) from input field files.
 * <pre>Note: input file should be CSV file</pre> 
 * @author Debasis
 */
public class NDLDuplicateDataChecker extends AbstractDataContainer<DataSourceNULLConfiguration> {
	
	static {
		NDLConfigurationContext.init();
		MAXIMUM_DATA_SPLIT_SIZE = Integer
				.parseInt(NDLConfigurationContext.getConfiguration("ndl.service.duplicate.checker.max.splitsize"));
	}
	
	static int MAXIMUM_DATA_SPLIT_SIZE;
	
	String duplicateField; // DOI etc.
	int threads = 10; // parallel processing factor
	int splitSize = 25000; // each thread will get how much rows
	int startRowIndex = 1; // CSV start row index
	int columnIndex = 0; //  0 index based
	int ndlIDLimit = 2; // NDL duplicates ID limit if found
	
	long lstart = System.currentTimeMillis();

	/**
	 * Constructor
	 * @param input input folder location (assumed all files are in CSV format) or single CSV file
	 * @param logLocation log location
	 * @param duplicateField duplicate field (DOI etc.)
	 */
	public NDLDuplicateDataChecker(String input, String logLocation, String duplicateField) {
		super(input, logLocation);
		this.duplicateField = duplicateField;
	}
	
	/**
	 * Constructor
	 * @param input input folder location (assumed all files are in CSV format) or single CSV file
	 * @param logLocation log location
	 * @param duplicateField duplicate field (DOI etc.)
	 * @param threads parallel processing factor
	 */
	public NDLDuplicateDataChecker(String input, String logLocation, String duplicateField, int threads) {
		this(input, logLocation, duplicateField);
		this.threads = threads;
	}
	
	/**
	 * Sets CSV start row index (0 based)
	 * @param startRowIndex starting row index (0 based)
	 */
	public void setStartRowIndex(int startRowIndex) {
		this.startRowIndex = startRowIndex;
	}
	
	/**
	 * Sets field column index (0 based)
	 * @param columnIndex field column index (0 based)
	 */
	public void setColumnIndex(int columnIndex) {
		this.columnIndex = columnIndex;
	}
	
	/**
	 * Each thread will get how much rows
	 * @param splitSize how much rows assigned to each thread
	 */
	public void setDataSplitSize(int splitSize) {
		if(splitSize > MAXIMUM_DATA_SPLIT_SIZE) {
			throw new IllegalArgumentException("Split size can't exceed: " + MAXIMUM_DATA_SPLIT_SIZE);
		}
		this.splitSize = splitSize;
	}
	
	// this class finds duplicate details
	class DuplicateChecker implements Callable<Long> {
		
		Collection<String> values = new ArrayList<String>(splitSize); // values to check
		// range
		long start;
		long end;
		
		// constructor
		DuplicateChecker(long start, long end) {
			this.start = start;
			this.end = end;
		}
		
		// constructor
		DuplicateChecker(long start, long end, Collection<String> values) {
			this(start, end);
			this.values.addAll(values);
		}
		
		// adds values
		void add(String value) {
			values.add(value);
		}
		
		@Override
		public Long call() throws Exception {
			System.out.println("Checking start: Range[" + start + ", " + end + "]");
			long dcount = 0;
			long localStart = System.currentTimeMillis();
			
			// open a file and write duplicate rows
			String filename = getFileName("duplicate." + duplicateField + ".range[" + start + "," + end + "]");
			CSVWriter writer = null;
			
			try {
				DuplicateDocumentsOutput duplicates = NDLServiceUtils.duplicateChecker(duplicateField, values);
				// iterate
				List<String[]> result = new ArrayList<String[]>();
				for(DuplicateDocument duplicate : duplicates.documents) {
					Collection<String> ndlIDDetails = new ArrayList<String>(ndlIDLimit); // duplicates if found
					Iterator<String> foundNDLIDDetails = duplicate.getNdli_id().iterator();
					int c = 0;
					while(foundNDLIDDetails.hasNext() && ++c <= ndlIDLimit) {
						ndlIDDetails.add(foundNDLIDDetails.next());
					}
					if(!ndlIDDetails.isEmpty()) {
						// duplicate found
						result.add(new String[]{duplicate.value, NDLDataUtils.join(ndlIDDetails, ',')});
						dcount++;
					}
				}
				
				if(!result.isEmpty()) {
					writer = NDLDataUtils.openCSV(new File(logLocation, filename)); // csv writer
					writer.writeNext(new String[]{duplicateField, "NDL_ID(s)"}); // headers
					// write all data
					writer.writeAll(result);
				}
				
			} finally {
				IOUtils.closeQuietly(writer);
			}
			
			long lend = System.currentTimeMillis();
			System.out.println("Checking done for range[" + start + ", " + end + "], duplicate found: " + dcount
					+ ". And time taken: " + CommonUtilities.durationMessage(lend - localStart));
			return dcount; // duplicates count
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processData() throws Exception {
		
		System.out.println("Starting for " + duplicateField + " duplicates check.");
		long dcount = 0; // duplicate count
		
		try {
			// parallel processing
			ExecutorService executor = Executors.newFixedThreadPool(threads);
			List<Future<Long>> results = new ArrayList<Future<Long>>(); // results from task
			
			// CSV file processing
			File[] files;
			File finput = new File(input);
			if(!finput.exists()) {
				// error
				throw new FileNotFoundException(input + " not found.");
			}
			if(finput.isDirectory()) {
				// directory
				// assumed all are CSV files
				files = finput.listFiles();
			} else {
				files = new File[]{finput};
			}
			long start = 1, end = 0; // range
			Collection<String> chunk = new ArrayList<String>(splitSize);
			int c = 0; // count
			for(File file : files) {
				CSVReader reader = null;
				try {
					reader = NDLDataUtils.readCSV(file, startRowIndex);
					String tokens[] = null;
					while((tokens = reader.readNext()) != null) {
						// gets field value by column index
						String fvalue = tokens[columnIndex];
						if(c == splitSize) {
							// one chunk should be submitted to thread
							// task to submit
							DuplicateChecker task = new DuplicateChecker(start, end, chunk);
							results.add(executor.submit(task));
							
							// reset
							c = 0;
							start = end + 1;
							chunk = new ArrayList<String>(splitSize);
						}
						// pointers move
						c++;
						end++;
						// chunk add with field value
						chunk.add(fvalue);
					}
				} finally {
					IOUtils.closeQuietly(reader);
				}
			}
			
			// if anything left
			if(!chunk.isEmpty()) {
				// task to submit
				DuplicateChecker task = new DuplicateChecker(start, end, chunk);
				results.add(executor.submit(task));
			}
			
			// extract result
			for(Future<Long> result : results) {
				dcount += result.get(); // extract result
			}
			
			executor.shutdown(); // terminate
		} finally {
			// close resources
			close();
		}
		
		long lend = System.currentTimeMillis();
		System.out.println("Total duplicates found: " + dcount);
		System.out.println("Total time taken: " + CommonUtilities.durationMessage(lend - lstart));
	}
	
	/**
	 * Checks for duplicates
	 * @throws Exception throws exception in case of errors
	 */
	public void check() throws Exception {
		processData();
	}
}