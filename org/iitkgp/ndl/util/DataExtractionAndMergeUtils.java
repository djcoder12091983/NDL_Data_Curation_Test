package org.iitkgp.ndl.util;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.Filter;
import org.iitkgp.ndl.data.NDLDataItem;
import org.iitkgp.ndl.data.NDLDataPair;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.compress.CompressedDataItem;
import org.iitkgp.ndl.data.compress.TarGZCompressedFileReader;
import org.iitkgp.ndl.data.compress.TarGZCompressedFileWriter;
import org.iitkgp.ndl.data.exception.NDLMultivaluedException;
import org.iitkgp.ndl.data.iterator.AIPDataIterator;
import org.iitkgp.ndl.data.iterator.AbstractNDLDataIterator;
import org.iitkgp.ndl.data.iterator.SIPDataIterator;

/**
 * This utilities help to extract data for curation and after curation merge the data with original structure.
 * Currently SIP/AIP data comes with 'data', 'sh_file' and 'collection_structure'.
 * So to curate data system needs data folder and then after curation on data then merges with collection_structure
 * <pre>This utilities works with compressed data(tar.gz)</pre>
 * @author Debasis
 */
public class DataExtractionAndMergeUtils {
	
	static SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy.MMM.dd.HH.mm.ss");
	static long DISPLAY_THRESHOLD_LIMIT = 50000;
	
	/**
	 * Extracts the data part and other data and placed in 'outputLocation' in compressed structure(tar.gz)
	 * It removes root/parent entry
	 * @param inputFile input file to extract from
	 * @param outputLocation where to extracted data to be placed
	 * @param sourceName source name (file logical name)
	 * @throws Exception throws error in case of errors
	 * @see #extractFromTarGz(String, String, String, boolean)
	 */
	public static void extractFromTarGz(String inputFile, String outputLocation, String sourceName) throws Exception {
		extractFromTarGz(inputFile, outputLocation, sourceName, true);
	}
	
	/**
	 * Extracts the data part and other data and placed in 'outputLocation' in compressed structure(tar.gz)
	 * @param inputFile input file to extract from
	 * @param outputLocation where to extracted data to be placed
	 * @param sourceName source name (file logical name)
	 * @param removeParent flag to determine whether remove root/parent entry
	 * @throws Exception throws error in case of errors
	 */
	public static void extractFromTarGz(String inputFile, String outputLocation, String sourceName,
			boolean removeParent) throws Exception {
		
		System.out.println("Start....");
		
		int dc = 0;
		int oc = 0;
		int c = 0;
		long start = System.currentTimeMillis();
		
		TarGZCompressedFileReader reader = null;
		TarGZCompressedFileWriter dataWriter = null;
		TarGZCompressedFileWriter othersWriter = null;
		try {
			reader = new TarGZCompressedFileReader(new File(inputFile));
			reader.init();
			dataWriter = new TarGZCompressedFileWriter(outputLocation, getFileName(sourceName + ".data"));
			othersWriter = new TarGZCompressedFileWriter(outputLocation, getFileName(sourceName + ".others"));
			dataWriter.init();
			othersWriter.init();
			CompressedDataItem item = null;
			while((item = reader.next()) != null) {
				String name = item.getEntryName();
				name = removeParent ? removeParentEntry(name) : name;
				byte[] bytes = item.getContents();
				if(name.startsWith("data/") || name.contains("/data/")) {
					// data part
					dataWriter.write(name, bytes);
					dc++;
				} else {
					// others
					othersWriter.write(name, bytes);
					oc++;
				}
				if(++c % DISPLAY_THRESHOLD_LIMIT == 0) {
					System.out.println("Processed: " + c + " entries");
				}
			}
		} finally {
			reader.close();
			dataWriter.close();
			othersWriter.close();
		}
		
		long end = System.currentTimeMillis();
		System.out.println(CommonUtilities.durationMessage(end - start));
		System.out.println("Data entry count: " + dc + ", Others entry count: " + oc);
		System.out.println("Done.");
	}
	
	// removes parent entry
	static String removeParentEntry(String entry) {
		return entry.substring(entry.indexOf('/') + 1);
	}
	
	// gets logical file name
	static String getFileName(String logicalName) {
		return DATE_FORMATTER.format(new Date()) + "." + logicalName;
	}
	
	/**
	 * After {@link #extractFromTarGz(String, String, String, boolean)} two files obtained 'data' and 'others'.
	 * Then 'data' is curated and supposed to merged with 'others'. That's what this AIP is for.
	 * @param dataFile curated data file (compressed tar.gz file)
	 * @param othersFile others file structure
	 * @param outputLocation where to merged data to be placed
	 * @param sourceName source name (file logical name)
	 * @throws Exception throws error in case of merging error occurs
	 */
	public static void mergeDataWithOriginalStructure(String dataFile, String othersFile, String outputLocation,
			String sourceName) throws Exception {
		
		System.out.println("Start.");
		
		int c = 0;
		long start = System.currentTimeMillis();
		
		TarGZCompressedFileReader readers[] = new TarGZCompressedFileReader[2];
		TarGZCompressedFileWriter writer = null;
		try {
			readers[0] = new TarGZCompressedFileReader(new File(dataFile));
			readers[1] = new TarGZCompressedFileReader(new File(othersFile));
			readers[0].init();
			readers[1].init();
			writer = new TarGZCompressedFileWriter(outputLocation, getFileName(sourceName + ".merged.data"));
			writer.init();
			for(TarGZCompressedFileReader reader : readers) {
				// merging
				CompressedDataItem item = null;
				while((item = reader.next()) != null) {
					String name = item.getEntryName();
					byte[] bytes = item.getContents();
					writer.write(name, bytes);
					
					if(++c % DISPLAY_THRESHOLD_LIMIT == 0) {
						System.out.println("Processed: " + c + " entries");
					}
				}
			}
			
			long end = System.currentTimeMillis();
			System.out.println(CommonUtilities.durationMessage(end - start));
			System.out.println("Done.");
			
		} finally {
			readers[0].close();
			readers[1].close();
			writer.close();
		}
	}
	
	/**
	 * This class encapsulates data merge detail
	 *  @param <D> data type (SIP/AIP etc.)
	 */
	public static class NDLDataMerge <D extends NDLDataItem> {
		
		File file;
		Filter<D> filter;
		
		/**
		 * constructor
		 * @param file file to read
		 * @param filter filter data from file
		 */
		public NDLDataMerge(File file, Filter<D> filter) {
			this.file = file;
			this.filter = filter;
		}
		
		/**
		 * constructor
		 * @param file file to read with no filter
		 */
		public NDLDataMerge(File file) {
			this.file = file;
		}
		
		/**
		 * constructor
		 * @param file file to read
		 * @param filter filter data from file
		 */
		public NDLDataMerge(String file, Filter<D> filter) {
			this.file = new File(file);
			this.filter = filter;
		}
		
		/**
		 * constructor
		 * @param file file to read with no filter
		 */
		public NDLDataMerge(String file) {
			this.file = new File(file);
		}
	}
	
	// merge the data
	private static <D extends NDLDataItem> NDLDataPair<Long> merge(AbstractNDLDataIterator<D> datai, Filter<D> filter,
			TarGZCompressedFileWriter out, Set<String> handles, long dlimit) throws Exception {
		
		long tc = 0;
		long skipped = 0;
		while(datai.hasNext()) {
			D d = datai.next();
			boolean f = false;
			if(filter != null) {
				if(filter.filter(d)) {
					// filtered data
					f = true;
				}
			} else {
				// no filter
				f = true;
			}
			
			if(f) {
				// duplicate handle ID check
				String id = d.getId();
				if(handles != null && handles.contains(id)) {
					throw new NDLMultivaluedException(id + " is repeated.");
				}
				handles.add(id);
				// added
				out.write(d.getContents());
			} else {
				// skipped
				 skipped++;
			}
			
			tc++;
			
			if(tc % dlimit == 0) {
				System.out.println("Processed: " + tc);
			}
		}
		
		// count detail
		return new NDLDataPair<Long>(tc, skipped);
	}
	
	/**
	 * Merge multiple SIP data inputs
	 * @param out out location
	 * @param name source logical name
	 * @param duplicateHandleIDCheck flag to check duplicate handles or not
	 * @param inputs multiple SIP data inputs
	 * @return returns total data count
	 * @throws Exception throws exception in case of errors
	 */
	public static long mergeSIP(String out, String name, boolean duplicateHandleIDCheck,
			NDLDataMerge<SIPDataItem>... inputs) throws Exception {
		return mergeSIP(new File(out), name, duplicateHandleIDCheck, inputs);
	}
	
	/**
	 * Merge multiple SIP data inputs
	 * @param out out location
	 * @param name source logical name
	 * @param duplicateHandleIDCheck flag to check duplicate handles or not
	 * @param inputs multiple SIP data inputs
	 * @return returns total data count
	 * @throws Exception throws exception in case of errors
	 */
	public static long mergeSIP(File out, String name, boolean duplicateHandleIDCheck, NDLDataMerge<SIPDataItem>... inputs)
			throws Exception {
		
		if(inputs.length <= 1) {
			// error
			throw new IllegalArgumentException("At least more than one input(s) expected.");
		}
		
		long tc = 0;
		
		// writer details
		TarGZCompressedFileWriter writer = new TarGZCompressedFileWriter(out,
				NDLDataUtils.getSourceFullFileName(name, "merged", false));
		writer.init();
		
		// data processing display limit
		long dlimit = Long.parseLong(NDLConfigurationContext.getConfiguration("process.display.threshold.limit"));
		
		Set<String> handles = null;
		if(duplicateHandleIDCheck) {
			handles = new HashSet<>(); // duplicate handles tracker;
		}
		
		try {
			// inputs iteration
			for(NDLDataMerge<SIPDataItem> input : inputs) {
				System.out.println("Processing: " + input.file.getAbsolutePath());
				
				File file = input.file;
				SIPDataIterator sipi = new SIPDataIterator(file);
				sipi.init();
				try {
					// merge data
					NDLDataPair<Long> c = merge(sipi, input.filter, writer, handles, dlimit);
					tc += c.first() - c.second(); // total accepted
					
					System.out.println("Found: " + c.first() + " Skipped: " + c.second());
				} finally {
					sipi.close();
				}
			}
			
			// total data count
			System.out.println("Total data count: " + tc);
		} finally {
			writer.close();
		}
		
		return tc;
	}
	
	/**
	 * Merge multiple AIP data inputs
	 * @param out out location
	 * @param name source logical name
	 * @param duplicateHandleIDCheck flag to check duplicate handles or not
	 * @param inputs multiple AIP data inputs
	 * @return returns total data count
	 * @throws Exception throws exception in case of errors
	 */
	public static long mergeAIP(String out, String name, boolean duplicateHandleIDCheck,
			NDLDataMerge<AIPDataItem>... inputs) throws Exception {
		return mergeAIP(new File(out), name, duplicateHandleIDCheck, inputs);
	}
	
	/**
	 * Merge multiple AIP data inputs
	 * @param out out location
	 * @param name source logical name
	 * @param duplicateHandleIDCheck flag to check duplicate handles or not
	 * @param inputs multiple AIP data inputs
	 * @return returns total data count
	 * @throws Exception throws exception in case of errors
	 */
	public static long mergeAIP(File out, String name, boolean duplicateHandleIDCheck,
			NDLDataMerge<AIPDataItem>... inputs) throws Exception {
		
		if(inputs.length <= 1) {
			// error
			throw new IllegalArgumentException("At least more than one input(s) expected.");
		}
		
		long tc = 0;
		
		// writer details
		TarGZCompressedFileWriter writer = new TarGZCompressedFileWriter(out,
				NDLDataUtils.getSourceFullFileName(name, "merged", false));
		writer.init();
		
		// data processing display limit
		long dlimit = Long.parseLong(NDLConfigurationContext.getConfiguration("process.display.threshold.limit"));
		
		Set<String> handles = null;
		if(duplicateHandleIDCheck) {
			handles = new HashSet<>(); // duplicate handles tracker;
		}
		
		try {
			// inputs iteration
			for(NDLDataMerge<AIPDataItem> input : inputs) {
				System.out.println("Processing: " + input.file.getAbsolutePath());
				
				File file = input.file;
				AIPDataIterator sipi = new AIPDataIterator(file);
				try {
					// merge data
					NDLDataPair<Long> c = merge(sipi, input.filter, writer, handles, dlimit);
					tc += c.first() - c.second(); // total accepted
					
					System.out.println("Found: " + c.first() + " Skipped: " + c.second());
				} finally {
					sipi.close();
				}
			}
		} finally {
			writer.close();
		}
		
		return tc;
	}

	// test
	public static void main(String[] args) throws Exception {
		/*String inputFile = "/home/dspace/debasis/NDL/NDL_sources/HAL/in/HAL-Export-08.08.2019.tar.gz";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/HAL/out";
		extractFromTarGz(inputFile, outputLocation, "hal.raw");*/
		
		/*String dataFile = "/home/dspace/debasis/NDL/generated_xml_data/SCRIP/out/2018.Nov.01.16.00.30.SCIRP.V2/2018.Nov.01.16.00.30.SCIRP.V2.Corrected.tar.gz";
		String othersFile = "/home/dspace/debasis/NDL/generated_xml_data/SCRIP/in/2018.Oct.31.11.40.24.SCIRP.others.tar.gz";
		String outputLocation = "/home/dspace/debasis/NDL/generated_xml_data/SCRIP/ingest";
		mergeDataWithOriginalStructure(dataFile, othersFile, outputLocation, "SCIRP.V2");*/
		
		String f1 = "/home/dspace/debasis/NDL/NDL_sources/NPTEL/in/2019.Nov.06.16.58.41.NPTEL.V3.Corrected.tar.gz";
		String f2 = "/home/dspace/debasis/NDL/NDL_sources/NPTEL/in/2019.Nov.07.11.46.06.NPTEL.WEB.V2.Corrected.tar.gz";
		
		String out = "/home/dspace/debasis/NDL/NDL_sources/NPTEL/in";
		
		mergeSIP(out, "NPTL.full", true, new NDLDataMerge<SIPDataItem>(f1), new NDLDataMerge<SIPDataItem>(f2));
	}
}