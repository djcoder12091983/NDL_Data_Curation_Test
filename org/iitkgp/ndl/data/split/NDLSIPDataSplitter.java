package org.iitkgp.ndl.data.split;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.iitkgp.ndl.data.NDLSIPDataTransformer;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.compress.CompressedFileMode;
import org.iitkgp.ndl.data.container.AbstractNDLDataContainer;
import org.iitkgp.ndl.data.container.DataContainerNULLConfiguration;
import org.iitkgp.ndl.data.iterator.DataIterator;
import org.iitkgp.ndl.data.iterator.DataSourceNULLConfiguration;
import org.iitkgp.ndl.data.iterator.SIPDataIterator;
import org.iitkgp.ndl.data.writer.NDLDataItemWriter;

/**
 * NDL SIP data splitter by given split threshold limit (default value is 25,000), see {@link #setSplitThreshold(long)}
 * @author Debasis
 */
public class NDLSIPDataSplitter extends
		AbstractNDLDataContainer<SIPDataItem, DataIterator<SIPDataItem, DataSourceNULLConfiguration>, DataSourceNULLConfiguration> {
	
	String outputLocation; // output location
	long splitThreshold = 25000; // default value
	// writers
	NDLDataItemWriter<SIPDataItem> writer = null;
	NDLDataItemWriter<SIPDataItem> skippedWriter = null;
	long counter = 0;
	int fileIndex = 0;
	
	// transformer
	NDLSIPDataTransformer transformer = null;
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param name logical name to add prefix to log files
	 * @param outputLocation output location where splitted data to be stored
	 */
	public NDLSIPDataSplitter(String input, String logLocation, String name, String outputLocation) {
		// no validation required so last parameter is false
		super(input, logLocation, name,
				new DataContainerNULLConfiguration<DataSourceNULLConfiguration>(new DataSourceNULLConfiguration()), false);
		this.outputLocation = outputLocation;
		dataReader = new SIPDataIterator(input);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void init(DataContainerNULLConfiguration<DataSourceNULLConfiguration> configuration) throws IOException {
		// super call
		super.init(configuration);
		// writers initialization
		initWriter();
		initSkippedWriter();
	}
	
	/**
	 * Adds transformer to change data if required
	 * @param transformer given transformer
	 */
	public void addTransformer(NDLSIPDataTransformer transformer) {
		this.transformer = transformer;
	}
	
	// skipped data writer
	void initSkippedWriter() throws IOException {
		String name = getFileName("skipped.data");
		skippedWriter = new NDLDataItemWriter<SIPDataItem>(outputLocation, name); // writer
		// handle compression mode for writing
		if(isCompressed()) {
			// compression mode is ON
			skippedWriter.setCompressOn(name, CompressedFileMode.TARGZ);
		}
		skippedWriter.init(); // initialization writer
	}
	
	// writer initialization for each group
	void initWriter() throws IOException {
		String name = getFileName("data." + ++fileIndex);
		writer = new NDLDataItemWriter<SIPDataItem>(outputLocation, name); // writer
		// handle compression mode for writing
		if(isCompressed()) {
			// compression mode is ON
			writer.setCompressOn(name, CompressedFileMode.TARGZ);
		}
		writer.init(); // initialization writer
	}
	
	/**
	 * Sets split threshold (default value is 25000)
	 * @param splitThreshold split threshold
	 */
	public void setSplitThreshold(long splitThreshold) {
		this.splitThreshold = splitThreshold;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean processItem(SIPDataItem item) throws Exception {
		if(transformer != null) {
			// transformation if needed
			transformer.transform(item);
		}
		return true; // passes all data
	}
	
	/**
	 * Splits data
	 * See {@link #processData()}
	 * @throws Exception throws exception in case of errors occur
	 */
	public void split() throws Exception {
		processData();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void postProcessItem(SIPDataItem item) throws Exception {
		// group data by threshold limit
		if(counter++ == splitThreshold) {
			System.out.println("Splitted: " + counter);
			// close existing writer and open new writer for new group
			writer.close();
			initWriter(); // writer initialization
			counter = 0; // reset counter
		}
		
		// write valid item
		writer.write(item);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void postProcessSkippedItem(SIPDataItem skippedItem) throws Exception {
		// skipped data
		skippedWriter.write(skippedItem);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {
		// super call
		super.close();
		// close writers
		IOUtils.closeQuietly(writer);
		IOUtils.closeQuietly(skippedWriter);
	}
}