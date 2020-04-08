package org.iitkgp.ndl.data.compress;

import org.apache.commons.io.IOUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.iterator.DataSourceNULLConfiguration;
import org.iitkgp.ndl.data.iterator.SIPDataIterator;
import org.iitkgp.ndl.data.writer.NDLDataItemWriter;

/**
 * SIP Abbreviation utility of TAR.GZ if entry name is too long
 * and make sure abbreviation does not affect data
 * @author Debasis
 */
public class SIPTarGZAbbreviator
		extends TarGZAbbreviator<SIPDataItem, SIPDataIterator, NDLDataItemWriter<SIPDataItem>> {
	
	/**
	 * Constructor
	 * @param input input source
	 * @param outLocation output location
	 * @param name source logical name
	 */
	public SIPTarGZAbbreviator(String input, String outLocation, String name) {
		super(input, outLocation, name);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void init() throws Exception {
		reader = new SIPDataIterator(input);
		reader.init(new DataSourceNULLConfiguration());
		writer = new NDLDataItemWriter<SIPDataItem>(outLocation);
		writer.setCompressOn(name + "." + namePrefix, CompressedFileMode.TARGZ);
		writer.init();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void destroy() throws Exception {
		reader.close();
		IOUtils.closeQuietly(writer);
	}
}