package org.iitkgp.ndl.data.writer;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import javax.xml.transform.TransformerException;

import org.apache.commons.io.IOUtils;
import org.iitkgp.ndl.data.NDLDataItem;
import org.iitkgp.ndl.data.compress.CompressedDataWriter;
import org.iitkgp.ndl.data.compress.CompressedFileMode;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * NDL data item writer. This writing can be in flat format (folder format) or in compressed form.
 * See {@link NDLDataUtils#getCompressedDataWriter(String, String, CompressedFileMode)}
 * @param <D> which data type to write
 * @see DataWriter
 */
public class NDLDataItemWriter<D extends NDLDataItem> implements DataWriter<D> {
	
	boolean compress = false;
	File outputLocation; // where to write
	// compression logic
	String compressedFileName;
	CompressedFileMode mode;
	
	// compressed writer
	CompressedDataWriter compressedDataWriter = null;
	
	/**
	 * Constructor
	 * @param outputLocation Where to write data
	 */
	public NDLDataItemWriter(String outputLocation) {
		this.outputLocation = new File(outputLocation);
	}
	
	/**
	 * Constructor
	 * @param outputLocation Where to write data
	 * @param subLocation sub location
	 */
	public NDLDataItemWriter(String outputLocation, String subLocation) {
		this.outputLocation = new File(outputLocation, subLocation);
	}
	
	/**
	 * Constructor
	 * @param outputLocation Where to write data
	 */
	public NDLDataItemWriter(File outputLocation) {
		this.outputLocation = outputLocation;
	}
	
	/**
	 * <pre>Sets compression flag on with file name and which type compression logic is required.</pre>
	 * <pre>This method should be called before {@link #init()}</pre>
	 * See {@link NDLDataUtils#getCompressedDataWriter(String, String, CompressedFileMode)}
	 * {@link CompressedFileMode}
	 * @param compressedFileName compressed file name
	 * @param mode compression mode, see {@link CompressedFileMode}
	 */
	public void setCompressOn(String compressedFileName, CompressedFileMode mode) {
		compress = true;
		this.compressedFileName = compressedFileName;
		this.mode = mode;
	}
	
	/**
	 * {@inheritDoc}
	 */
	public void init() throws IOException {
		if(compress) {
			compressedDataWriter = NDLDataUtils.getCompressedDataWriter(outputLocation, compressedFileName, mode);
			compressedDataWriter.init();
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	public void write(NDLDataItem item) throws IOException {
		Map<String, byte[]> contents = null;
		try {
			contents = item.getContents();
		} catch(TransformerException ex) {
			// XML conversion error
			throw new IOException(ex.getMessage(), ex);
		}
		if(compress) {
			compressedDataWriter.write(contents);
		} else {
			// flat file structure
			NDLDataUtils.writeItems(contents, outputLocation);
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	public void close() throws IOException {
		if(compress) {
			IOUtils.closeQuietly(compressedDataWriter);
		}
	}
	
	/**
	 * gets compressed file
	 * @return returns compressed file
	 */
	public File getCompressedFile() {
		if(compressedDataWriter == null) {
			throw new IllegalStateException("Call it after init method.");
		}
		return compressedDataWriter.file();
	}
}