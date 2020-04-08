package org.iitkgp.ndl.data.compress;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;

/**
 * TAR.GZ Compress data reader
 * @author Debasis
 */
public class TarGZCompressedFileReader implements CompressedDataReader {
	
	File inputFile = null;
	InputStream zipInputStream = null;
	TarArchiveInputStream zipInput = null;
	TarArchiveEntry currentZipEntry = null;
	
	/**
	 * Constructor
	 * @param inputFile input file name
	 */
	public TarGZCompressedFileReader(File inputFile) {
		this.inputFile = inputFile;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void init() throws IOException {
		zipInputStream = new BufferedInputStream(new GZIPInputStream(new FileInputStream(inputFile)));
	    zipInput = new TarArchiveInputStream(zipInputStream);
	    currentZipEntry = zipInput.getNextTarEntry();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public CompressedDataItem next() throws IOException {
		String entryName = null;
		byte[] contents = null;
		while(currentZipEntry != null) {
			if (currentZipEntry.isDirectory()) {
	    		// directory
	    		currentZipEntry = zipInput.getNextTarEntry();
	            continue;
	        } else if(currentZipEntry.isFile()) {
	        	// item found
	        	// item
	        	entryName = currentZipEntry.getName();
	        	contents = getBytes(zipInput);
	        	// next entry
	        	currentZipEntry = zipInput.getNextTarEntry();
	        	break;
	        }
		}
		if(entryName != null && contents != null) {
			return new CompressedDataItem(entryName, contents);
		} else {
			// no more entry found
			return null;
		}
	}
	
	// gets entry byte contents for current entry
	byte[] getBytes(TarArchiveInputStream in) throws IOException {
		
		ByteArrayOutputStream temp_out = new ByteArrayOutputStream();
		
		byte[] bytes = new byte[1024];
		int l = 0;
		while((l = in.read(bytes)) != -1) {
			temp_out.write(bytes, 0, l);
		}
		
		byte[] contents = temp_out.toByteArray();
		temp_out.close();
		
		return contents;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {
		IOUtils.closeQuietly(zipInputStream);
		IOUtils.closeQuietly(zipInput);
	}

}