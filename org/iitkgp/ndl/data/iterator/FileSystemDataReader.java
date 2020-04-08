package org.iitkgp.ndl.data.iterator;

import java.io.File;
import java.io.IOException;
import java.util.Stack;

import org.iitkgp.ndl.data.Filter;

/**
 * File system data reader
 * @author Debasis
 */
public class FileSystemDataReader implements DataReader {
	
	File inputLocation = null;
	Stack<File> files = new Stack<File>();
	Filter<File> filter = null;
	
	/**
	 * Constructor
	 * @param inputLocation input location
	 */
	public FileSystemDataReader(String inputLocation) {
		this.inputLocation = new File(inputLocation);
	}
	
	/**
	 * Constructor
	 * @param inputLocation input location
	 * @param filter filter logic for file selection
	 */
	public FileSystemDataReader(String inputLocation, Filter<File> filter) {
		this.inputLocation = new File(inputLocation);
		this.filter = filter;
	}
	
	/**
	 * Constructor
	 * @param inputLocation input location
	 */
	public FileSystemDataReader(File inputLocation) {
		this.inputLocation = inputLocation;
	}
	
	/**
	 * Constructor
	 * @param inputLocation input location
	 * @param filter filter logic for file selection
	 */
	public FileSystemDataReader(File inputLocation, Filter<File> filter) {
		this.inputLocation = inputLocation;
		this.filter = filter;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void init() throws IOException {
		if(!inputLocation.isDirectory()) {
			throw new IOException(inputLocation.getAbsolutePath() + " is not a directory.");
		}
		File fileList[] = inputLocation.listFiles();
		int l = fileList.length;
		for(int i = l - 1; i >= 0; i--) {
			// reverse order
			File file = fileList[i];
			files.push(file);
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public FileDataItem next() throws IOException {
		if(files.isEmpty()) {
			// no more entry available
			return null;
		}
		File file = files.pop();
		if(file.isDirectory()) {
			// look for next leaf entry
			File fileList[] = file.listFiles();
			int l = fileList.length;
			for(int i = l - 1; i >= 0; i--) {
				// reverse order
				File f = fileList[i];
				files.push(f);
			}
			return next(); // recursive call
		} else if(file.isFile()) {
			// normal leaf entry
			if(filter != null) {
				if(filter.filter(file)) {
					return new FileDataItem(file);
				} else {
					return next(); // recursive call
				}
			} else {
				return new FileDataItem(file);
			}
		} else {
			return next(); // recursive call
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {
		// blank
		files.removeAllElements(); // clear stack if premature termination happens
	}
}