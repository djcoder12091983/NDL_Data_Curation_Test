package org.iitkgp.ndl.data.compress;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.iitkgp.ndl.data.NDLDataItem;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.iterator.AbstractNDLDataIterator;
import org.iitkgp.ndl.data.writer.NDLDataItemWriter;

/**
 * Abbreviation utilities of TAR.GZ if entry name is too long
 * and make sure abbreviation does not affect data
 * @param <D> data type
 * @param <R> reader
 * @param <W> writer
 * @author Debasis
 */
public abstract class TarGZAbbreviator<D extends NDLDataItem, R extends AbstractNDLDataIterator<D>, W extends NDLDataItemWriter<D>> {
	
	String input;
	String outLocation;
	String name; // logical name of the source
	R reader;
	W writer;
	
	SimpleDateFormat dateFormatter = new SimpleDateFormat("dd.MMM.yyyy");
	String namePrefix = dateFormatter.format(new Date());
	
	/**
	 * Constructor
	 * @param input input source
	 * @param outLocation output location
	 * @param name source logical name
	 */
	public TarGZAbbreviator(String input, String outLocation, String name) {
		this.input = input;
		this.outLocation = outLocation;
		this.name = name;
	}
	
	/**
	 * Initializes the container
	 * @throws Exception throws error in case of initialization fails
	 */
	protected abstract void init() throws Exception;
	
	/**
	 * Destroys the container
	 * @throws Exception throws error in case of initialization fails
	 */
	protected abstract void destroy() throws Exception;
	
	/**
	 * Abbreviates tar.gz data
	 * @throws Exception throws exception in case operation fails
	 */
	public void abbreviate() throws Exception {
		
		init(); // initialization
		long count = 0;
		
		long folderindex = 1;
		while(reader.hasNext()) {
			D item = reader.next();
			Map<String , byte[]> contents = item.getContents();
			Map<String , byte[]> modifiedContents = new HashMap<String, byte[]>();
			for(String name : contents.keySet()) {
				
				byte[] content = contents.get(name);
				String tokens[] = name.split("/");
				int l = tokens.length;
				String modifiedName = this.name + "." + namePrefix + "/" + folderindex + "/" + tokens[l - 1];
				modifiedContents.put(modifiedName, content);
			}
			
			SIPDataItem newItem = new SIPDataItem();
			newItem.load(modifiedContents);
			// write
			writer.write(newItem);
			
			if(++count%10000 == 0) {
				System.out.println("Processed: " + count);
			}
			
			folderindex++; // next folder
		}
		
		System.out.println("Total Processed: " + count);
		
		destroy(); // clean up
	}
}