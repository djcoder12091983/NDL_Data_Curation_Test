package org.iitkgp.ndl.data.asset;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;

import javax.imageio.ImageIO;

import org.apache.commons.io.IOUtils;
import org.iitkgp.ndl.data.Handler;
import org.iitkgp.ndl.data.compress.TarGZCompressedFileReader;
import org.iitkgp.ndl.data.compress.TarGZCompressedFileWriter;
import org.iitkgp.ndl.data.iterator.DataItem;
import org.iitkgp.ndl.data.iterator.DataReader;
import org.iitkgp.ndl.data.iterator.FileSystemDataReader;
import org.iitkgp.ndl.util.CommonUtilities;
import org.iitkgp.ndl.util.NDLDataUtils;

import com.opencsv.CSVWriter;

/**
 * Thumbnail asset JPEG conversion
 * @author Debasis
 */
public class AssetThumbnailJPEGConversion {
	
	static String JPEG =  "jpeg";
	static String NDL_JPEG =  "jpg";
	static int DISPLAY_LIMIT = 10000;
	
	String inputFile = null;
	String outLocation = null;
	
	int sampleFiles2Check = 100; // sample files to check for confirmation
	
	Handler<byte[], byte[]> errorHandler = null; // error handler
	Handler<byte[], byte[]> conversionHandler = null; // conversion handler
	
	AssetThumbnailJPEGConversion(String inputFile, String outLocation) {
		this.inputFile = inputFile;
		this.outLocation = outLocation;
	}
	
	AssetThumbnailJPEGConversion(String inputFile, String outLocation, Handler<byte[], byte[]> conversionHandler) {
		this.inputFile = inputFile;
		this.outLocation = outLocation;
		this.conversionHandler = conversionHandler;
	}
	
	AssetThumbnailJPEGConversion(String inputFile, String outLocation, Handler<byte[], byte[]> conversionHandler,
			Handler<byte[], byte[]> errorHandler) {
		this.inputFile = inputFile;
		this.outLocation = outLocation;
		this.errorHandler = errorHandler;
		this.conversionHandler = conversionHandler;
	}
	
	/**
	 * Sets sample files conversion number for checking
	 * @param sampleFiles2Check file numbers to check
	 */
	public void setSampleFiles2Check(int sampleFiles2Check) {
		this.sampleFiles2Check = sampleFiles2Check;
	}
	
	// default conversion handler
	Handler<byte[], byte[]> defaultConversionHandler = new Handler<byte[], byte[]>() {
		
		// default conversion logic
		@Override
		public byte[] handle(byte[] input) throws Exception {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			try {
				BufferedImage image = ImageIO.read(new ByteArrayInputStream(input));
				ImageIO.write(image, JPEG, out);
				byte[] output = out.toByteArray();
				return output;
			} catch(Exception ex) {
				// error handling
				throw ex;
			} finally {
				// close resource
				IOUtils.closeQuietly(out);
			}
		}
	};
	
	// error handler if conversion fails, fall back strategy
	public void setErrorHandler(Handler<byte[], byte[]> errorHandler) {
		this.errorHandler = errorHandler;
	}
	
	// JPEG conversion logic
	void convert(String filePrefix) throws Exception {
		System.out.println("Processing starts ....");
		
		// sample files location
		File in = new File(outLocation, "sample_files_in");
		File normal = new File(outLocation, "sample_files_out_1");
		File fallback = new File(outLocation, "sample_files_out_2");
		in.mkdirs();
		normal.mkdirs();
		fallback.mkdirs();
		
		// counters
		int normalc = 0;
		int fallbackc = 0;
		
		CSVWriter error = NDLDataUtils.openCSV(new File(outLocation, filePrefix + ".JPEG.thumnails.error.csv"), ',', '"');
		error.writeNext(new String[]{"Error File", "Error Message"});
		TarGZCompressedFileWriter writer = new TarGZCompressedFileWriter(outLocation, filePrefix + ".JPEG.thumnails");
		writer.init(); // writer initialization
		File input = new File(inputFile);
		DataReader reader = null;
		// specific reader loading
		if(input.isFile() && input.getName().endsWith("tar.gz")) {
			reader = new TarGZCompressedFileReader(input) ;
		} else if(input.isDirectory()) {
			reader = new FileSystemDataReader(input);
		}
		reader.init(); // reader initialization
		int c = 0, errorCounter = 0;
		long start = System.currentTimeMillis();
		DataItem data = null;
		while((data = reader.next()) != null) {
			String name = data.getEntryName();
			name = name.substring(name.lastIndexOf('/') + 1);
			int p = name.lastIndexOf('.');
			String ext = null;
			if(p != -1) {
				// remove extension
				ext = name.substring(p);
				name = name.substring(0, p);
			}
			byte contents[] = data.getContents();
			if(contents.length == 0) {
				// invalid item
				continue;
			}
			boolean samplef = false;
			byte[] outputContents = null;
			try {
				if(conversionHandler != null) {
					// custom handler
					outputContents = conversionHandler.handle(contents);
				} else {
					// default handler, fall back
					outputContents = defaultConversionHandler.handle(contents);
				}
				if(++normalc <= sampleFiles2Check) {
					// normal case
					IOUtils.write(outputContents, new FileOutputStream(new File(normal, name + "." + NDL_JPEG)));
					samplef = true;
				}
			} catch(Exception ex) {
				// error
				//System.err.println(data.getEntryName() + " Processing image Error: " + ex.getMessage());
				// error handler
				if(errorHandler != null) {
					// error handler
					try {
						outputContents = errorHandler.handle(contents);
						if(++fallbackc <= sampleFiles2Check) {
							// normal case
							IOUtils.write(outputContents, new FileOutputStream(new File(fallback, name + "." + NDL_JPEG)));
							samplef = true;
						}
					} catch(Exception excp) {
						// excp.printStackTrace(System.out);
						// still error
						System.err.println(data.getEntryName() + " Still processing image Error: " + excp.getMessage());
						errorCounter++;
						error.writeNext(new String[]{data.getEntryName(), ex.getMessage()});
					}
				} else {
					// not error handler
					errorCounter++;
					error.writeNext(new String[]{data.getEntryName(), ex.getMessage()});
				}
			} finally {
				// final action
				if(outputContents != null) {
					// out contents
					Map<String, byte[]> outContents = new HashMap<String, byte[]>(2);
					outContents.put(filePrefix + "/" + name + "." + NDL_JPEG, outputContents);
					// write
					writer.write(outContents);
				}
				if(samplef) {
					// sample out file written
					IOUtils.write(contents, new FileOutputStream(new File(in, name + "." + ext)));
				}
			}
			
			if(++c % DISPLAY_LIMIT == 0) {
				// track counter and display
				System.out.println("Processed: " + c + " Failed: " + errorCounter);
				System.out.println(CommonUtilities.durationMessage(System.currentTimeMillis() - start));
			}
		}
		
		System.out.println("Processed: " + c + " Failed: " + errorCounter);
		System.out.println(CommonUtilities.durationMessage(System.currentTimeMillis() - start));
		
		reader.close();
		writer.close();
		error.close();
	}
	
	public static void main(String[] args) throws Exception {
		String inputFile = "/home/dspace/debasis/NDL/IAR/raw_data/research_article/assets/RA.thumbnails.tar.gz";
		String outLocation = "/home/dspace/debasis/NDL/IAR/raw_data/research_article/assets/";
		
		AssetThumbnailJPEGConversion p = new AssetThumbnailJPEGConversion(inputFile, outLocation);
		// error handler for GIF, for other image 
		/*p.setErrorHandler(new Handler<byte[], byte[]>() {
			// fall back strategy
			@Override
			public byte[] handle(byte[] input) throws Exception {
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				try {
					// give a second try
					GifImage gif = GIFDecoder.read(input);
					BufferedImage image = gif.getFrame(0);
					ImageIO.write(image, JPEG, out);
					byte[] output = out.toByteArray();
					return output;
				} finally {
					IOUtils.closeQuietly(out);
				}
				
			}
		});*/
		p.convert("JSTOR");
		
		System.out.println("Done.");
	}
}