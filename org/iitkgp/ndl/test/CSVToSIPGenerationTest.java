package org.iitkgp.ndl.test;

import org.iitkgp.ndl.data.RowData;
import org.iitkgp.ndl.data.generation.NDLCSVToSIPGeneration;

/**
 * Testing of {@link NDLCSVToSIPGeneration}
 * @author Debasis
 *
 */
public class CSVToSIPGenerationTest extends NDLCSVToSIPGeneration {
	
	// constructor
	public CSVToSIPGenerationTest(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	// generation logic
	@Override
	protected boolean generateTargetItem(RowData csv, RowData target) throws Exception {
		/*String id = csv.getSingleData("id"); // ID
		if(containsMappingKey("delete." + id)) {
			// to be deleted
			return false;
		}*/
		
		/*copyCSVColumns("dc.contributor.author", "dc.description", "dc.identifier.uri", "dc.subject", "dc.title",
				"ndl.educationalLevel", "ndl.subject.classification");
		
		copy("dc.identifier.other@alternateContentUri", "dc.identifier.other:alternateContentUri");*/
		
		copyCSVColumns("lrmi.educationalRole");
		
		// success correction
		return true;
	}
	
	// testing
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/Samim/pbslearningmedia_1579690185_mapped_0.csv"; // flat CSV location
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/Samim/temp"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/Samim/temp";
		String name = "csv.sip.test";
		// multiple value separator is pipe
		CSVToSIPGenerationTest p = new CSVToSIPGenerationTest(input, logLocation, outputLocation, name);
		
		//String deleteFile = "<delete file>"; // which has single column contains handles to delete
		//p.addMappingResource(deleteFile, "delete"); // this logical name used to access the handle
		p.turnOnFullHandleIDColumnFlag("_id");
		p.turnOnFullDataCopyFlag();
		
		p.genrateData(); // generates data
		
		System.out.println("Done.");
	}
}