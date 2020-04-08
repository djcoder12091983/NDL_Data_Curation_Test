package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.container.DataContainerNULLConfiguration;
import org.iitkgp.ndl.data.iterator.DataSourceNULLConfiguration;
import org.iitkgp.ndl.data.validator.NDLSIPDataValidator;

// test of NDLSIPDataValidator
public class NDLSIPDataValidatorTest {
	
	// TEST
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/2019.Dec.26.11.42.07.Compadre_curation.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/out";
		System.out.println("Start.");
		NDLSIPDataValidator checker = new NDLSIPDataValidator(input, logLocation,
				new DataContainerNULLConfiguration<DataSourceNULLConfiguration>(new DataSourceNULLConfiguration()));
		checker.turnOffUniqueFieldValidations();
		/*checker.addDataFilter(new Filter<SIPDataItem>() {
		
			// filter logic
			@Override
			public boolean filter(SIPDataItem data) {
				String handle = data.getId();
				return !handle.contains("/V-") && !handle.contains("/J-");
			}
		});*/
		// custom uniqueness
		//checker.setUniqueField("dc.title");
		//checker.setUniqueField("dc.description.abstract");
		//checker.setUniqueField("dc.description.uri");
		checker.validate();
		System.out.println("End.");
	}
}