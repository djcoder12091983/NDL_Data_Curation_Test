package org.iitkgp.ndl.test.source;

import java.util.Set;

import org.iitkgp.ndl.data.Filter;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.split.NDLSIPDataSplitter;
import org.iitkgp.ndl.util.NDLDataUtils;

public class NDLSIPDataSplitterTest extends NDLSIPDataSplitter {

	public NDLSIPDataSplitterTest(String input, String logLocation, String name, String outputLocation) {
		super(input, logLocation, name, outputLocation);
	}
	
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/OA/input/2019.Jul.23.14.02.55.OpenAccess.V8.Corrected.tar.gz";
		String logLocation = "<log_location>";
		String name = "zenodo.v8.split";
		String outLocation = "/home/dspace/debasis/NDL/NDL_sources/OA/input";
		
		Set<String> split = NDLDataUtils.loadSet("");
		
		NDLSIPDataSplitterTest p = new NDLSIPDataSplitterTest(input, logLocation, name, outLocation);
		p.setSplitThreshold(100000); // 1L data split
		p.addDataFilter(new Filter<SIPDataItem>() {
			// skips non-author item
			
			@Override
			public boolean filter(SIPDataItem data) {
				//return data.exists("dc.contributor.author");
				return split.contains(NDLDataUtils.getHandleSuffixID(data.getId()));
			}
		});
		/*p.addTransformer(new NDLSIPDataTransformer() {
			
			@Override
			public void transform(SIPDataItem input) {
				// TODO add transformation if needed	
			}
		});*/
		p.split(); // split data
		
		System.out.println("Done");
	}
}