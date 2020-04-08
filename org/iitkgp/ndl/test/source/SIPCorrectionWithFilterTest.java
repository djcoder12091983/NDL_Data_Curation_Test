package org.iitkgp.ndl.test.source;

import java.util.HashSet;
import java.util.Set;

import org.iitkgp.ndl.data.Filter;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class SIPCorrectionWithFilterTest extends NDLSIPCorrectionContainer {
	
	Set<String> uniquet = new HashSet<>();
	
	// constructor
	public SIPCorrectionWithFilterTest(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name, false);
	}
	
	// correction logic
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		// this true does not matter if filter is applied
		/*AssetDetail detail = target.readAsset(NDLAssetType.THUMBNAIL);
		if(detail != null) {
			System.out.println(target.getId() + " => " + detail.getName());
		}*/
		//return target.getId().equals("dli_ndli/316259");
		return true;
	}
	
	// testing
	public static void main(String[] args) throws Exception {
		// flat SIP location or compressed SIP location
		String input = "/home/dspace/debasis/NDL/NDL_sources/DLI/in/2019.Nov.19.12.58.30.DLI.filter.v1.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/DLI/out"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/DLI/out";
		String name = "DLI.filter.test";
		
		SIPCorrectionWithFilterTest p = new SIPCorrectionWithFilterTest(input, logLocation, outputLocation, name);
		//p.turnOffLoadHierarchyFlag();
		
		String filterFile = "/home/dspace/debasis/NDL/NDL_sources/DLI/conf/dli.filter.test1";
		Set<String> filter = NDLDataUtils.loadSet(filterFile);
		
		p.addDataFilter(new Filter<SIPDataItem>() {
			
			@Override
			public boolean filter(SIPDataItem data) {
				return filter.contains(NDLDataUtils.getHandleSuffixID(data.getId()));
				/*String lang = data.getSingleValue("dc.language.iso");
				if(StringUtils.containsAny(lang, "hin", "eng")) {
					String t = data.getSingleValue("dc.title");
					String tokens[] = t.split("\\|");
					if(tokens.length <= 2) {
						// with single pipe or no pipe
						return p.uniquet.add(t); // take unique titles
					}
				}
				return false;*/
				
				//return data.getId().equals("asme/64_gtp_3") || data.getId().equals("asme/64_gtp_4");
			}
		});
		p.processData();
		
		System.out.println("Done.");
	}

}