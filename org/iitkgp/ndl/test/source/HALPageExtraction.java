package org.iitkgp.ndl.test.source;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.NDLSIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

// page extraction HAL
public class HALPageExtraction extends NDLSIPDataContainer {
	
	int pcc = 0;
	int tc = 0;

	public HALPageExtraction(String input, String logLocation, String name) {
		super(input, logLocation, name);
	}
	
	boolean multiplehyphen(String text) {
		int l = text.length();
		int c = 0;
		for(int i = 0; i < l; i++) {
			if(text.charAt(i) == '-') {
				if(++c > 1) {
					return true;
				}
			}
		}
		return c != 1;
	}

	@Override
	protected boolean readItem(SIPDataItem item) throws Exception {
		
		String pc = item.getSingleValue("dc.format.extent:pageCount");
		if(StringUtils.isNotBlank(pc)) {
			pc = pc.replaceAll(" *(p|P)\\.? *", "").replaceAll("-+", "-");
			/*if(item.getId().equals("hal/artxibo_01491771v1")) {
				System.out.println("PC: " + pc);
			}*/
			tc++;
			String tokens[] = pc.split("\\(|\\)|( +)|\\.|,|\\[|\\]");
			int l = tokens.length;
			/*if(item.getId().equals("hal/artxibo_01491771v1")) {
				System.out.println("L: " + l);
			}*/
			boolean f = false;
			for(int i = 0; i < l; i ++) {
				String t = tokens[i];
				String sp = null, ep = null;
				if(!multiplehyphen(t)) {
					if(t.equals("-")) {
						if(i - 1 >= 0 && i + 1 < l) {
							sp = tokens[i - 1];
							ep = tokens[i + 1];
						}
					} else if(t.charAt(0) == '-') {
						// start
						if(i - 1 >= 0) {
							sp = tokens[i - 1];
							ep = t.replace("-", "");
						}
					} else if(t.charAt(t.length() - 1) == '-') {
						// end
						if(i + 1 < l) {
							sp = t.replace("-", "");
							ep = tokens[i + 1];
						}
					} else {
						String tokens1[] = t.split("-");
						if(tokens1.length == 2) {
							// page separation
							sp = tokens1[0];
							ep = tokens1[1];
						}
					}
					
					if(StringUtils.isNotBlank(sp) && StringUtils.isNotBlank(ep)) {
						sp = sp.replaceFirst("^0+", "");
						ep = ep.replaceFirst("^0+", "");
						if(NumberUtils.isDigits(sp) && NumberUtils.isDigits(ep)) {
							//System.out.println("ID: " + item.getId() + " => " + pc);
							int c = Integer.parseInt(ep) - Integer.parseInt(sp) + 1;
							if(c > 0 && c < 750) {
								// pagination limit
								log("hal_pages", new String[] { NDLDataUtils.getHandleSuffixID(item.getId()), pc, sp,
										ep, String.valueOf(c) });
								pcc++;
								f = true;
							}
							break;
						}
					}
				} else if(StringUtils.containsIgnoreCase(t, "page")) {
					if(i - 1 >= 0 && NumberUtils.isDigits(tokens[i - 1])) {
						// page count
						log("hal_pages", new String[] { NDLDataUtils.getHandleSuffixID(item.getId()), pc, null, null,
								tokens[i - 1] });
						pcc++;
						f = true;
					}
				}
			}
			
			if(!f) {
				// wrong pages
				log("hal_wrong_pages", item.getId() + " => " + pc);
			}
		}
		
		return false;
	}
	
	public static void main(String[] args) throws Exception {
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/HAL/in/2019.Sep.12.19.07.36.HAL.V3.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/HAL/out";
		String name = "hal.pages.v5";
		
		HALPageExtraction t = new HALPageExtraction(input, logLocation, name);
		//t.addCSVLogger("journals", new String[]{"Journal", "Handle", "ISSN"});
		t.addCSVLogger("hal_pages", new String[]{"ID", "Text", "Start", "End", "PC"});
		t.addTextLogger("hal_wrong_pages");
		t.processData();
		
		System.out.println("Page count found: " + t.pcc + " out of " + t.tc);
		
		System.out.println("Done.");
	}
}