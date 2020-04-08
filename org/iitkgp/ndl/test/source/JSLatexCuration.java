package org.iitkgp.ndl.test.source;

import java.util.Collection;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.data.normalizer.NDLDataNormalizer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class JSLatexCuration extends NDLSIPCorrectionContainer {
	
	/*Map<String, String> lrtmap;
	long lrtfound = 0;*/
	
	public JSLatexCuration(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		normalize("dc.title", "dc.description", "dc.coverage.spatial", "dc.subject"); // normalize latex
		/*List<String> values = target.getValue("dc.subject.mesh");
		if(isAllUnassigned("lrmi.learningResourceType")) {
			// unassigned only
			OUTER:
			for(String value : values) {
				String tokens[] = value.split(" +");
				for(String token : tokens) {
					for(String key : lrtmap.keySet()) {
						if(StringUtils.containsIgnoreCase(token, key)) {
							// LRT Found
							lrtfound++;
							target.add("lrmi.learningResourceType", lrtmap.get(key));
							break OUTER;
						}
					}
				}
			}
		}*/
		
		return true;
	}
	
	/*@Override
	protected void intermediateProcessHandler() {
		// super call
		super.intermediateProcessHandler();
		// lrt mapping
		System.out.println("Total LRT found: " + lrtfound);
	}*/
	
	class LatexNormalizer extends NDLDataNormalizer {
		
		@Override
		public Collection<String> transform(String input) {
			return NDLDataUtils.createNewList(latexNormalizer(input));
		}
	}
	
	static String latexNormalizer(String text) {
		String tokens[] = text.split(" +");
		StringBuilder mtext = new StringBuilder();
		int l = tokens.length;
		for(int i = 0; i < l; i++) {
			// normalize each token
			boolean f = tokens[i].contains("\\math") || tokens[i].contains("<sup>") || tokens[i].contains("<sub>");
			String t = tokens[i].replaceAll("(\\\\)*\\\\math", "\\\\math");
			t = t.replaceAll("<sup>", "^{").replaceAll("</sup>", "}").replaceAll("<sub>", "_{").replaceAll("</sub>",
					"}");
			//System.out.println("1. " + t);
			t = t.replaceAll("</?[a-zA-Z]+>", ""); // other tags remove
			//System.out.println("2. " + t);
			if(f && !t.contains("$")) {
				// assumed it's not OK
				mtext.append('$').append(t).append('$');
			} else if(t.contains("^") && !t.contains("$")) {
				// special case
				mtext.append('$').append(t.replace("<", "{").replace(">", "}")).append('$');
			} else {
				mtext.append(t);
			}
			mtext.append(' ');
		}
		mtext.deleteCharAt(mtext.length() - 1); // last space remove
		
		return mtext.toString();
	}
	
	public static void main_2(String[] args) throws Exception {
		
		// flat SIP location or compressed SIP location
		String input = "/home/dspace/debasis/NDL/NDL_sources/JSTAGE/in/2019.Aug.08.17.58.18.js.v5.1.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/JSTAGE/logs"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/JSTAGE/out";
		String name = "js.v5.2";
		
		//String lrtMapFile = "/home/dspace/debasis/NDL/NDL_sources/JSTAGE/conf/LRT 2.csv";
		
		JSLatexCuration p = new JSLatexCuration(input, logLocation, outputLocation, name);
		//p.lrtmap = NDLDataUtils.loadKeyValue(lrtMapFile);
		LatexNormalizer ln = p.new LatexNormalizer();
		p.turnOffLoadHierarchyFlag();
		p.turnOffControlFieldsValidationFlag();
		p.addNormalizer("dc.title", ln);
		p.addNormalizer("dc.description", ln);
		p.addNormalizer("dc.coverage.spatial", ln);
		p.addNormalizer("dc.subject", ln);
		
		p.correctData();
		
		System.out.println("Done.");
	}
	
	public static void main(String[] args) {
		System.out.println(latexNormalizer("^<13>C-NMR spectrum"));
	}
}