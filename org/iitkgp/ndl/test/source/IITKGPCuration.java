package org.iitkgp.ndl.test.source;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.data.normalizer.NDLDataNormalizer;
import org.iitkgp.ndl.util.NDLDataUtils;

import com.opencsv.CSVWriter;

// iit kgp tentative curation
public class IITKGPCuration extends NDLSIPCorrectionContainer {
	
	static Set<String> WRONG_NAME_TOKENS1;
	Map<Combiner, Data> groupdata = new HashMap<>();
	
	// TODO it can also be externalized
	static Set<String> WRONG_NAME_TOKENS2 = new HashSet<>();
	static {
		WRONG_NAME_TOKENS2.add("tech");
		WRONG_NAME_TOKENS2.add("dept");
		// TODO add list
	}
	
	class Data {
		String handle;
		List<String> accessions = new LinkedList<String>();
		
		public Data(String handle) {
			this.handle = handle;
		}
		
		void addAccession(String accession) {
			accessions.add(accession);
		}
	}
	
	// group by clause
	class Combiner {
		String title;
		String names;
		
		public Combiner(String title, String ... names) {
			this.title = title;
			this.names = NDLDataUtils.join('|', names);
		}
		
		public Combiner(String title, Collection<String> names) {
			this.title = title;
			this.names = NDLDataUtils.join(names, '|');
		}
		
		@Override
		public int hashCode() {
			int h = 31;
			h = h * 17 + title.hashCode();
			h = h * 17 + names.hashCode();
			return h;
		}
		
		@Override
		public boolean equals(Object obj) {
			if(obj == null) {
				return false;
			} else if(obj == this) {
				return true;
			} else {
				if(obj instanceof Combiner) {
					Combiner o = (Combiner)obj;
					return o.title.equals(this.title) && o.names.equals(this.names);
				} else {
					return false;
				}
			}
		}
	}

	public IITKGPCuration(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}
	
	Collection<String> nn(String input) {
		input = input.replaceAll("((\\(|\\[|<).+(\\)|\\]|>))", "");
		String andsplit[] = input.split("(?i)( +and +)|&");
		List<String> newnames = new LinkedList<>();
		for(String name : andsplit) {
			// split by and
			try {
				if(name.length() > 100) {
					// long names
					log("invalid.names", name);
					continue; // try next
				}
				String tokens[] = name.split("(\\.| )+");
				/*for(String t : tokens) {
					System.out.println(t);
				}*/
				boolean f = true;
				int l = tokens.length;
				if(l == 1 && name.length() < 5) {
					//System.out.println("11");
					// single short token delete
					f = false;
				} else if(l > 5) {
					//System.out.println("22");
					// long name
					f = false;
				} else {
					boolean longt = false;
					for(String t : tokens) {
						t = t.trim();
						// check all single token
						if(t.length() > 1) {
							longt = true;
						}
						if(WRONG_NAME_TOKENS1.contains(t)) {
							// invalid name
							f = false;
							break;
						}
					}
					//System.out.println(longt);
					if(!longt) {
						f = false;
					}
				}
				
				if(!f) {
					// invalid name
					log("invalid.names", name);
				} else {
					// normalize
					newnames.add(NDLDataUtils.normalizeSimpleNameByWrongNameTokens(name, WRONG_NAME_TOKENS2, true));
				}
			} catch(Exception ex) {
				// error
				throw new IllegalStateException(ex.getMessage(), ex);
			}
		}
		return newnames;
	}
	
	// simple name normalization
	class NameNormalizer extends NDLDataNormalizer{
		 
		@Override
		public Collection<String> transform(String input) {
			return nn(input);
		}
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		// simple name normalize
		normalize("dc.contributor.author", "dc.contributor.editor");
		
		// group by title and names
		// item merging
		String title = target.getSingleValue("dc.title");
		Collection<String> names = target.getValue("dc.contributor.author");
		names.addAll(target.getValue("dc.contributor.editor"));
		Combiner group = new Combiner(title, names);
		if(groupdata.containsKey(group)) {
			// merging case
			// preserve accession and delete item
			Data d = groupdata.get(group);
			d.addAccession(target.getSingleValue("dc.identifier.other:accessionNo"));
			log("delete.items", target.getId() + " duplicates@" + d.handle);
			return false;
		} else {
			Data d = new Data(target.getId());
			d.addAccession(target.getSingleValue("dc.identifier.other:accessionNo"));
			groupdata.put(group, d);
		}
		
		return true;
	}
	
	// correct
	public static void main(String[] args) throws Exception {
		
		NDLDataUtils.addNameNormalizationConfiguration("max.name.tokens.length", "5");
		
		String input = "/home/dspace/debasis/NDL/NDL_sources/iit-kgp/in/2019.Oct.22.19.45.26.Cli_Final_Author.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/iit-kgp/out";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/iit-kgp/out";
		String name = "iitkgp.tentative.final.v5";
		
		String wrongNameTokesFile = "/home/dspace/debasis/NDL/NDL_sources/iit-kgp/in/wrong-name-tokens";
		WRONG_NAME_TOKENS1 = NDLDataUtils.loadSet(wrongNameTokesFile, true);
		// TODO WRONG_NAME_TOKENS2 if needed
		
		IITKGPCuration p = new IITKGPCuration(input, logLocation, outputLocation, name);
		NameNormalizer nn = p.new NameNormalizer();
		p.addTextLogger("invalid.names");
		p.addTextLogger("delete.items");
		p.addNormalizer("dc.contributor.author", nn);
		p.addNormalizer("dc.contributor.editor", nn);
		p.correctData();
		
		// merged data written into CSV
		System.out.println("Merging accessions....");
		
		File csvfile = new File(outputLocation, "merged.accession.items.csv");
		CSVWriter mergedcsv = NDLDataUtils.openCSV(csvfile);
		mergedcsv.writeNext(new String[]{"ID", "Accessions"});
		int mac = 0;
		for(Combiner c : p.groupdata.keySet()) {
			Data d = p.groupdata.get(c);
			if(d.accessions.size() > 1) {
				// merging case
				mergedcsv.writeNext(new String[]{d.handle, NDLDataUtils.join(d.accessions, '|')});
				mac++;
			}
		}
		
		System.out.println("Merged: " + mac);
		
		mergedcsv.close();
		
		System.out.println("Done.");
	}
	
	/*public static void main1(String[] args) throws Exception {
		NDLDataUtils.addNameNormalizationConfiguration("max.name.tokens.length", "5");
		
		String wrongNameTokesFile = "/home/dspace/debasis/NDL/NDL_sources/iit-kgp/in/wrong-name-tokens";
		WRONG_NAME_TOKENS1 = NDLDataUtils.loadSet(wrongNameTokesFile, true);
		

		String input = "/home/dspace/debasis/NDL/NDL_sources/iit-kgp/in/2019.Oct.22.19.45.26.Cli_Final_Author.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/iit-kgp/out";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/iit-kgp/out";
		String name = "iitkgp.tentative.final.v5";
		
		IITKGPCuration p = new IITKGPCuration(input, logLocation, outputLocation, name);
		p.addTextLogger("invalid.names");
				
		System.out.println(p.nn("COLE, G. H. A."));
		//System.out.println(NDLDataUtils.normalizeSimpleName("PATANKAR S V", true));
	}*/
}