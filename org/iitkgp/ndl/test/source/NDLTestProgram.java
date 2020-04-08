package org.iitkgp.ndl.test.source;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.iitkgp.ndl.util.NDLDataUtils;

import com.opencsv.CSVReader;

// program testing
public class NDLTestProgram {

	public static void main1(String[] args) throws IOException {
		String file = "/home/dspace/debasis/NDL/test/names.csv";
		CSVReader reader = null;
		try {
			reader = NDLDataUtils.readCSV(new File(file), 1);
			String tokens[] = null;
			Map<String, String[]> sorted = new TreeMap<String, String[]>();
			while((tokens = reader.readNext()) != null) {
				sorted.put(tokens[0], Arrays.copyOfRange(tokens, 1, tokens.length));
			}
			//List<String[]> data = reader.readAll();
			/*Set<String[]> sorted = new TreeSet<String[]>(new Comparator<String[]>() {
				@Override
				public int compare(String[] first, String[] second) {
					return first[0].compareTo(second[0]);
				}
			});
			sorted.addAll(data);*/
			/*Collections.sort(data, new Comparator<String[]>() {
				@Override
				public int compare(String[] first, String[] second) {
					return first[0].compareTo(second[0]);
				}
			});*/
			
			/*for(String[] d : data) {
				System.out.println(d[0] + '\t' + d[1]);
			}*/
			
			/*for(String[] d : sorted) {
				System.out.println(d[0] + '\t' + d[1]);
			}*/
			for(String key : sorted.keySet()) {
				System.out.println(key + " => " + NDLDataUtils.join(',', sorted.get(key)));
			}
		} finally {
			IOUtils.closeQuietly(reader);
		}
	}
	
	public static void main(String[] args) {
		/*Object []a=new String[3];
		a[0]=new Integer(9);
		a[1]=new String("America");*/
	}
}