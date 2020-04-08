package org.iitkgp.ndl.test.source;

import org.iitkgp.ndl.data.compress.SIPTarGZAbbreviator;

/**
 * IAR NAL compressed tar.gz file Abbreviator to shorten entry name
 * @see SIPTarGZAbbreviator
 * @author Debasis
 */
public class IARNALTarGZAbbreviator {

	// test
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/IAR/raw_data/nasa-techdocs/in/nasa_techdocs-v3.tar.gz";
		String out = "/home/dspace/debasis/NDL/IAR/raw_data/nasa-techdocs/in";
		String name = "Nasa.Techdocs.v3";
		
		System.out.println("Start.");
		
		SIPTarGZAbbreviator a = new SIPTarGZAbbreviator(input, out, name);
		a.abbreviate();
		
		System.out.println("Done.");
	}
}