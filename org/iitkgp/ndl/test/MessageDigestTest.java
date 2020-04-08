package org.iitkgp.ndl.test;

import java.io.File;
import java.security.MessageDigest;

import org.apache.commons.io.FileUtils;

// message digest test
public class MessageDigestTest {
	
	// byte array 2 hex string
	static String byte2hex(byte[] bytes) {
		StringBuilder checksum = new StringBuilder();
		int l = bytes.length;
		for(int i = 0; i < l; i++) {
			checksum.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
		}
		return checksum.toString();
	}
	
	public static void main(String[] args) throws Exception {
		String file = "/home/dspace/debasis/NDL/generated_xml_data/AIP_ASSET_TEST/AIP/bitstream_59427.xlsx";
		MessageDigest md = MessageDigest.getInstance("MD5");
		md.update(FileUtils.readFileToByteArray(new File(file))); // gets hash value
		byte hash[] = md.digest();
		System.out.println(byte2hex(hash));
	}
}