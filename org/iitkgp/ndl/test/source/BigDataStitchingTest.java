package org.iitkgp.ndl.test.source;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;

// big data stitching test to avoid out-of-memory error
public class BigDataStitchingTest extends NDLSIPCorrectionContainer {
	
	static String GLOBAL_TITLE;
	//static long ITEMS = 25 * 100000;
	static Random  random = new Random();
	
	int level = 0;
	long total = 0;
	
	BigInteger handle = new BigInteger("100000000000000000000");
	Queue<StitchingNode> nodes = new LinkedList<>();
	
	class StitchingNode {
		String id = null; //25 bytes
		String title = null; // 500 bytes
		
		public StitchingNode(String id, String title) {
			this.id = id;
			this.title = title;
		}
		
		Map<String, StitchingNode> children = new HashMap<>(2);
		
		StitchingNode add(String id, String title) {
			StitchingNode child = new StitchingNode(id, title);
			children.put(id, child);
			return child;
		}
	}
	
	static {
		StringBuilder t = new StringBuilder();
		for(int i = 0; i < 500; i++) {
			t.append('X');
		}
		GLOBAL_TITLE = t.toString();
	}

	public BigDataStitchingTest(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
		handle = handle.add(BigInteger.ONE);
		nodes.add(new StitchingNode(handle.toString(), GLOBAL_TITLE));
	}
	
	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		if(++level < 5) {
			Queue<StitchingNode> tnodes = new LinkedList<>();
			while(!nodes.isEmpty()) {
				tnodes.add(nodes.poll());
			}
			while(!tnodes.isEmpty()) {
				StitchingNode node = tnodes.poll();
				for(int i = 0; i < 40; i++) {
					handle = handle.add(BigInteger.ONE);
					StitchingNode child = node.add(handle.toString(), GLOBAL_TITLE);
					total++;
					if(level < 4) {
						// last child not to wrote
						nodes.add(child);
					}
				}
			}
		}
		
		return true;
	}
	
	@Override
	protected void intermediateProcessHandler() {
		System.out.println("Total count: " + total);
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/Karnatak_HC_Stitch/2019.Sep.17.10.54.52.Karnatak_Output_SIP_9.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/Karnatak_HC_Stitch/out"; // log location if any
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/Karnatak_HC_Stitch/out";
		String name = "big.data.test";
		
		BigDataStitchingTest p = new BigDataStitchingTest(input, logLocation, outputLocation, name);
		p.correctData();
		
		System.out.println("Done.");
	}
}