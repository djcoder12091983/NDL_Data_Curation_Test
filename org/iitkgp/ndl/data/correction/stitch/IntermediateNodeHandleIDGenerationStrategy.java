package org.iitkgp.ndl.data.correction.stitch;

import java.util.HashSet;
import java.util.Set;

import org.iitkgp.ndl.data.correction.stitch.context.NDLStitchingContext;

/**
 * This is required for intermediate node(virtual node) handle ID generation strategy for SIP stitching
 * @author Debasis
 */
public abstract class IntermediateNodeHandleIDGenerationStrategy {
	
	protected Set<Integer> levels = new HashSet<>(4);
	
	/**
	 * Adds associated level for handle ID generation
	 * @param levels associated levels
	 */
	public IntermediateNodeHandleIDGenerationStrategy(int ... levels) {
		for(int level : levels) {
			this.levels.add(level);
		}
	}
	
	/**
	 * Checks whether level is registered for handle ID generation strategy
	 * @param level level to consider for handle ID generation
	 * @return returns true if exists otherwise false
	 */
	public boolean levelExists(int level) {
		return levels.contains(level);
	}
	
	/**
	 * Generates handle ID fragment for a given order/level and node ID
	 * @param level given order/level
	 * @param id given node ID
	 * @param context SIP stitching context
	 * @return returns handle ID fragment
	 */
	public abstract String generate(int level, String id, NDLStitchingContext context);
}