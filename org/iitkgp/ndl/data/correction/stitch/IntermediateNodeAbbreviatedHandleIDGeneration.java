package org.iitkgp.ndl.data.correction.stitch;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.correction.stitch.context.NDLStitchingContext;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * This is required for intermediate node(virtual node) handle ID generation strategy for stitching.
 * It generates handle ID as abbreviated form, using an ID text
 * @author Debasis
 */
public class IntermediateNodeAbbreviatedHandleIDGeneration extends IntermediateNodeHandleIDGenerationStrategy {
	
	Map<Integer, Map<String, String>> idGenerationDetail = new HashMap<>(4);
	
	/**
	 * Adds associated level for handle ID generation
	 * @param levels associated levels
	 */
	public IntermediateNodeAbbreviatedHandleIDGeneration(int ... levels) {
		super(levels);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String generate(int level, String id, NDLStitchingContext context) {
		
		if(!levels.contains(level)) {
			// cross validate
			throw new IllegalStateException("Level: " + level + " is not registered for handle ID generation.");
		}
		
		id = NDLDataUtils.removeMultipleSpaces(id); // normalize id
		Map<String, String> detailmap = idGenerationDetail.get(level);
		if(detailmap == null) {
			// first time
			detailmap = new HashMap<>(4);
			idGenerationDetail.put(level, detailmap);
		}
		String assignedID = detailmap.get(id);
		if(StringUtils.isBlank(assignedID)) {
			// not assigned yet
			assignedID = NDLDataUtils.splitByInitialLetter(id) + "_" + context.nextAutoGenerateHandleID();
			detailmap.put(id, assignedID);
		}
		
		return assignedID;
	}
}