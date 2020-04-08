package org.iitkgp.ndl.data.hierarchy;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.iitkgp.ndl.data.Filter;
import org.iitkgp.ndl.data.NDLDataItem;
import org.iitkgp.ndl.data.RowData;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * This class encapsulates stitching logic
 * @param <T> Source type AIP/SIP etc.
 * @author Debasis
 */
public class NDLStitchingManager<T extends NDLDataItem> {
	
	// stitching data
	List<NDLStitchingDetail> data = new LinkedList<NDLStitchingDetail>();
	Comparator<NDLStitchingDetail> groupingLogic = null;
	Map<Integer, Map<String, String>> idMapping = new HashMap<Integer, Map<String, String>>(2);
	// either grouping detail
	Map<Integer, NDLStitchingGroupingDetail> stitchingGroupingDetail
		= new HashMap<Integer, NDLStitchingGroupingDetail>(2);
	// or parent detail when parent exists
	Map<Integer, String> parentHandles = new HashMap<Integer, String>(2);
	int maxLevel;
	String handlePrefix;
	Filter<NDLStitchingDetail> filter = null;
	boolean preserveStructureFlag = true;
	RowData globalData = new RowData(); // global data
	Map<Integer, RowData> levelData = new HashMap<Integer, RowData>(2); // level specific data
	
	/**
	 * Constructor
	 * @param maxLevel sticthing maximum level
	 * @param handlePrefix handle prefix
	 */
	public NDLStitchingManager(int maxLevel, String handlePrefix) {
		this.maxLevel = maxLevel;
		this.handlePrefix = handlePrefix;
	}
	
	/**
	 * Constructor
	 * @param maxLevel sticthing maximum level
	 * @param handlePrefix handle prefix
	 * @param preserveStructureFlag this flag indicates whether parent items kept inside collection
	 * or 'parent' folder. This flag does not make sense for CSV to SIP generation.
	 */
	public NDLStitchingManager(int maxLevel, String handlePrefix, boolean preserveStructureFlag) {
		this(maxLevel, handlePrefix);
		this.preserveStructureFlag = preserveStructureFlag;
	}
	
	/**
	 * Sets grouping logic by defining grouping logic it can be sorted accordingly
	 * @param groupingLogic comparator logic for grouping (sorting)
	 */
	public void setGroupingLogic(Comparator<NDLStitchingDetail> groupingLogic) {
		this.groupingLogic = groupingLogic;
	}
	
	/**
	 * Adds filter for stitching data
	 * @param filter filter data for stitching
	 */
	public void addFilter(Filter<NDLStitchingDetail> filter) {
		this.filter = filter;
	}
	
	/**
	 * Adds data for stitching
	 * @param item item to add for stitching
	 */
	public void addData(NDLStitchingDetail item) {
		boolean f = true;
		if(filter != null) {
			f = filter.filter(item);
		}
		if(f) {
			data.add(item);
		}
	}
	
	/**
	 * Adds either stitching grouping detail or {@link #addParentDetail(int, String)}
	 * @param level this stitching level starts from 1, 2 .. so on
	 * @param detail stitching grouping detail
	 */
	public void addStitchingGroupingDetail(int level, NDLStitchingGroupingDetail detail) {
		stitchingGroupingDetail.put(level, detail);
	}
	
	/**
	 * Adds either parent detail (in case no virtual node needs not to be created, it already exists)
	 * or {@link #addStitchingGroupingDetail(int, NDLStitchingGroupingDetail)}
	 * @param level this stitching level starts from 1, 2 .. so on
	 * @param parent parent handle id
	 */
	public void addParentDetail(int level, String parent) {
		parentHandles.put(level, NDLDataUtils.getHandleSuffixID(parent));
	}
	
	/**
	 * Adds level specific data
	 * @param level this stitching level starts from 1, 2 .. so on
	 * @param data level specific data
	 */
	public void addLevelData(int level, RowData data) {
		levelData.put(level, data);
	}
}