package org.iitkgp.ndl.data.correction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.AIPDataItem;
import org.iitkgp.ndl.data.container.NDLAIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * NDL AIP pre-stitching program which needs to run before actual stitching.
 * It generates hierarchy tree which will be used in AIP stitching.
 * Virtual nodes will be created before hand. Order and parent information should exist in every item.
 * <pre>Default root node identification strategy is: either parent-information is 0 or parent-information is missing
 * of developer should override the strategy</pre>
 * <pre>Default assumption is parent-information is kept in 'dc.relation.ispartof' field,
 * developer can override this information.</pre>
 * @see NDLAIPStitchingCorrection
 * @author Debasis
 */
public class NDLAIPPreStitching extends NDLAIPDataContainer {
	
	// hierarchy file name
	static final String HIERARCRCHY_FILE_NAME = "hierarachy.tree";
	
	String source;
	String logLocation;

	List<String[]> data = new ArrayList<String[]>();
	Map<String, String> titles = new HashMap<String, String>();
	
	String parentInformationField = "dc.relation.ispartof";
	String orderField;
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param name logical data source name, by which logging file(s) etc. prefixed
	 * @param orderField the order information field
	 */
	public NDLAIPPreStitching(String input, String logLocation, String name, String orderField) {
		super(input, logLocation, name);
		this.orderField = orderField;
	}
	
	/**
	 * Sets the parent information (parent handle id) field;
	 * @param parentInformationField parent information field name
	 */
	public void setParentInformationField(String parentInformationField) {
		this.parentInformationField = parentInformationField;
	}
	
	/**
	 * Returns true if item is root
	 * @param item item to check
	 * @return Returns true if item is root, otherwise false
	 */
	protected boolean isRoot(AIPDataItem item) {
		String ispart = NDLDataUtils.getHandleSuffixID(item.getSingleValue(parentInformationField));
		return StringUtils.equals(ispart, "0") || StringUtils.isBlank(ispart);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean readItem(AIPDataItem item) throws Exception {
		
		if(item.isItem()) {
			// valid item
			String ispart = isRoot(item) ? null
					: NDLDataUtils.getHandleSuffixID(item.getSingleValue(parentInformationField));
			String order = item.getSingleValue(orderField); // order field
			String title = item.getSingleValue("dc.title"); // title
			String id = NDLDataUtils.getHandleSuffixID(item.getId());
			
			String row[] = new String[]{ispart, id, title, order};
			data.add(row);
			titles.put(id, title);
		}
		
		return true;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void postProcessData() throws Exception {
		super.postProcessData();
		
		// csv logger
		addCSVLogger(HIERARCRCHY_FILE_NAME, new String[] {"Parent_ID", "Parent_Title", "Item_ID", "Title", "Order"});
		// write hierarchy data
		for(String[] row : data) {
			log(HIERARCRCHY_FILE_NAME, new String[]{row[0], titles.get(row[0]), row[1], row[2], row[3]});
		}
	}
}