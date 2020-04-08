package org.iitkgp.ndl.data.correction;

import org.iitkgp.ndl.data.NDLDataItem;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.DataContainerNULLConfiguration;
import org.iitkgp.ndl.data.iterator.SIPDataIterator;

/**
 * <pre>Responsible for NDL data SIP to SIP correction/curation process, logging, assets add etc.</pre>
 * <pre>This class takes care of normalization if any new value added.</pre>
 * <ul>
 * <li>To add custom logic see {@link #correctTargetItem(NDLDataItem)}</li>
 * <li>To add initialization logic see {@link #preProcessData()} {@link #init(DataContainerNULLConfiguration)}</li>
 * <li>To add destroy/complete logic see {@link #postProcessData()} {@link #close()}</li>
 * <li>To add text logging file use
 * {@link #addTextLogger(String)}
 * {@link #addTextLogger(String, String)}
 * </li>
 * <li>To add CSV logging file use
 * {@link #addCSVLogger(String, org.iitkgp.ndl.data.CSVConfiguration)}
 * {@link #addCSVLogger(String)}
 * {@link #addCSVLogger(String, String[], org.iitkgp.ndl.data.CSVConfiguration)}
 * {@link #addCSVLogger(String, String[])}
 * </li>
 * <li>
 * To add mapping resource use
 * {@link #addMappingResource(java.io.File, String)}
 * {@link #addMappingResource(java.io.File, String, String)}
 * </li>
 * </ul>
 * <pre><b>When override a method then don't forget to call super method.</b></pre>
 * <pre>To correct data try to use {@link #add(String, String)} {@link #add(String, String, char)}
 * {@link #deleteIfContains(String, java.util.Set)} {@link #deleteIfNotContains(String, java.util.Set)}
 * {@link #normalize(String...)} {@link #normalize(String, Character)}</pre>
 * <pre>To use more correction API(s) see {@link NDLDataItem} methods</pre>
 * <pre><b>Example:</b> {@code
 * // sample SIP to SIP correction
 * public class NDLSIPCorrectionContainerTest extends NDLSIPCorrectionContainer {
 * 	// constructor
 * 	public NDLSIPCorrectionContainerTest(String input, String logLocation, String outputLocation, String name) {
 * 		super(input, logLocation, outputLocation, name);
 * 	}
 * 	// correction logic
 * 	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
 * 		// some sample corrections
 * 		String id = target.getId();
 * 		if(containsMappingKey("delete." + id)) {
 * 			// to be deleted
 * 			return false;
 * 		}
 * 		add("dc.xxx.yyy", "some value"); // add some value to field dc.xxx.yyy
 * 		move("dc.xxx.yy1", "dc.xxx.yy2"); // move dc.xxx.yy1 to dc.xxx.yy2
 * 		deleteIfContains("dc.xxx.yy3", "wrong_value1", "wrong_value2"); // delete values with some filters
 * 		delete("dc.xxx.yy4"); // dc.xxx.yy4 field delete
 * 		// etc.
 * 		// success correction
 * 		return true;
 * 	}
 * 	// testing
 * 	public static void main(String[] args) throws Exception {
 * 		String input = "input source"; // flat SIP location or compressed SIP location
 * 		String logLocation = "log location"; // log location if any
 * 		String outputLocation = "output location where to write the data";
 * 		String name = "logical source name";
 * 		NDLSIPCorrectionContainerTest p = new NDLSIPCorrectionContainerTest(input, logLocation, outputLocation, name);
 * 
 * 		String deleteFile = "delete file"; // which has single column contains handles to delete
 * 		p.addMappingResource(deleteFile, "delete"); // this logical name used to access the handle
 *		p.correctData(); // corrects data
 * 	}
 * }
 * }</pre>
 * @author Debasis
 */
public abstract class NDLSIPCorrectionContainer
		extends AbstractNDLDataCorrectionContainer<SIPDataItem, SIPDataIterator> {
	
	long itemCounter = 0; // item counter to generate SIP folder
	boolean preserveFolderStructure = true;
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param outputLocation output location where corrected data to be stored
	 * @param name logical data source name, by which logging file(s) etc. prefixed
	 */
	public NDLSIPCorrectionContainer(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, name, outputLocation, false);
		dataReader = new SIPDataIterator(input);
	}
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param outputLocation output location where corrected data to be stored
	 * @param name logical data source name, by which logging file(s) etc. prefixed
	 * @param validationFlag validation flag for data validation
	 */
	public NDLSIPCorrectionContainer(String input, String logLocation, String outputLocation, String name,
			boolean validationFlag) {
		super(input, logLocation, name, outputLocation, false, validationFlag);
		dataReader = new SIPDataIterator(input);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void preProcessItem(SIPDataItem item) throws Exception {
		super.preProcessItem(item); // let super call do it's job
		if(escapeHTMLFlag) {
			item.turnOnEscapeHTMLFlag();
		}
	}
	
	/**
	 * Turn off 'preserveFolderStructure' flag which means child items go to 'Child' folder
	 * and parent items go to 'Parent' folder.
	 */
	public void dontPreserveFolderStructure() {
		preserveFolderStructure = false;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getFolderName(SIPDataItem item) {
		if(preserveFolderStructure){
			// use original folder structure
			return null;
		}
		String folder = getFileName(true) + "/" + (item.isParentItem() ? "Parent" : "Child") + "/" + ++itemCounter;
		return folder;
	}
}