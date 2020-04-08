package org.iitkgp.ndl.data;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.iitkgp.ndl.data.exception.NDLMultivaluedException;
import org.w3c.dom.Document;

/**
 * Encapsulates AIP other details (assets etc.) along with XML document
 * @see AIPDataItem
 * @author Debasis, Vishal
 */
public class AIPItemContents {
	
	Document document;
	Map<String, byte[]> others = new HashMap<String, byte[]>(2);
	Map<String, List<AIPFileGroup>> fileGroups = new HashMap<String, List<AIPFileGroup>>(2);
	long size;
	
	/**
	 * COnstructor
	 * @param document XML document
	 */
	public AIPItemContents(Document document) {
		this.document = document;
	}
	
	/**
	 * Constructor
	 * @param document XML document
	 * @param others other contents (assets etc.)
	 */
	public AIPItemContents(Document document, Map<String, byte[]> others) {
		this.document = document;
		this.others.putAll(others);
	}
	
	/**
	 * Adds entry
	 * @param entry entry name
	 * @param contents byte contents
	 */
	void add(String entry, byte[] contents) {
		others.put(entry, contents);
	}
	
	/**
	 * Gets XML document
	 * @return returns associated XML document
	 */
	public Document getDocument() {
		return document;
	}
	
	/**
	 * Gets other details
	 * @return returns other details
	 */
	Map<String, byte[]> getOthers() {
		return others;
	}
	
	/**
	 * Gets AIP file group detail by name
	 * @param name given file name
	 * @return returns AIP file group details if exists otherwise NULL
	 */
	public List<AIPFileGroup> getFileGroupDetails(String name) {
		return fileGroups.get(name);
	}
	
	/**
	 * Gets AIP file group detail by name
	 * @param name given file name
	 * @return returns AIP file group detail if exists otherwise NULL
	 * @throws NDLMultivaluedException throws error if multiple value found
	 * @see #getFileGroupDetails(String)
	 */
	public AIPFileGroup getFileGroupDetail(String name) {
		List<AIPFileGroup> details = fileGroups.get(name);
		if(details != null) {
			if(details.size() > 1) {
				throw new NDLMultivaluedException(
						"AIP file groups multiple found for name: " + name + ". Use 'getFileGroupDetails'.");
			} else {
				return details.get(0);
			}
		} else {
			// not found
			return null;
		}
	}
	
	/**
	 * By name (file type name) add file group detail
	 * @param name given name (file type name)
	 * @param detail associated file group detail
	 */
	public void addFileGroup(String name, AIPFileGroup detail) {
		List<AIPFileGroup> details = fileGroups.get(name);
		if(details == null) {
			details = new LinkedList<AIPFileGroup>();
			fileGroups.put(name, details);
		}
		details.add(detail);
	}
	
	/**
	 * Adds file groups
	 * @param fileGroups file groups
	 */
	public void addFileGroups(Map<String, List<AIPFileGroup>> fileGroups) {
		if(fileGroups != null) {
			this.fileGroups.putAll(fileGroups);
		}
	}
	
	/**
	 * Sets tentative size in bytes
	 * @param size tentative size in bytes
	 */
	public void size(long size) {
		this.size = size;
	}
	
	/**
	 * Gets tentative size in bytes
	 * @return returns tentative size in bytes
	 */
	public long size() {
		return size;
	}
}