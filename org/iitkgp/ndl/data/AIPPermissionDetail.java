package org.iitkgp.ndl.data;

import java.util.LinkedList;
import java.util.List;

/**
 * AIP permission detail
 * @author Debasis
 */
public class AIPPermissionDetail {
	
	String groupName;
	String type;
	// members
	List<String> members = new LinkedList<String>();
	
	/**
	 * AIP permission constructor
	 * @param groupName group name
	 * @param type group type
	 */
	public AIPPermissionDetail(String groupName, String type) {
		this.groupName = groupName;
		this.type = type;
	}
	
	/**
	 * Gets group name
	 * @return returns group name
	 */
	public String getGroupName() {
		return groupName;
	}
	
	/**
	 * Gets group type
	 * @return returns group type
	 */
	public String getType() {
		return type;
	}
	
	/**
	 * Adds member by name
	 * @param member member name
	 */
	public void addMember(String member) {
		members.add(member);
	}

	/**
	 * Gets member list
	 * @return returns member list 
	 */
	public List<String> getMembers() {
		return members;
	}
}