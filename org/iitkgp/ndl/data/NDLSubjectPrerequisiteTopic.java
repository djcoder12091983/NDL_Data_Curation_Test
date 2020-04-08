package org.iitkgp.ndl.data;

import java.util.LinkedList;
import java.util.List;

import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * Subject prerequisitetopic
 * @author Debasis
 */
public class NDLSubjectPrerequisiteTopic {
	
	List<HandleTitle> items = new LinkedList<HandleTitle>();
	List<String> topics = new LinkedList<String>();
	
	/**
	 * Adds item
	 * @param item item to add
	 */
	public void addItem(HandleTitle item) {
		items.add(item);
	}
	
	/**
	 * Adds item
	 * @param handle handle of the item
	 * @param title title of the item
	 */
	public void addItem(String handle, String title) {
		items.add(new HandleTitle(handle, title));
	}

	/**
	 * Adds items
	 * @param items items to add
	 */
	public void addItems(List<HandleTitle> items) {
		this.items.addAll(items);
	}
	
	/**
	 * Adds item
	 * @param topic item to add
	 */
	public void addTopic(String topic) {
		topics.add(topic);
	}

	/**
	 * Adds items
	 * @param topics items to add
	 */
	public void addTopics(List<String> topics) {
		this.topics.addAll(topics);
	}
	
	/**
	 * Gets JSON string
	 * @return returns JSONfied string
	 */
	public String jsonify() {
		return NDLDataUtils.getJson(this);
	}
}

// for internal usage
// handle and title
class HandleTitle {
	
	String handle;
	String title;
	
	HandleTitle(String handle, String title) {
		this.handle = handle;
		this.title = title;
	}
}