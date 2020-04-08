package org.iitkgp.ndl.data;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class XMLLevelXPathDetail {
	
	int level;
	String selectXPath;
	List<KeyXPath> dataXPaths = new LinkedList<KeyXPath>();
	Set<String> excludeNodes = new HashSet<String>();
	boolean data2bFetched = true;
	
	public XMLLevelXPathDetail(int level, String selectXPath) {
		this.level = level;
		this.selectXPath = selectXPath;
	}
	
	public XMLLevelXPathDetail(int level, String selectXPath, boolean data2bFetched) {
		this.level = level;
		this.selectXPath = selectXPath;
		this.data2bFetched = data2bFetched;
	}
	
	public boolean isData2bFetched() {
		return data2bFetched;
	}
	
	public void addDataXPath(String key, String xPath) {
		dataXPaths.add(new KeyXPath(key, xPath));
	}
	
	public void addExcludeNode(String xPath) {
		excludeNodes.add(xPath);
	}
	
	public List<KeyXPath> getDataXPaths() {
		return dataXPaths;
	}
	
	public Set<String> getExcludeNodes() {
		return excludeNodes;
	}
	
	public String getSelectXPath() {
		return selectXPath;
	}

}