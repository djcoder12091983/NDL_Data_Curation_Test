package org.iitkgp.ndl.converter;

import java.io.File;
import java.util.List;
import java.util.Stack;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.RowData;
import org.iitkgp.ndl.data.RowDataList;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

class XMLNode {
    
    Node node;
    RowData data;
    int level;
    
    public XMLNode(Node node, int level){
        this.node = node;
        this.level = level;
        data = new RowData();
    }
    
    public XMLNode(Node node,int level, RowData data){
        this.node = node;
        this.level = level;
        this.data = data;
    }
    
    public void addData(RowData data){
        this.data.addAllData(data);
    }
}

public abstract class XML2CSVConverter {
	
	String sourcePath = null;
    String targetPath = null;
    int threshold = 5000;
    String filePrefix = null;
    
    protected XPath xPathManager = XPathFactory.newInstance().newXPath();
    
    public XML2CSVConverter(String sourcePath, String targetPath) {
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
    }

    public XML2CSVConverter(String sourcePath, String targetPath, int threshold) {
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.threshold = threshold;
    }
    
    public void setFilePrefix(String filePrefix) {
		this.filePrefix = filePrefix;
	}
    
    public abstract List<Node> getNextNodes(Node node, int level);
    
    public abstract RowData getData(Node node,int level);
    
    void process() throws Exception {
    	
    	DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        
        File files[] = new File(sourcePath).listFiles();
        RowDataList itemsData = new RowDataList(8); // default column size
        int fileIndex = 1;
        for(File file : files) {
        	
        	System.out.println("Processing: " + file.getName());
        	
            // iterate files one by one
            Document document = db.parse(file);
            
            Stack<XMLNode> nodes = new Stack<XMLNode>();
            nodes.push(new XMLNode(document, 1)); // level starts with 1
            while(!nodes.isEmpty()) {
                XMLNode node = nodes.pop();
                Node currentNode = node.node;
                int level = node.level;
                // get next nodes
                List<Node> nextNodes = getNextNodes(currentNode, level);
                // add next nodes
                if(nextNodes.isEmpty()) {
                    // leaf node/terminating node
                	
                	RowData nodedata = getData(currentNode, level);
                	nodedata.addAllData(node.data); // add previous collected data
                	itemsData.addRowData(nodedata);
                	if(itemsData.size() >= threshold) {
                        // flush the data
						String fileName = (filePrefix != null ? filePrefix : StringUtils.EMPTY) + "data.file."
								+ fileIndex + ".csv";
                    	System.out.println("Threshold reached: " + fileName + " saving the data.");
                    	itemsData.flush2CSV(new File(targetPath, fileName), '|');
                    	fileIndex++;
                    }
                } else {
                	// get data from node
                    RowData nodedata = getData(currentNode, level);
                    // next brances exist
                    for(Node nextNode : nextNodes) {
                        XMLNode nextXMLNode = new XMLNode(nextNode, level + 1);
                        if (nodedata != null) {
                            nextXMLNode.addData(nodedata);
                        }
                        nodes.add(nextXMLNode);
                    }
                }
            }
        }
        
        // flush remaining data
        if(!itemsData.isEmpty()) {
            // flush the data
			String fileName = (filePrefix != null ? filePrefix : StringUtils.EMPTY) + "data.file."
					+ fileIndex + ".csv";
        	System.out.println("Remaining: " + fileName + " saving the data.");
        	itemsData.flush2CSV(new File(targetPath, fileName), '|');
        }
    }
}