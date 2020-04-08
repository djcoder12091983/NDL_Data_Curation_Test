package org.iitkgp.ndl.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.core.NDLXMLDataNode;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * NDL XML data item related utilities, for more details see API list.
 * @author Debasis, Vishal, Aurghya
 */
public class NDLXMLUtils {
	
	static DocumentBuilderFactory DOC_FACTORY = null;
	/**
	 * document builder for XML documents
	 */
	public static DocumentBuilder DOC_BUILDER = null;
	/**
	 * X-Path manager for XML document manipulation
	 */
	public static XPath XPATH_MANAGER = null;
	
	/**
	 * XML transformer factory
	 */
	public static TransformerFactory XML_TRANSFORMER_FACTORY = null;
	/**
	 * XML transformer
	 */
	public static Transformer XML_TRANSFORMER = null;
	
	static {
		// static block initialization
		try {
			DOC_FACTORY = DocumentBuilderFactory.newInstance();
			DOC_BUILDER = DOC_FACTORY.newDocumentBuilder();
			XPATH_MANAGER = XPathFactory.newInstance().newXPath();
			
			XML_TRANSFORMER_FACTORY = TransformerFactory.newInstance();
			XML_TRANSFORMER = XML_TRANSFORMER_FACTORY.newTransformer();
		} catch (Exception ex) {
			throw new IllegalStateException(ex.getMessage(), ex);
		}
	}
	
	/**
	 * Parses XML document
	 * @param contents XML contents
	 * @return returns parsed XML document
	 * @throws IOException throws exception when I/O related error occurs
	 * @throws SAXException throws exception when invalid XML document is provided
	 */
	public static Document parseDocument(byte[] contents) throws IOException, SAXException {
		ByteArrayInputStream in = new ByteArrayInputStream(contents);
		Document document = DOC_BUILDER.parse(in);
		in.close();
		return document;
	}

	/**
	 * gets XML node list by a given X-path and node to be processed
	 * @param node node to processed
	 * @param xPath given x-path
	 * @return returns node list
	 * @throws XPathExpressionException throws exception when invalid x-path is given
	 */
	public static NodeList getNodeList(Node node, String xPath) throws XPathExpressionException {
		return ((NodeList) XPATH_MANAGER.compile(xPath).evaluate(node, XPathConstants.NODESET));
	}
	
	/**
	 * Gets NDL compatible nodes from XML nodes
	 * @param nodes xml nodes
	 * @return returns NDL compatible nodes
	 */
	public static List<NDLDataNode> getNDLNodeList(NodeList nodes) {
		int l = nodes.getLength();
		List<NDLDataNode> ndlnodes = new ArrayList<NDLDataNode>(l);
		for(int i = 0; i < l; i++) {
			Node node = nodes.item(i);
			ndlnodes.add(new NDLXMLDataNode(node));
		}
		return ndlnodes;
	}
	
	/**
	 * gets single node by a given X-path and node to be processed
	 * @param node node to processed
	 * @param xPath given x-path
	 * @return returns target node, NULL if multiple nodes or no node found
	 * @throws XPathExpressionException throws exception when invalid x-path is given
	 */
	public static Node getSingleNode(Node node, String xPath) throws XPathExpressionException {
		NodeList nodes = ((NodeList) XPATH_MANAGER.compile(xPath).evaluate(node, XPathConstants.NODESET));
		/*System.out.println("xpaath: " + xPath);
		System.out.println(nodes.getLength());*/
		if(nodes.getLength() == 1) {
			return nodes.item(0);
		} else {
			// invalid
			return null;
		}
	}

	/**
	 * Gets XML node attribute value
	 * @param node XML node
	 * @param attribute attribute name
	 * @return returns attribute associated value
	 */
	public static String getAttributeValue(Node node, String attribute) {
		NamedNodeMap attributes = node.getAttributes();
		Node attributeNode = attributes.getNamedItem(attribute);
		if(attributeNode != null) {
			return attributeNode.getTextContent();
		} else {
			return null;
		}
	}
}