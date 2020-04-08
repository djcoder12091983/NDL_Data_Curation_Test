package org.iitkgp.ndl.converter;

import java.io.FileInputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.data.KeyXPath;
import org.iitkgp.ndl.data.RowData;
import org.iitkgp.ndl.data.XMLLevelXPathDetail;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class DefaultXML2CSVConverter extends XML2CSVConverter {
	
	List<XMLLevelXPathDetail> configuration = null;
	
	public DefaultXML2CSVConverter(String sourcePath, String targetPath, List<XMLLevelXPathDetail> configuration) {
		super(sourcePath, targetPath);
		this.configuration = configuration;
	}
	
	public DefaultXML2CSVConverter(String sourcePath, String targetPath, List<XMLLevelXPathDetail> configuration,
			int threshold) throws Exception {
		super(sourcePath, targetPath, threshold);
		this.configuration = configuration;
	}
	
	public DefaultXML2CSVConverter(String sourcePath, String targetPath, String configurationFile) throws Exception {
		super(sourcePath, targetPath);
		loadConfigurationFile(configurationFile);
	}
	
	public DefaultXML2CSVConverter(String sourcePath, String targetPath, String configurationFile, int threshold)
			throws Exception {
		super(sourcePath, targetPath, threshold);
		loadConfigurationFile(configurationFile);
	}
	
	public void loadConfigurationFile(String configurationFile) throws Exception {
		// load configuration
		Gson gson = new Gson();
		configuration = gson.fromJson(IOUtils.toString(new FileInputStream(configurationFile), "UTF-8"),
				new TypeToken<List<XMLLevelXPathDetail>>() {
				}.getType());
	}
	
	@Override
	public RowData getData(Node node, int level) {
		XMLLevelXPathDetail xPathDetail = configuration.get(level-1); //assumed level information kept sequentially
		if(!xPathDetail.isData2bFetched()) {
			// no need to fetch data
			return null;
		}
		
		List<KeyXPath> dataXPaths = xPathDetail.getDataXPaths();
		
		RowData rowData = new RowData();
		if(!dataXPaths.isEmpty()) {
			for(KeyXPath dataXPath : dataXPaths) {
				List<Node> dataNodes = getNodeList(node, dataXPath.getXPath());
				if(!dataNodes.isEmpty()) {
					// data nodes found
					for(Node dataNode : dataNodes) {
						String value = dataNode.getTextContent();
						if(StringUtils.isNotBlank(value)) {
							// not-blank value
							rowData.addData(dataXPath.getKey(), value.replaceAll("\\r?\\n", " ").trim());
						}
					}
				}
			}
		} else {
			// supposed to have all data-nodes
			Set<String> excludeNodes = xPathDetail.getExcludeNodes();
			NodeList children = node.getChildNodes();
			int l = children.getLength();
			for(int i=0; i<l; i++) {
				Node child = children.item(i);
				String nodeName = child.getNodeName();
				if(!excludeNodes.contains(nodeName)) {
					// needs to consider
					String value = child.getTextContent();
					if(StringUtils.isNotBlank(value)) {
						// not-blank value
						rowData.addData(nodeName, value.replaceAll("\\r?\\n", " ").trim());
					}
				}
			}
		}
		
		return rowData;
	}
	
	@Override
	public List<Node> getNextNodes(Node node, int level) {
		XMLLevelXPathDetail xPathDetail = configuration.get(level-1); //assumed level information kept sequentially
		String selectXPath = xPathDetail.getSelectXPath();
		
		if(StringUtils.isNotBlank(selectXPath)) {
			List<Node> nextNodes = getNodeList(node, selectXPath);
			return nextNodes;
		} else {
			// empty
			return new LinkedList<Node>();
		}
	}
	
	List<Node> getNodeList(Node node, String xPath) {
		List<Node> nodes = new LinkedList<Node>();
		try {
			NodeList nodeList = ((NodeList) xPathManager.compile(xPath).evaluate(node, XPathConstants.NODESET));
			int l = nodeList.getLength();
			for(int i=0; i<l; i++) {
				nodes.add(nodeList.item(i));
			}
		} catch(Exception ex) {
			// error
			System.err.println("[Error] " + ex.getMessage());
		}
		return nodes;
	}
	
	public static void main_2(String[] args) throws Exception {
		
		List<XMLLevelXPathDetail> configuration = new LinkedList<XMLLevelXPathDetail>();
		configuration.add(new XMLLevelXPathDetail(1, "//ebook", false));
		XMLLevelXPathDetail xPathDetail = new XMLLevelXPathDetail(2, StringUtils.EMPTY);
		
		// data to be fetched
		xPathDetail.addDataXPath("ID", "Id");
		xPathDetail.addDataXPath("Title English", "Title_En");
		xPathDetail.addDataXPath("Title Hindi", "Title_Hi");
		xPathDetail.addDataXPath("Title Urdu", "Title_Ur");
		xPathDetail.addDataXPath("Subtitle English", "SubTitle_En");
		xPathDetail.addDataXPath("Subtitle Hindi", "SubTitle_Hi");
		xPathDetail.addDataXPath("Subtitle Urdu", "SubTitle_Ur");
		xPathDetail.addDataXPath("Price English", "Price_En");
		xPathDetail.addDataXPath("First Edition Year", "FirstEditionYear");
		xPathDetail.addDataXPath("Language", "Language");
		xPathDetail.addDataXPath("Book Type", "EbookFormat");
		xPathDetail.addDataXPath("URL", "Url");
		xPathDetail.addDataXPath("Thumb", "Thumb");
		
		xPathDetail.addDataXPath("Author ID", "author/Id");
		xPathDetail.addDataXPath("Author English", "author/Name_En");
		xPathDetail.addDataXPath("Author Hindi", "author/Name_Hi");
		xPathDetail.addDataXPath("Author Urdu", "author/Name_Ur");
		xPathDetail.addDataXPath("Contributor ID", "contributor/Id");
		xPathDetail.addDataXPath("Contributor English", "contributor/Name_En");
		xPathDetail.addDataXPath("Contributor Hindi", "contributor/Name_Hi");
		xPathDetail.addDataXPath("Contributor Urdu", "contributor/Name_Ur");
		xPathDetail.addDataXPath("Editor ID", "editor/Id");
		xPathDetail.addDataXPath("Editor English", "editor/Name_En");
		xPathDetail.addDataXPath("Editor Hindi", "editor/Name_Hi");
		xPathDetail.addDataXPath("Editor Urdu", "editor/Name_Ur");
		xPathDetail.addDataXPath("Poet ID", "poet/Id");
		xPathDetail.addDataXPath("Poet English", "poet/Name_En");
		xPathDetail.addDataXPath("Poet Hindi", "poet/Name_Hi");
		xPathDetail.addDataXPath("Poet Urdu", "poet/Name_Ur");
		xPathDetail.addDataXPath("Publisher ID", "publisher/Id");
		xPathDetail.addDataXPath("Publisher English", "publisher/Name_En");
		xPathDetail.addDataXPath("Publisher Hindi", "publisher/Name_Hi");
		xPathDetail.addDataXPath("Publisher Urdu", "publisher/Name_Ur");
		
		configuration.add(xPathDetail);
		
		System.out.println("Start!!");
		
		String sourcePath = "/home/dspace/debasis/NDL/rekhta/data";
		String targetPath = "/home/dspace/debasis/NDL/rekhta/csv";
		DefaultXML2CSVConverter converter = new DefaultXML2CSVConverter(sourcePath, targetPath, configuration);
		converter.setFilePrefix("IAR.audio.");
		converter.process();
		
		System.out.println("End!!");
	}
	
	public static void main(String[] args) throws Exception {
		
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
		XPath xPath = XPathFactory.newInstance().newXPath();
		
		Document document = docBuilder.parse("/home/dspace/debasis/NDL/rekhta/data/rekhta.1.xml");
		String exp = "//ebook";
		NodeList nodes = (NodeList) xPath.compile(exp).evaluate(document, XPathConstants.NODESET);
		int l = nodes.getLength();
		
		/*Document newdoc = docBuilder.newDocument();
		Element root = newdoc.createElement("ebooks");
		newdoc.appendChild(root);
		
		System.out.println("Start!!");
		int fileidx = 1;
		for(int i=0; i<l; i++) {
			Node node = nodes.item(i);
			Node newnode = node.cloneNode(true);
			newdoc.adoptNode(newnode);
			root.appendChild(newnode);
			if((i+1)%500==0 || i == l-1) {
				// 500 copy or last chunk
				// write the content into xml file
				TransformerFactory transformerFactory = TransformerFactory.newInstance();
				Transformer transformer = transformerFactory.newTransformer();
				DOMSource source = new DOMSource(newdoc);
				StreamResult result = new StreamResult(new File("/home/dspace/debasis/NDL/rekhta/data/rekhta." + fileidx++ + ".xml"));
	
				transformer.transform(source, result);
				
				// reset document
				newdoc = docBuilder.newDocument();
				root = newdoc.createElement("ebooks");
				newdoc.appendChild(root);
			}
		}
		System.out.println("End!!");*/
		
		System.out.println(l);
	}
	
	public static void main_1(String[] args) throws Exception {
		
		// generates configuration JSON
		//Gson gson = new Gson();
		List<XMLLevelXPathDetail> configuration = new LinkedList<XMLLevelXPathDetail>();
		configuration.add(new XMLLevelXPathDetail(1, "./metadata", false));
		XMLLevelXPathDetail xPathDetail = new XMLLevelXPathDetail(2, StringUtils.EMPTY);
		xPathDetail.addExcludeNode("curation");
		configuration.add(xPathDetail);
		
		System.out.println("Start!!");
		
		String sourcePath = "/home/dspace/debasis/NDL/IAR/raw_data/video";
		String targetPath = "/home/dspace/debasis/NDL/IAR/csv_data";
		DefaultXML2CSVConverter converter = new DefaultXML2CSVConverter(sourcePath, targetPath, configuration);
		converter.setFilePrefix("rekhta.");
		converter.process();
		
		System.out.println("End!!");
		
		/*String json = gson.toJson(configuration);
		System.out.println(json);*/
	}

}