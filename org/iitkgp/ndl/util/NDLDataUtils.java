package org.iitkgp.ndl.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.TransformerException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.data.AIPFileGroup;
import org.iitkgp.ndl.data.AIPItemContents;
import org.iitkgp.ndl.data.CSVConfiguration;
import org.iitkgp.ndl.data.DataType;
import org.iitkgp.ndl.data.NDLAssetType;
import org.iitkgp.ndl.data.NDLDataItem;
import org.iitkgp.ndl.data.NDLDataPair;
import org.iitkgp.ndl.data.NDLField;
import org.iitkgp.ndl.data.NDLLanguageTranslate;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.Transformer;
import org.iitkgp.ndl.data.asset.AssetDetail;
import org.iitkgp.ndl.data.asset.NDLAssetDetail;
import org.iitkgp.ndl.data.asset.NDLAssetFilterHandler;
import org.iitkgp.ndl.data.compress.CompressedDataReader;
import org.iitkgp.ndl.data.compress.CompressedDataWriter;
import org.iitkgp.ndl.data.compress.CompressedFileMode;
import org.iitkgp.ndl.data.compress.TarGZCompressedFileReader;
import org.iitkgp.ndl.data.compress.TarGZCompressedFileWriter;
import org.iitkgp.ndl.data.container.AbstractDataContainer;
import org.iitkgp.ndl.data.normalizer.exception.DataNormalizationException;
import org.iitkgp.ndl.data.validator.AbstractNDLDataValidationBox;
import org.iitkgp.ndl.relation.HasPart;
import org.iitkgp.ndl.relation.IsPartOf;
import org.jsoup.Jsoup;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.itextpdf.text.pdf.PdfReader;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;

/**
 * <pre>NDL data related utilities, for more details see API list.</pre>
 * <pre>Some configurations defined in <b>/conf/default.global.configuration.properties</b></pre>
 * Here parameter <b>field</b> refers to NDL field name
 * @author Debasis, Aurghya, Vishal
 */
public class NDLDataUtils {
	
	static String FILE_NAME_DATE_PATTERN = "yyyy.MMM.dd.HH.mm.ss";
	static String FILE_NAME_DATE_ONLY_PATTERN = "yyyy.MMM.dd";
	static SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat(FILE_NAME_DATE_PATTERN);
	static SimpleDateFormat DATE_ONLY_FORMATTER = new SimpleDateFormat(FILE_NAME_DATE_ONLY_PATTERN);
	
	/**
	 * NDL date format
	 */
	public static String NDL_DATE_FORMAT;
	
	/**
	 * NDL date format regx
	 */
	public static String NDL_DATE_FORMAT_REGX;
	
	/**
	 * default asset suffix
	 */
	public static String DEFAULT_ASSET = "DEFAULT_ASSET";
	
	/**
	 * DEFAULT CSV separator
	 */
	public static final char DEFAULT_CSV_SEPARATOR = ',';
	
	/**
	 * DEFAULT CSV QUOTE character
	 */
	public static final char DEFAULT_CSV_QUOTE_CHARACTER = '"';
	
	/**
	 * new line
	 */
	public static String NEW_LINE = System.getProperty("line.separator");
	/**
	 * global logger file name, see {@link AbstractDataContainer#log(String)}
	 */
	public static String GLOBAL_LOGGER_FILE_NAME;
	/**
	 * HTML escape Gson object to manipulate json
	 */
	public static Gson HTML_ESCAPE_GSON = new GsonBuilder().disableHtmlEscaping().create();
	/**
	 * AIP mets file
	 */
	public static String METS_XML_FILE;
	/**
	 * SIP dubline core xml file
	 */
	public static String DUBLINECORE_FILE;
	/**
	 * SIP LRMI xml file
	 */
	public static String LRMI_FILE;
	/**
	 * SIP handle file
	 */
	public static String HANDLE_FILE;
	/**
	 * SIP NDL metadata file
	 */
	public static String METADATA_NDL_FILE;
	/**
	 * SIP NDL contents file
	 */
	public static String CONTENTS_FILE;
	/**
	 * AIP item file prefix
	 */
	public static String AIP_ITEM_FILE_PREFIX;
	/**
	 * AIP collection file prefix
	 */
	public static String AIP_COLLECTION_FILE_PREFIX;
	/**
	 * AIP bitstream file prefix
	 */
	public static String AIP_BITSTREAM_FILE_PREFIX;
	/**
	 * AIP collection file prefix
	 */
	public static String AIP_COMMUNITY_FILE_PREFIX;
	/**
	 * DCVALUE tag
	 */
	public static final String DCVALUE_TAG = "dcvalue";
	/**
	 * ELEMENT tag
	 */
	public static final String ELEMENT_TAG = "element";
	/**
	 * QUALIFIER tag
	 */
	public static final String QUALIFIER_TAG = "qualifier";
	
	/**
	 * Value indicates to remove the field
	 */
	public static final String FIELD_DELETE = "delete";
	
	// invalid JSON key
	static final String INVALID_JSON_KEY = "__NA__";
	
	// PT time format
	// static final String PT_FORMAT = "yyyy:MM:dd:hh:mm:ss";
	// PT format mapping
	//static final Map<Integer, Character> PT_MAPPING = new LinkedHashMap<Integer, Character>(8);
	static final char[] PT_MAPPING_SYMBOLS = new char[]{'Y', 'M', 'D', 'H', 'M', 'S'};

	/**
	 * SIP file name to schema mapping
	 * example: dubline_core.xml-&gt;dc
	 */
	public static Map<String, String> FILE2SCHEMA_SIP = new HashMap<String, String>(4);
	/**
	 * SIP schema to file name mapping
	 * example: dc-&gt;dubline_core.xml
	 */
	public static Map<String, String> SCHEMA2FILE_SIP = new HashMap<String, String>(4);
	
	/**
	 * Space identification regular expression
	 */
	public static final String SPACE_REGX = "( | |	)+";
	
	/**
	 * Multiple line identification regular expression
	 */
	public static final String MULTILINE_REGX = "(\\r?\\n)|(\\r)";
	
	/**
	 * Detail (handle wise errors) validation logger name
	 */
	public static final String DETAIL_VALIDATION_LOGGER = "detail.validation.logger";
	
	/**
	 * Less (a brief) validation logger name
	 */
	public static final String LESS_VALIDATION_LOGGER = "less.validation.logger";
	
	// handle ID pattern
	static Pattern HANDLE_PATTERN = Pattern.compile("[0-9A-Za-z_.-]+");
	
	// SAX parser for LATEX simple sup/sub handle
	static SAXParserFactory SAXF = SAXParserFactory.newInstance();
	static SAXParser SAXP;
	
	// internal usages
	static Type MAP_TYPE = new TypeToken<Map<String, String>>(){}.getType();
	static Set<String> NUMERIC_JSON_KEY = new HashSet<String>();
	// array pattern to parse json-path expression
	static Pattern ARRAY_PATTERN = Pattern.compile("(([a-zA-Z]+[0-9]*)\\[)([0-9]+)(\\])");
	// asset mapping
	static String NDL_ASSET_MAPPING_FILE = "/conf/ndl.asset.mapping.conf.properties";
	static Properties NDL_ASSET_MAPPING = new Properties(); // asset type to name
	// name normalization configuration
	static String NDL_NAME_NORMALIZATION_COFIGURATION_FILE = "/conf/default.name.normlization.properties";
	static Properties NDL_NAME_NORMALIZATION_COFIGURATION = new Properties();
	
	// has-part details
	static Type HASPART_LIST_TYPE = new TypeToken<List<HasPart>>(){}.getType();
	// roman details
	static Pattern ROMAN_NUMBER_REGEX = Pattern
			.compile("^(?=[MDCLXVI])M*(C[MD]|D?C{0,3})(X[CL]|L?X{0,3})(I[XV]|V?I{0,3})$", Pattern.CASE_INSENSITIVE);
	static Map<Character, Integer> ROMAN_VALUE_MAPPING = new HashMap<Character, Integer>(4);
	
	// words to number conversion mapping
	static Map<String, NDLDataPair<String>> WORDS2NUMBER_MAPPING = new HashMap<>();
	
	// months
	static Map<Integer, String> SHORTHAND_DATE_DISPLAY = new HashMap<Integer, String>(4);
	static Map<String, MonthDetail> MONTH_NAMES = new HashMap<String, MonthDetail>();
	static Map<Integer, MonthDetail> MONTH_DETAILS = new HashMap<Integer, MonthDetail>();
	static class MonthDetail {
		String name;
		int index;
		int days;
		
		public MonthDetail(String name, int index, int days) {
			this.name = name;
			this.index = index;
			this.days = days;
		}
	}
	
	static {
		// context startup
		NDLConfigurationContext.init();
		
		NDL_DATE_FORMAT = NDLConfigurationContext.getConfiguration("ndl.date.format");
		NDL_DATE_FORMAT_REGX = NDLConfigurationContext.getConfiguration("ndl.date.format.regx");
		
		// init
		GLOBAL_LOGGER_FILE_NAME = NDLConfigurationContext.getConfiguration("global.logger.log.file.name");
		METS_XML_FILE = NDLConfigurationContext.getConfiguration("aip.mets.file.name");
		DUBLINECORE_FILE = NDLConfigurationContext.getConfiguration("sip.dc.file.name");
		LRMI_FILE = NDLConfigurationContext.getConfiguration("sip.lrmi.file.name");
		HANDLE_FILE = NDLConfigurationContext.getConfiguration("sip.handle.file.name");
		METADATA_NDL_FILE = NDLConfigurationContext.getConfiguration("sip.ndl.file.name");
		CONTENTS_FILE = NDLConfigurationContext.getConfiguration("sip.contents.file.name");
		AIP_ITEM_FILE_PREFIX = NDLConfigurationContext.getConfiguration("aip.item.file.prefix");
		AIP_COLLECTION_FILE_PREFIX = NDLConfigurationContext.getConfiguration("aip.collection.file.prefix");
		AIP_BITSTREAM_FILE_PREFIX = NDLConfigurationContext.getConfiguration("aip.bitstream.file.prefix");
		AIP_COMMUNITY_FILE_PREFIX = NDLConfigurationContext.getConfiguration("aip.community.file.prefix");
		// DCVALUE_TAG = NDLConfigurationContext.getConfiguration("sip.dcvalue.tag");
		// ELEMENT_TAG = NDLConfigurationContext.getConfiguration("element.tag");
		// QUALIFIER_TAG = NDLConfigurationContext.getConfiguration("qualifier.tag");
		
		// static block initialization
		FILE2SCHEMA_SIP.put(DUBLINECORE_FILE, "dc");
		FILE2SCHEMA_SIP.put(LRMI_FILE, "lrmi");
		FILE2SCHEMA_SIP.put(METADATA_NDL_FILE, "ndl");
		// vice versa
		SCHEMA2FILE_SIP.put("dc", DUBLINECORE_FILE);
		SCHEMA2FILE_SIP.put("lrmi", LRMI_FILE);
		SCHEMA2FILE_SIP.put("ndl", METADATA_NDL_FILE);

		// numeric JOSN key
		List<String> numericKeys = Arrays
				.asList(NDLConfigurationContext.getConfiguration("data.numeric.json.keys").split("\\|"));
		NUMERIC_JSON_KEY.addAll(numericKeys);
		
		// NDL asset mapping
		try {
			NDL_ASSET_MAPPING.load(loadResource(NDL_ASSET_MAPPING_FILE));
		} catch(Exception ex) {
			// error
			throw new IllegalStateException(
					"NDL asset mapping configuration file not found: " + NDL_ASSET_MAPPING_FILE);
		}
		
		// NDL asset mapping
		try {
			NDL_NAME_NORMALIZATION_COFIGURATION.load(loadResource(NDL_NAME_NORMALIZATION_COFIGURATION_FILE));
		} catch(Exception ex) {
			// error
			throw new IllegalStateException(
					"NDL configuration file not found: " + NDL_NAME_NORMALIZATION_COFIGURATION_FILE);
		}
		
		// SAX parser loading
		try {
			SAXP = SAXF.newSAXParser();
		} catch(Exception ex) {
			// error
			throw new IllegalStateException(ex.getMessage(), ex);
		}
		
		// short hand date display
		SHORTHAND_DATE_DISPLAY.put(1, "st");
		SHORTHAND_DATE_DISPLAY.put(2, "nd");
		SHORTHAND_DATE_DISPLAY.put(3, "rd");
		
		// momths
		MONTH_NAMES.put("jan", new MonthDetail("jan", 1, 31));
		MONTH_NAMES.put("january", new MonthDetail("january", 1, 31));
		MONTH_NAMES.put("feb", new MonthDetail("feb", 2, 28));
		MONTH_NAMES.put("february", new MonthDetail("february", 2, 28));
		MONTH_NAMES.put("mar", new MonthDetail("mar", 3, 31));
		MONTH_NAMES.put("march", new MonthDetail("march", 3, 31));
		MONTH_NAMES.put("apr", new MonthDetail("apr", 4, 30));
		MONTH_NAMES.put("april", new MonthDetail("april", 4, 30));
		MONTH_NAMES.put("may", new MonthDetail("may", 5, 31));
		MONTH_NAMES.put("jun", new MonthDetail("jun", 6, 30));
		MONTH_NAMES.put("june", new MonthDetail("june", 6, 30));
		MONTH_NAMES.put("jul", new MonthDetail("jul", 7, 31));
		MONTH_NAMES.put("july", new MonthDetail("july", 7, 31));
		MONTH_NAMES.put("aug", new MonthDetail("aug", 8, 31));
		MONTH_NAMES.put("august", new MonthDetail("august", 8, 31));
		MONTH_NAMES.put("sep", new MonthDetail("sep", 9, 30));
		MONTH_NAMES.put("september", new MonthDetail("september", 9, 30));
		MONTH_NAMES.put("oct", new MonthDetail("oct", 10, 31));
		MONTH_NAMES.put("october", new MonthDetail("october", 10, 31));
		MONTH_NAMES.put("nov", new MonthDetail("nov", 11, 30));
		MONTH_NAMES.put("november", new MonthDetail("november", 11, 30));
		MONTH_NAMES.put("dec", new MonthDetail("dec", 12, 31));
		MONTH_NAMES.put("december", new MonthDetail("december", 12, 31));
		
		MONTH_DETAILS.put(1, new MonthDetail("jan|january", 1, 31));
		MONTH_DETAILS.put(2, new MonthDetail("feb|february", 2, 28));
		MONTH_DETAILS.put(3, new MonthDetail("mar|march", 3, 31));
		MONTH_DETAILS.put(4, new MonthDetail("apr|april", 4, 30));
		MONTH_DETAILS.put(5, new MonthDetail("may", 5, 31));
		MONTH_DETAILS.put(6, new MonthDetail("jun|june", 6, 30));
		MONTH_DETAILS.put(7, new MonthDetail("jul|july", 7, 31));
		MONTH_DETAILS.put(8, new MonthDetail("aug|august", 8, 31));
		MONTH_DETAILS.put(9, new MonthDetail("sep|september", 9, 30));
		MONTH_DETAILS.put(10, new MonthDetail("oct|october", 10, 31));
		MONTH_DETAILS.put(11, new MonthDetail("nov|november", 11, 30));
		MONTH_DETAILS.put(12, new MonthDetail("dec|december", 12, 31));
		
		ROMAN_VALUE_MAPPING.put('i', 1);
		ROMAN_VALUE_MAPPING.put('v', 5);
		ROMAN_VALUE_MAPPING.put('x', 10);
		ROMAN_VALUE_MAPPING.put('l', 50);
		ROMAN_VALUE_MAPPING.put('c', 100);
		ROMAN_VALUE_MAPPING.put('d', 500);
		ROMAN_VALUE_MAPPING.put('m', 1000);
		
		// words to number mapping detail
		WORDS2NUMBER_MAPPING.put("zero", new NDLDataPair<>("0", "+"));
		WORDS2NUMBER_MAPPING.put("one", new NDLDataPair<>("1", "+"));
		WORDS2NUMBER_MAPPING.put("two", new NDLDataPair<>("2", "+"));
		WORDS2NUMBER_MAPPING.put("three", new NDLDataPair<>("3", "+"));
		WORDS2NUMBER_MAPPING.put("four", new NDLDataPair<>("4", "+"));
		WORDS2NUMBER_MAPPING.put("five", new NDLDataPair<>("5", "+"));
		WORDS2NUMBER_MAPPING.put("six", new NDLDataPair<>("6", "+"));
		WORDS2NUMBER_MAPPING.put("seven", new NDLDataPair<>("7", "+"));
		WORDS2NUMBER_MAPPING.put("eight", new NDLDataPair<>("8", "+"));
		WORDS2NUMBER_MAPPING.put("nine", new NDLDataPair<>("9", "+"));
		WORDS2NUMBER_MAPPING.put("ten", new NDLDataPair<>("10", "+"));
		WORDS2NUMBER_MAPPING.put("eleven", new NDLDataPair<>("11", "+"));
		WORDS2NUMBER_MAPPING.put("twelve", new NDLDataPair<>("12", "+"));
		WORDS2NUMBER_MAPPING.put("thirteen", new NDLDataPair<>("13", "+"));
		WORDS2NUMBER_MAPPING.put("fourteen", new NDLDataPair<>("14", "+"));
		WORDS2NUMBER_MAPPING.put("fifteen", new NDLDataPair<>("15", "+"));
		WORDS2NUMBER_MAPPING.put("sixteen", new NDLDataPair<>("16", "+"));
		WORDS2NUMBER_MAPPING.put("seventeen", new NDLDataPair<>("17", "+"));
		WORDS2NUMBER_MAPPING.put("eighteen", new NDLDataPair<>("18", "+"));
		WORDS2NUMBER_MAPPING.put("nineteen", new NDLDataPair<>("19", "+"));
		WORDS2NUMBER_MAPPING.put("twenty", new NDLDataPair<>("20", "+"));
		WORDS2NUMBER_MAPPING.put("thirty", new NDLDataPair<>("30", "+"));
		WORDS2NUMBER_MAPPING.put("forty", new NDLDataPair<>("40", "+"));
		WORDS2NUMBER_MAPPING.put("fifty", new NDLDataPair<>("50", "+"));
		WORDS2NUMBER_MAPPING.put("sixty", new NDLDataPair<>("60", "+"));
		WORDS2NUMBER_MAPPING.put("seventy", new NDLDataPair<>("70", "+"));
		WORDS2NUMBER_MAPPING.put("eighty", new NDLDataPair<>("80", "+"));
		WORDS2NUMBER_MAPPING.put("ninety", new NDLDataPair<>("90", "+"));
		WORDS2NUMBER_MAPPING.put("hundred", new NDLDataPair<>("100", "*"));
		WORDS2NUMBER_MAPPING.put("thousand", new NDLDataPair<>("1000", "*"));
		WORDS2NUMBER_MAPPING.put("million", new NDLDataPair<>("1000000", "*"));
		WORDS2NUMBER_MAPPING.put("billion", new NDLDataPair<>("1000000000", "*"));
		WORDS2NUMBER_MAPPING.put("trillion", new NDLDataPair<>("1000000000000L", "*"));
	}
	
	// for internal usage
	static class SAXPHandler4SimpleSupSubLatex extends DefaultHandler {
		//boolean f = false;
		boolean sup = false, sub = false;
		StringBuilder mtext = new StringBuilder();
		
		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes)
				throws SAXException {
			//System.out.println(qName);
			if(qName.equals("sup")) {
				sup = true;
				sub = false;
			} else if(qName.equals("sub")) {
				sub = true;
				sup = false;
			}
		}
		
		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			sup = sub = false;
		}

		@Override
		public void characters(char[] ch, int start, int length) throws SAXException {
			//System.out.println("TV: " + StringEscapeUtils.unescapeXml(new String(ch, start, length)));
			//System.out.println("TV: " + new String(ch, start, length));
			String v = new String(ch, start, length);
			if(sup) {
				v = "<sup>" + v.replaceAll(" +", "") + "</sup>";
			} else if(sub) {
				v = "<sub>" + v.replaceAll(" +", "") + "</sub>";
			}
			mtext.append(v);
		}
	}
	
	// leap year check
	static boolean isLeapYear(int year) {
		boolean leap = false;
		if(year % 4 == 0) {
			if (year % 100 == 0) {
				// year is divisible by 400, hence the year is a leap year
				if (year % 400 == 0) {
					leap = true;
				} else {
					leap = false;
				}
			} else {
				leap = true;
			}
		} else {
			leap = false;
		}
		return leap;
	}
	
	/**
	 * Name normalization configuration setup
	 * @param key configuration key name
	 * @param value configuration key value
	 */
	public static void addNameNormalizationConfiguration(String key, String value) {
		NDL_NAME_NORMALIZATION_COFIGURATION.setProperty(key, value);
	}
	
	/**
	 * Gets shorthand date from given date (1-31)
	 * @param date given date
	 * @return returns shorthand display name
	 */
	public static String getShorthandDisplayDate(int date) {
		if(date > 10 && date < 21) {
			return date + "th";
		} else {
			int idx = date % 10;
			if(idx > 0 && idx < 4) {
				return date + SHORTHAND_DATE_DISPLAY.get(idx);
			} else {
				return date + "th";
			}
		}
	}
	
	/**
	 * Compares month by name
	 * @param month1 month 1
	 * @param month2 month 2
	 * @return returns comparison code
	 */
	public static int compareMonth(String month1, String month2) {
		MonthDetail md1 = MONTH_NAMES.get(month1.toLowerCase());
		MonthDetail md2 = MONTH_NAMES.get(month2.toLowerCase());
		return Integer.valueOf(md1.index).compareTo(Integer.valueOf(md2.index));
	}
	
	/**
	 * Creates set with given values
	 * @param values given values
	 * @param <T> type T
	 * @return returns created set
	 */
	@SafeVarargs
	public static <T> Set<T> createSet(T ... values) {
		Set<T> set = new HashSet<T>(2);
		for(T value : values) {
			set.add(value);
		}
		
		return set;
	}
	
	/**
	 * Creates list with given values
	 * @param values given values
	 * @param <T> type T
	 * @return returns created list
	 */
	@SafeVarargs
	public static <T> List<T> createList(T ... values) {
		List<T> list = new LinkedList<T>();
		for(T value : values) {
			list.add(value);
		}
		
		return list;
	}
	
	/**
	 * Loads resource from classpath for a given configuration file
	 * @param configurationFile given configuration file
	 * @return returns loaded input stream 
	 * @throws IOException throws exception in case loading error occurs
	 */
	public static InputStream loadResource(String configurationFile) throws IOException {
		return NDLDataUtils.class.getResourceAsStream(configurationFile);
	}
	
	/**
	 * Gets resource path for a given resource from classpath
	 * @param resource given resource
	 * @return returns resource path
	 */
	public static String getResourcePath(String resource) {
		return NDLDataUtils.class.getResource(resource).getPath();
	}
	
	/**
	 * Gets array variable name and index by expression, array[0] returns array and 0
	 * @param expression expression to evaluate
	 * @return returns variable name and index
	 */
	public static String[] getArrayIndex(String expression) {
		Matcher m = ARRAY_PATTERN.matcher(expression);
		if(m.find()) {
			return new String[]{m.group(2), m.group(3)};
		} else {
			return null;
		}
	}
	
	/**
	 * Checks whether JSON field is NDL numeric or not,
	 * see <b>/conf/default.global.configuration.properties data.numeric.json.keys</b>
	 * @param field field name
	 * @return returns true whether it's numeric or not
	 */
	public static boolean isNumericJSONKeyedField(String field) {
		return NUMERIC_JSON_KEY.contains(field);
	}
	
	/**
	 * Gets SIP schema name by file name, see {@link #FILE2SCHEMA_SIP}
	 * @param file file name
	 * @return returns associated schema, if not found returns NULL
	 */
	public static String getSchema4File(String file) {
        return FILE2SCHEMA_SIP.get(file);
    }
	
	/**
	 * Gets SIP schema name by NDL field, see {@link #getSchema4File(String)} 
	 * @param field NDL field name
	 * @return returns associated schema, if not found returns NULL
	 */
	public static String getSchema4Field(String field) {
		String tokens[] = field.split("\\.");
		return tokens[0];
	}

	/**
	 * Opens CSV file for writing
	 * @param out out file to write data
	 * @param separator CSV separator
	 * @param quote CSV quote
	 * @return returns writer instance
	 * @throws IOException throws exception if open fails
	 */
	public static CSVWriter openCSV(File out, char separator, char quote) throws IOException {
		return new CSVWriter(new FileWriter(out), separator, quote, CSVWriter.DEFAULT_ESCAPE_CHARACTER,
				CSVWriter.DEFAULT_LINE_END);
	}
	
	// internal usage
	static CSVParser getCSVParser(char separator, char quote) {
		return new CSVParserBuilder().withSeparator(separator).withQuoteChar(quote).withEscapeChar('\0').build();
	}
	
	/**
	 * Opens CSV file for writing with default settings
	 * @param out out file to write data
	 * @return returns writer instance
	 * @throws IOException throws exception if open fails
	 */
	public static CSVWriter openCSV(File out) throws IOException {
		return openCSV(out, ',', '"');
	}
	
	/**
	 * Opens CSV file for read
	 * @param in input CSV file
	 * @param separator CSV separator
	 * @param quote CSV quote
	 * @param multilineLimit multiple line limit
	 * @param startIndex 0 index based start row index
	 * @return returns reader instance
	 * @throws IOException throws exception if open fails
	 */
	public static CSVReader readCSV(File in, char separator, char quote, int multilineLimit, int startIndex)
			throws IOException {
		CSVParser parser = getCSVParser(separator, quote);
		return new CSVReaderBuilder(new BufferedReader(new FileReader(in))).withCSVParser(parser)
				.withSkipLines(startIndex).withMultilineLimit(multilineLimit).build();
	}
	
	/**
	 * Opens CSV file for read
	 * @param in input CSV file
	 * @param separator CSV separator
	 * @param quote CSV quote
	 * @param startIndex 0 index based start row index
	 * @return returns reader instance
	 * @throws IOException throws exception if open fails
	 */
	public static CSVReader readCSV(File in, char separator, char quote, int startIndex) throws IOException {
		CSVParser parser = getCSVParser(separator, quote);
		return new CSVReaderBuilder(new BufferedReader(new FileReader(in))).withCSVParser(parser)
				.withSkipLines(startIndex).build();
	}
	
	/**
	 * Opens CSV file for read
	 * @param in input CSV file
	 * @param separator CSV separator
	 * @param quote CSV quote
	 * @param multilineLimit multiple line limit
	 * @return returns reader instance
	 * @throws IOException throws exception if open fails
	 */
	public static CSVReader readCSVWithMultiline(File in, char separator, char quote, int multilineLimit)
			throws IOException {
		CSVParser parser = getCSVParser(separator, quote);
		return new CSVReaderBuilder(new BufferedReader(new FileReader(in))).withCSVParser(parser)
				.withMultilineLimit(multilineLimit).build();
	}
	
	/**
	 * Opens CSV file for read
	 * @param reader reader detail
	 * @param separator CSV separator
	 * @param quote CSV quote
	 * @param multilineLimit multiple line limit
	 * @param startIndex 0 index based start row index
	 * @return returns reader instance
	 * @throws IOException throws exception if open fails
	 */
	public static CSVReader readCSV(Reader reader, char separator, char quote, int multilineLimit, int startIndex)
			throws IOException {
		CSVParser parser = getCSVParser(separator, quote);
		return new CSVReaderBuilder(new BufferedReader(reader)).withCSVParser(parser).withSkipLines(startIndex)
				.withMultilineLimit(multilineLimit).build();
	}
	
	/**
	 * Opens CSV file for read
	 * @param reader reader detail
	 * @param separator CSV separator
	 * @param quote CSV quote
	 * @param multilineLimit multiple line limit
	 * @return returns reader instance
	 * @throws IOException throws exception if open fails
	 */
	public static CSVReader readCSVWithMultiline(Reader reader, char separator, char quote, int multilineLimit)
			throws IOException {
		CSVParser parser = getCSVParser(separator, quote);
		return new CSVReaderBuilder(new BufferedReader(reader)).withCSVParser(parser).withMultilineLimit(multilineLimit)
				.build();
	}
	
	/**
	 * Opens CSV file for read
	 * @param reader reader detail
	 * @param separator CSV separator
	 * @param quote CSV quote
	 * @param startIndex 0 index based start row index
	 * @return returns reader instance
	 * @throws IOException throws exception if open fails
	 */
	public static CSVReader readCSV(Reader reader, char separator, char quote, int startIndex) throws IOException {
		CSVParser parser = getCSVParser(separator, quote);
		return new CSVReaderBuilder(new BufferedReader(reader)).withCSVParser(parser).withSkipLines(startIndex).build();
	}
	
	/**
	 * Opens CSV file for read
	 * @param reader reader detail
	 * @param separator CSV separator
	 * @param quote CSV quote
	 * @return returns reader instance
	 * @throws IOException throws exception if open fails
	 */
	public static CSVReader readCSV(Reader reader, char separator, char quote) throws IOException {
		CSVParser parser = getCSVParser(separator, quote);
		return new CSVReaderBuilder(new BufferedReader(reader)).withCSVParser(parser).build();
	}
	
	/**
	 * Opens CSV file read with default settings
	 * @param in input CSV file
	 * @param startIndex 0 index based start row index
	 * @return returns reader instance
	 * @throws IOException throws exception if open fails
	 */
	public static CSVReader readCSV(File in, int startIndex) throws IOException {
		return NDLDataUtils.readCSV(in, ',', '"', startIndex);
	}
	
	/**
	 * Opens CSV file for read
	 * @param in input CSV file
	 * @param separator CSV separator
	 * @param quote CSV quote
	 * @return returns CSV reader reference
	 * @throws IOException throws exception if open fails
	 */
	public static CSVReader readCSV(File in, char separator, char quote) throws IOException {
		CSVParser parser = getCSVParser(separator, quote);
		return new CSVReaderBuilder(new BufferedReader(new FileReader(in))).withCSVParser(parser).build();
	}
	
	/**
	 * Opens CSV file for read with default settings
	 * @param in input CSV file
	 * @return returns CSV reader reference
	 * @throws IOException throws exception if open fails
	 */
	public static CSVReader readCSV(File in) throws IOException {
		return NDLDataUtils.readCSV(in, ',', '"');
	}
	
	/**
	 * Loads AIP document 
	 * @param contents AIP document byte array contents
	 * @return returns loaded AIP document
	 * @throws IOException throws exception if loading fails
	 * @throws SAXException throws exception if invalid XML contents if provided
	 */
	public static AIPItemContents loadDocument4AIP(byte[] contents) throws IOException, SAXException {
		return loadDocument4AIP(contents, true);
	}

	/**
	 * Loads AIP document 
	 * @param contents AIP document byte array contents
	 * @param assetLoadingFlag this flag indicates whether to load asset or not
	 * @return returns loaded AIP document
	 * @throws IOException throws exception if loading fails
	 * @throws SAXException throws exception if invalid XML contents if provided
	 */
	public static AIPItemContents loadDocument4AIP(byte[] contents, boolean assetLoadingFlag)
			throws IOException, SAXException {
		ZipInputStream zipInputStream = new ZipInputStream(new ByteArrayInputStream(contents));
		ZipEntry itemEntry = null;

		AIPItemContents aipContents = null;
		Map<String, List<AIPFileGroup>> fileGroups = null;
		Map<String, byte[]> others = new HashMap<String, byte[]>(2);
		Document document = null;
		long size = 0;
		while ((itemEntry = zipInputStream.getNextEntry()) != null) {
			// read contents
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			byte itemContents[] = new byte[1024];
			int bytes;
			while ((bytes = zipInputStream.read(itemContents, 0, 1024)) > -1) {
				buffer.write(itemContents, 0, bytes);
			}
			buffer.flush(); // forces write all contents
			byte[] stream = buffer.toByteArray();
			buffer.close();
			
			String name = itemEntry.getName();
			if (name.equals(METS_XML_FILE)) {
				// size calculation
				size += stream.length;
				
				ByteArrayInputStream in = new ByteArrayInputStream(stream);
				document = NDLXMLUtils.DOC_BUILDER.parse(in);
				// populate file groups
				NodeList nodelist = document.getElementsByTagName("fileGrp");
				int s = nodelist.getLength();
				fileGroups = new HashMap<String, List<AIPFileGroup>>(2);
				for(int i = 0; i < s; i++) {
					// each file group
					Element node = (Element)nodelist.item(i);
					String type = node.getAttribute("USE");
					Element first = (Element)node.getElementsByTagName("file").item(0);
					Element second  = (Element)first.getElementsByTagName("FLocat").item(0);
					String fileName = second.getAttribute("xlink:href");
					// file group
					AIPFileGroup fg = new AIPFileGroup(fileName, first.getAttribute("MIMETYPE"),
							Long.valueOf(first.getAttribute("SIZE")));
					fg.setChecksum(first.getAttribute("CHECKSUM"));
					fg.setChecksumType(first.getAttribute("CHECKSUMTYPE"));
					// populate map
					List<AIPFileGroup> groups = fileGroups.get(type);
					if(groups == null) {
						groups = new LinkedList<AIPFileGroup>();
						fileGroups.put(type, groups);
					}
					groups.add(fg);
				}
				in.close();
			} else if(assetLoadingFlag) {
				// size calculation
				size += stream.length;
				// others if asset loading flag is enabled
				others.put(name, stream);
			}
			zipInputStream.closeEntry();
		}
		zipInputStream.close();
		
		aipContents = new AIPItemContents(document, others);
		aipContents.addFileGroups(fileGroups); // adds file groups if exists
		aipContents.size(size); // sets size
		return aipContents;
	}
	
	/**
	 * Gets AIP entry x-path
	 * @return return AIP entry x-path
	 */
	public static String getAIPEntryXPath() {
		return "//dmdSec[@ID='dmdSec_2']/mdWrap/xmlData/*[@dspaceType='ITEM']";
	}
	
	/**
	 * Checks whether given AIP item is valid or not
	 * @param document given AIP item
	 * @return return true if valid otherwise false
	 */
	public static boolean validateAIPItem(Document document) {
		if(document == null) {
			return false; // invalid
		}
		try {
			Node entry = NDLXMLUtils.getSingleNode(document, NDLDataUtils.getAIPEntryXPath());
			if(entry == null) {
				// invalid item
				return false;
			}
			return true; // valid
		} catch(Exception ex) {
			// invalid item
			return false;
		}
	}

	/**
	 * gets XPath for AIP document for given NDL field, for more details see AIP XML document
	 * @param field NDL field name
	 * @return returns XPath
	 */
	public static String getXPath4AIP(String field) {
		// dc.xx.yy => mdschema(dc) element(xx) qualifier(yy)
		String tokens[] = field.split("\\.");
		StringBuilder xpath = new StringBuilder();
		xpath.append("//dmdSec[@ID='dmdSec_2']//*[@mdschema='").append(tokens[0]).append("'][@element='")
				.append(tokens[1]).append("']");
		if (tokens.length == 3) {
			xpath.append("[@qualifier='").append(tokens[2]).append("']");
		} else {
			xpath.append("[not(@qualifier)]");
		}
		return xpath.toString();
	}
	
	/**
	 * gets XPath for AIP document for given NDL field, for more details see AIP XML document
	 * @param field NDL field name
	 * @return returns XPath
	 */
	public static String getXPath4AIP(NDLField field) {
		return getXPath4AIP(field.getField());
	}

	/**
	 * gets XPath for SIP document for given NDL field, for more details see SIP XML document(s)
	 * @param field NDL field name
	 * @return returns XPath
	 */
	public static String getXPath4SIP(String field) {
		// dc.xx.yy => particular-document element(xx) qualifier(yy)
		String tokens[] = field.split("\\.");
		StringBuilder xpath1 = new StringBuilder();
		StringBuilder xpath2 = new StringBuilder();
		xpath1.append("//dcvalue[@element='").append(tokens[1]).append("']");
		if (tokens.length == 3) {
			xpath1.append("[@qualifier='").append(tokens[2]).append("']");
		} else {
			xpath1.append("[not(@qualifier)]");
			// also consider QUALIFIER=NONE
			xpath2.append("//dcvalue[@element='").append(tokens[1]).append("']");
			xpath2.append("[@qualifier='none']");
		}
		if(xpath2.length() > 0) {
			xpath1.append("|").append(xpath2);
		}
		return xpath1.toString();
	}
	
	/**
	 * gets XPath for SIP document for given NDL field, for more details see SIP XML document(s)
	 * @param field NDL field name
	 * @return returns XPath
	 */
	public static String getXPath4SIP(NDLField field) {
		// dc.xx.yy => particular-document element(xx) qualifier(yy)
		return getXPath4SIP(field.getField());
	}
	
	/**
	 * Converts map&lt;string, string&gt; for a NDL json which has one key and one value
	 * @param json json text
	 * @return returns map&lt;string, string&gt; which has one key and one value
	 */
	public static Map<String, String> mapFromJson(String json) {
		return mapFromJson(json, false);
	}
	
	/**
	 * Converts map&lt;string, string&gt; for a NDL json which has one key and one value
	 * @param json json text
	 * @param suppressError this flag indicates whether to suppress error or not
	 * @return returns map&lt;string, string&gt; which has one key and one value
	 */
	public static Map<String, String> mapFromJson(String json, boolean suppressError) {
		if(StringUtils.isBlank(json)) {
			// handle blank case
			return null;
		}
		try {
			return HTML_ESCAPE_GSON.fromJson(json, MAP_TYPE);
		} catch(Exception ex) {
			if(suppressError) {
				// empty map
				System.err.println("[WARN] Malformed JSON: " + json);
				return new HashMap<String, String>(2);
			} else {
				throw ex;
			}
		}
	}
	
	/**
	 * Gets  value by json key if exists
	 * @param json json string
	 * @param key given key name
	 * @param suppressError error suppression for JSON parsing
	 * @return returns value associated with key
	 */
	public static String getValueByJsonKey(String json, String key, boolean suppressError) {
		try {
			// json parsing
			Map<String, String> map = NDLDataUtils.mapFromJson(json);
			return map.get(key);
		} catch(Exception ex) {
			// JSON error
			if(suppressError) {
				if(getJsonParseErrorDisplayFlag()) {
					System.err.println("[WARN] " + ex.getMessage());
				}
			} else {
				// propagate error
				throw ex;
			}
		}
		return null;
	}
	
	/**
	 * Gets  value by json key if exists
	 * Note: If JSON error occurs then it suppresses and returns NULL value
	 * @param json json string
	 * @param key given key name
	 * @return returns value associated with key
	 */
	public static String getValueByJsonKey(String json, String key) {
		return getValueByJsonKey(json, key, true);
	}
	
	/**
	 * Gets  values by json key if exists
	 * Note: If JSON error occurs then it suppresses and returns empty list
	 * @param json json string values
	 * @param key given key name
	 * @return returns value associated with key
	 */
	public static Collection<String> getValuesByJsonKey(Collection<String> json, String key) {
		List<String> values = new LinkedList<String>();
		for(String j : json) {
			String v = getValueByJsonKey(j, key);
			if(StringUtils.isNotBlank(v)) {
				values.add(v);
			}
		}
		return values;
	}
	
	/**
	 * Gets json key and value for a json value
	 * @param json given json
	 * @param suppressError error suppression for JSON parsing
	 * @return returns key/value pair
	 */
	public static String[] getJSONKeyedValue(String json, boolean suppressError) {
		return getJSONKeyedValue(json, false, suppressError);
	}
	
	/**
	 * returns whether invalid JSON key
	 * @param pair JSON pair
	 * @return returns true invalid JSON otehrwise false
	 */
	public static boolean invalidJSON(String[] pair) {
		return StringUtils.equalsIgnoreCase(pair[0], INVALID_JSON_KEY);
	}
	
	/**
	 * Gets json key and value for a json value
	 * @param json given json
	 * @param escapeHTML whether to escape HTML or not
	 * @param suppressError error suppression for JSON parsing
	 * @return returns key/value pair
	 */
	public static String[] getJSONKeyedValue(String json, boolean escapeHTML, boolean suppressError) {
		if(StringUtils.isBlank(json)) {
			// invalid JSON
			return new String[]{INVALID_JSON_KEY, "__NA__"};
		}
		Map<String, String> individual = null;
		try {
			// json parsing
			individual = NDLDataUtils.mapFromJson(json);
		} catch(Exception ex) {
			// JSON error
			if(suppressError) {
				if(getJsonParseErrorDisplayFlag()) {
					System.err.println("[WARN] " + ex.getMessage());
				}
				return new String[]{INVALID_JSON_KEY, "__NA__"};
			} else {
				// propagate error
				throw ex;
			}
		}
		String key = individual.keySet().iterator().next();
		String value = individual.get(key).trim();
		return new String[]{key.trim(), escapeHTML ? StringEscapeUtils.unescapeHtml4(value) : value};
	}
	
	/**
	 * Gets json key and value for a json value
	 * @param json given json
	 * @return returns key/value pair
	 */
	public static String[] getJSONKeyedValue(String json) {
		return getJSONKeyedValue(json, false, true);
	}
	
	/**
	 * returns json text for a given object
	 * @param object given object
	 * @return returns json text
	 */
	public static String getJson(Object object) {
		return HTML_ESCAPE_GSON.toJson(object);
	}
	
	/**
	 * Gets json text for given single key-value pair
	 * @param key json key
	 * @param value associated value
	 * @return returns json text
	 */
	public static String getJson(String key, Object value) {
		if(StringUtils.isBlank(value.toString())) {
			// invalid
			return StringUtils.EMPTY;
		}
		Map<String, Object> jsonDetail = new HashMap<String, Object>(2);
		jsonDetail.put(key, value);
		
		return HTML_ESCAPE_GSON.toJson(jsonDetail);
	}
	
	/**
	 * <pre>gets associated value for a given json key and json-text</pre>
	 * <pre>note: it prints an error message in case malformed json found</pre>
	 * @param fieldValue json text
	 * @param jsonKey json key
	 * @return returns associated value
	 */
	public static String getNDLFieldValue(String fieldValue, String jsonKey) {
		return getNDLFieldValue(fieldValue, jsonKey, true);
	}
	
	/**
	 * <pre>gets associated value for a given json key and json-text</pre>
	 * <pre>note: it prints an error message in case malformed json found</pre>
	 * @param fieldValue json text
	 * @param jsonKey json key
	 * @param flushError flush error on json parsing error
	 * @return returns associated value
	 */
	public static String getNDLFieldValue(String fieldValue, String jsonKey, boolean flushError) {
		return getNDLFieldValue(fieldValue, jsonKey, false, flushError);
	}
	
	/**
	 * <pre>gets associated value for a given json key and json-text</pre>
	 * <pre>note: it prints an error message in case malformed json found</pre>
	 * @param fieldValue json text
	 * @param jsonKey json key
	 * @param escapeHTML escape HTML flag
	 * @param flushError flush error on json parsing error
	 * @return returns associated value
	 */
	public static String getNDLFieldValue(String fieldValue, String jsonKey, boolean escapeHTML, boolean flushError) {
		if(StringUtils.isNotBlank(fieldValue)) {
			// valid value
			if(StringUtils.isNotBlank(jsonKey)) {
				// json key exists
				try {
					Map<String, String> map = NDLDataUtils.mapFromJson(fieldValue);
					String v = map.get(jsonKey);
					if(escapeHTML) {
						return StringEscapeUtils.unescapeHtml4(v);
					} else {
						return v;
					}
				} catch(Exception ex) {
					// error
					if(flushError) {
						System.err.println("[ERROR] Malformed json: " + fieldValue);
						return fieldValue;
					} else {
						// skip
						return null;
					}
				}
			} else {
				if(escapeHTML) {
					return StringEscapeUtils.unescapeHtml4(fieldValue);
				} else {
					return fieldValue;
				}
			}
		} else {
			return fieldValue;
		}
	}
	
	/**
	 * Checks whether field json-key exists for an item
	 * @param item given item
	 * @param field field name
	 * @param jsonKey given json-key
	 * @return returns true if exists otherwise false
	 */
	public static boolean existNDLFieldKey(NDLDataItem item, String field, String jsonKey) {
		// json key exists
		Map<String, String> map = NDLDataUtils.mapFromJson(field);
		if(map.containsKey(jsonKey)) {
			// found
			return true;
		}
		return false;
	}
	
	/**
	 * Gets XML byte contents from XML document object 
	 * @param document XML document object
	 * @return returns XML byte contents
	 * @throws IOException throws exception when byte stream write fails
	 * @throws TransformerException throws exception when conversion fails
	 */
	public static byte[] getXMLContents(Document document) throws IOException, TransformerException {
		DOMSource source = new DOMSource(document);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		StreamResult result = new StreamResult(out);
		NDLXMLUtils.XML_TRANSFORMER.transform(source, result);
		out.close(); // closes the stream
		byte[] contents = out.toByteArray(); // gets byte array
		return contents;
	}
	
	/**
	 * Creates blank SIP by a given handle ID
	 * @param handle given handle ID
	 * @return creates blank SIP item
	 * @throws IOException throws exception when I/O related errors occur
	 * @throws SAXException throws exception when XML creation error occurs
	 */
	public static SIPDataItem createBlankSIP(String handle) throws IOException, SAXException {
		return createBlankSIP(null, handle);
	}
	
	/**
	 * Creates blank SIP by a given handle ID and prefix(folder/path location)
	 * @param prefix prefix (folder/path location)
	 * @param handle given handle ID
	 * @return creates blank SIP item
	 * @throws IOException throws exception when I/O related errors occur
	 * @throws SAXException throws exception when XML creation error occurs
	 */
	public static SIPDataItem createBlankSIP(String prefix, String handle)
			throws IOException, SAXException {
		return createBlankSIP(prefix, handle, false);
	}
	
	/**
	 * Creates blank SIP by a given handle ID and prefix(folder/path location)
	 * @param prefix prefix (folder/path location)
	 * @param handle given handle ID
	 * @param parent parent flag, whether item is parent or not
	 * @return creates blank SIP item
	 * @throws IOException throws exception when I/O related errors occur
	 * @throws SAXException throws exception when XML creation error occurs
	 */
	public static SIPDataItem createBlankSIP(String prefix, String handle, boolean parent)
			throws IOException, SAXException {
		Map<String, byte[]> contents = new HashMap<String, byte[]>(4);
		String id = handle.substring(handle.indexOf('/')+1);
		prefix = NDLDataUtils.NVL(prefix, id);
		contents.put(prefix + "/" + DUBLINECORE_FILE,
				new StringBuilder("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"no\"?>")
						.append("<dublin_core schema=\"dc\">").append("</dublin_core>").toString().getBytes());
		contents.put(prefix + "/" + LRMI_FILE,
				new StringBuilder("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"no\"?>")
						.append("<dublin_core schema=\"lrmi\">").append("</dublin_core>").toString().getBytes());
		contents.put(prefix + "/" + METADATA_NDL_FILE,
				new StringBuilder("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"no\"?>")
						.append("<dublin_core schema=\"ndl\">").append("</dublin_core>").toString().getBytes());
		contents.put(prefix + "/handle", handle.getBytes());
		contents.put(prefix + "/contents", "".getBytes());
		
		SIPDataItem item = new SIPDataItem();
		item.setParent(parent);
		item.load(contents);
		return item;
	}
	
	/**
	 * Creates blank SIP by a given handle ID and prefix(folder/path location)
	 * @param handle given handle ID
	 * @param parent parent flag, whether item is parent or not
	 * @return creates blank SIP item
	 * @throws IOException throws exception when I/O related errors occur
	 * @throws SAXException throws exception when XML creation error occurs
	 */
	public static SIPDataItem createBlankSIP(String handle, boolean parent) throws IOException, SAXException {
		return createBlankSIP(null, handle, parent);
	}
	
	/**
	 * Write NDL data items into target location
	 * @param contents NDL data item contents
	 * @param outLocation out location where to write
	 * @throws IOException throws exception if write fails
	 */
	public static void writeItems(Map<String, byte[]> contents, String outLocation) throws IOException {
		writeItems(contents, new File(outLocation));
	}
	
	/**
	 * Write NDL data items into target location
	 * @param contents NDL data item contents
	 * @param outLocation out location where to write
	 * @throws IOException throws exception if write fails
	 */
	public static void writeItems(Map<String, byte[]> contents, File outLocation) throws IOException {
		for(String key : contents.keySet()) {
			int p = key.lastIndexOf('/');
			String parent = key.substring(0, p);
			outLocation = NDLDataUtils.createFolder(new File(outLocation, parent)); // create folder if not exists
			String filename = key.substring(p+1);
			// write
			IOUtils.write(contents.get(key), new FileOutputStream(new File(outLocation, filename)));
		}
	}
	
	/**
	 * Gets compressed reader according to source input
	 * @param inputFile source input
	 * @return returns compatible reader, if found otherwise throws exception
	 * @throws IllegalStateException throws exception when incompatible compressed reader loaded
	 */
	public static CompressedDataReader getCompressedDataReader(File inputFile) {
		// TODO this should be done externally
		String fileName = inputFile.getName();
		if(fileName.endsWith(CompressedFileMode.TARGZ.getMode())) {
			// TAR GZ handler
			return new TarGZCompressedFileReader(inputFile);
		} else {
			// handle other cases
			throw new IllegalStateException("Incompatible file: " + fileName);
		}
	}
	
	/**
	 * Gets compressed data writer according to compression mode
	 * @param outLocation out location where to write file
	 * @param fileName file name to write
	 * @param mode compression mode, see {@link CompressedFileMode}
	 * @return returns compatible compressed data writer
	 * @throws IllegalStateException throws exception when incompatible compression mode provided
	 */
	public static CompressedDataWriter getCompressedDataWriter(File outLocation, String fileName,
			CompressedFileMode mode) {
		// TODO this should be done externally
		switch (mode) {
		case TARGZ:
			return new TarGZCompressedFileWriter(outLocation, fileName);
		default:
			// default case
			throw new IllegalStateException("Incompatible compression mode: " + mode.getMode());
		}
	}
	
	/**
	 * Gets compressed data writer according to compression mode
	 * @param outLocation out location where to write file
	 * @param fileName file name to write
	 * @param mode compression mode, see {@link CompressedFileMode}
	 * @return returns compatible compressed data writer
	 * @throws IllegalStateException throws exception when incompatible compression mode provided
	 */
	public static CompressedDataWriter getCompressedDataWriter(String outLocation, String fileName,
			CompressedFileMode mode) {
		return getCompressedDataWriter(new File(outLocation), fileName, mode);
	}
	
	/**
	 * Loads <b>data-set</b> from a text-file, text file has only single column
	 * @param file file name from where <b>data-set</b> to be loaded
	 * @return returns <b>data-set</b>
	 * @throws IOException throws exception if file I/O related error occurs
	 */
	public static Set<String> loadSet(String file) throws IOException {
		return loadSet(new File(file), null, false, false);
	}
	
	/**
	 * Loads <b>data-set</b> from a text-file, text file has only single column
	 * @param file file name from where <b>data-set</b> to be loaded
	 * @param transformer transforms the row if needed
	 * @return returns <b>data-set</b>
	 * @throws IOException throws exception if file I/O related error occurs
	 */
	public static Set<String> loadSet(String file, Transformer<String, String> transformer) throws IOException {
		return loadSet(new File(file), transformer, false, false);
	}
	
	/**
	 * Loads <b>data-set</b> from a text-file, text file has only single column
	 * @param file file name from where <b>data-set</b> to be loaded
	 * @return returns <b>data-set</b>
	 * @throws IOException throws exception if file I/O related error occurs
	 */
	public static Set<String> loadSet(File file) throws IOException {
		return loadSet(file, null, false, false);
	}
	
	/**
	 * Loads <b>data-set</b> from a text-file, text file has only single column
	 * @param file file name from where <b>data-set</b> to be loaded
	 * @param transformer line transformer, if line to transformed into multiple values
	 *        when it's is ON then ignoreCase parameter is ignored
	 * @return returns <b>data-set</b>
	 * @throws IOException throws exception if file I/O related error occurs
	 */
	public static Set<String> loadSet(File file, Transformer<String, String> transformer) throws IOException {
		return loadSet(file, transformer, false, false);
	}
	
	/**
	 * Loads <b>data-set</b> from a text-file, text file has only single column
	 * @param file file name from where <b>data-set</b> to be loaded
	 * @param ignoreCase flag whether to ignore case
	 * @return returns <b>data-set</b>
	 * @throws IOException throws exception if file I/O related error occurs
	 */
	public static Set<String> loadSet(String file, boolean ignoreCase) throws IOException {
		return loadSet(new File(file), null, ignoreCase, false);
	}
	
	/**
	 * Loads <b>data-set</b> from a text-file, text file has only single column
	 * @param file file name from where <b>data-set</b> to be loaded
	 * @param ignoreCase flag whether to ignore case
	 * @param splitTokens this flag indicates whether tokens split by space happens or not
	 * @return returns <b>data-set</b>
	 * @throws IOException throws exception if file I/O related error occurs
	 */
	public static Set<String> loadSet(String file, boolean ignoreCase, boolean splitTokens) throws IOException {
		return loadSet(new File(file), null, ignoreCase, splitTokens);
	}
	
	/**
	 * Loads <b>data-set</b> from a text-file, text file has only single column
	 * values are tokenized by space if exists 
	 * @param file file name from where <b>data-set</b> to be loaded
	 * @param transformer line transformer, if line to transformed into multiple values
	 *        when it's is ON then ignoreCase parameter is ignored
	 * @param ignoreCase flag whether to ignore case
	 * @param splitTokens this flag indicates whether tokens split by space happens or not
	 * @return returns <b>data-set</b>
	 * @throws IOException throws exception if file I/O related error occurs
	 */
	public static Set<String> loadSet(File file, Transformer<String, String> transformer, boolean ignoreCase,
			boolean splitTokens) throws IOException {
		Set<String> values = new HashSet<String>();
		List<String> lines = IOUtils.readLines(new FileInputStream(file), "UTF-8");
		for(String line : lines) {
			if(transformer != null) {
				values.addAll(transformer.transform(line.trim()));
			} else {
				if(splitTokens) {
					StringTokenizer tokens = new StringTokenizer(line);
					while(tokens.hasMoreTokens()) {
						String token = tokens.nextToken().trim();
						values.add(ignoreCase ? token.toLowerCase() : token);
					}
				} else {
					values.add(ignoreCase ? line.toLowerCase() : line);
				}
			}
		}
		return values;
	}
	
	/**
	 * Loads <b>data-map</b> from a CSV file, CSV file has at least two columns, first two columns used in mapping
	 * @param file file name from where <b>data-map</b> to be loaded
	 * @param ignoreCase flag whether to ignore case for key part
	 * @return returns <b>data-map</b>
	 * @throws IOException throws exception if file I/O related error occurs
	 */
	public static Map<String, String[]> loadMap(String file, boolean ignoreCase) throws IOException {
		return loadMap(new File(file), ignoreCase);
	}
	
	/**
	 * Loads <b>data-map</b> from a CSV file, CSV file has at least two columns, first two columns used in mapping
	 * @param file file name from where <b>data-map</b> to be loaded
	 * @param ignoreCase flag whether to ignore case for key part
	 * @return returns <b>data-map</b>
	 * @throws IOException throws exception if file I/O related error occurs
	 */
	public static Map<String, String[]> loadMap(File file, boolean ignoreCase) throws IOException {
		Map<String, String[]> map = new HashMap<String, String[]>();
		// assumed first column is PK
		CSVReader reader = readCSV(file, ',', '"', 1);
		String[] tokens = null;
		while((tokens = reader.readNext()) != null) {
			int l = tokens.length-1;
			String values[] = new String[l];
			for(int i=0; i<l; i++) {
				values[i] = tokens[i+1];
			}
			map.put(ignoreCase ? tokens[0].toLowerCase() : tokens[0], values);
		}
		reader.close();
		return map;
	}
	
	/**
	 * Loads <b>key-value</b> from a CSV file, CSV file has at least two columns, first two columns used in mapping
	 * @param file file name from where <b>key-value</b> to be loaded
	 * @param ignoreCase flag whether to ignore case for key part
	 * @return returns <b>key-value</b>
	 * @throws IOException throws exception if file I/O related error occurs
	 */
	public static Map<String, String> loadKeyValue(File file, boolean ignoreCase) throws IOException {
		Map<String, String> map = new LinkedHashMap<String, String>();
		// assumed first column is PK
		CSVReader reader = readCSV(file, ',', '"', 1);
		String[] tokens = null;
		while((tokens = reader.readNext()) != null) {
			map.put(ignoreCase ? tokens[0].toLowerCase() : tokens[0], tokens[1]);
		}
		reader.close();
		return map;
	}
	
	/**
	 * Loads <b>key-value</b> from a CSV file, CSV file has at least two columns, first two columns used in mapping
	 * @param file file name from where <b>key-value</b> to be loaded
	 * @param ignoreCase flag whether to ignore case for key part
	 * @return returns <b>key-value</b>
	 * @throws IOException throws exception if file I/O related error occurs
	 */
	public static Map<String, String> loadKeyValue(String file, boolean ignoreCase) throws IOException {
		return loadKeyValue(new File(file), ignoreCase);
	}
	
	/**
	 * Loads <b>key-value</b> from a CSV file, CSV file has at least two columns, first two columns used in mapping
	 * @param file file name from where <b>key-value</b> to be loaded
	 * @return returns <b>key-value</b>
	 * @throws IOException throws exception if file I/O related error occurs
	 */
	public static Map<String, String> loadKeyValue(File file) throws IOException {
		return loadKeyValue(file, false);
	}
	
	/**
	 * Loads <b>key-value</b> from a CSV file, CSV file has at least two columns, first two columns used in mapping
	 * <pre>It maintains key order.</pre>
	 * @param file file name from where <b>key-value</b> to be loaded
	 * @return returns <b>key-value</b>
	 * @throws IOException throws exception if file I/O related error occurs
	 */
	public static Map<String, String> loadKeyValue(String file) throws IOException {
		return loadKeyValue(new File(file), false);
	}
	
	/**
	 * Loads <b>data-map</b> from a CSV file, CSV file has at least two columns, first two columns used in mapping
	 * @param file file name from where <b>data-map</b> to be loaded
	 * @return returns <b>data-map</b>
	 * @throws IOException throws exception if file I/O related error occurs
	 */
	public static Map<String, String[]> loadMap(String file) throws IOException {
		return loadMap(new File(file), false);
	}
	
	/**
	 * Loads <b>data-map</b> from a CSV file, CSV file has at least two columns, first two columns used in mapping
	 * @param file file name from where <b>data-map</b> to be loaded
	 * @return returns <b>data-map</b>
	 * @throws IOException throws exception if file I/O related error occurs
	 */
	public static Map<String, String[]> loadMap(File file) throws IOException {
		return loadMap(file, false);
	}
	
	/**
	 * Text splitted into tokens and each token has initial capital
	 * @param text text to convert
	 * @param ignoreCase this flag determines whether other than first letter will be lower or not
	 * @return returns modified text
	 */
	public static String initCap(String text, boolean ignoreCase) {
		String splits[] = text.split("( | )+");
		StringBuilder modifiedValue = new StringBuilder();
		int l = splits.length;
		for(int i=0; i<l; i++) {
			String split = splits[i].trim();
			// init-cap but let others remain same
			if(StringUtils.isBlank(split)) {
				continue;
			}
			if(i == 0) {
				// first
				split = split.substring(0, 1).toUpperCase()
						+ (ignoreCase ? split.substring(1).toLowerCase() : split.substring(1));
			} else {
				// next
				if(ignoreCase) {
					split = split.toLowerCase();
				}
			}
			modifiedValue.append(split).append(" ");
		}
		if(modifiedValue.length() > 0) {
			return modifiedValue.deleteCharAt(modifiedValue.length() - 1).toString();
		} else {
			return modifiedValue.toString();
		}
	}
	
	/**
	 * Text splitted into tokens and each token has initial capital
	 * <pre>other than first letter will be lower</pre>
	 * @param text text to convert
	 * @return returns modified text
	 */
	public static String initCap(String text) {
		return initCap(text, true);
	}
	
	/**
	 * Joins a collection by joiner character, example: <b>[xxx, yyy]</b> list joins with dot(.) returns <b>xxx.yyy</b>
	 * @param values collection values
	 * @param joiner joiner character
	 * @return returns modified string
	 */
	public static <T> String join(Collection<T> values, char joiner) {
		return Joiner.on(joiner).skipNulls().join(values);
	}
	
	/**
	 * Joins a collection by joiner character, example: <b>[xxx, yyy]</b> list joins with dot(.) returns <b>xxx.yyy</b>
	 * @param values collection values
	 * @param joiner joiner string
	 * @return returns modified string
	 */
	public static <T> String join(Collection<T> values, String joiner) {
		return Joiner.on(joiner).skipNulls().join(values);
	}
	
	/**
	 * Joins an array by joiner character, example: <b>[xxx, yyy]</b> list joins with dot(.) returns <b>xxx.yyy</b>
	 * @param joiner joiner string
	 * @param values array values
	 * @return returns modified string
	 */
	public static <T> String join(String joiner, T ... values) {
		return Joiner.on(joiner).skipNulls().join(values);
	}
	
	/**
	 * Joins an array by joiner character, example: <b>[xxx, yyy]</b> list joins with dot(.) returns <b>xxx.yyy</b>
	 * @param joiner joiner character
	 * @param values array values
	 * @return returns modified string
	 */
	public static <T> String join(char joiner, T ... values) {
		return Joiner.on(joiner).skipNulls().join(values);
	}
	
	/**
	 * gets asset name by asset type
	 * @param assetType asset type
	 * @return returns asset name by asset type
	 */
	public static String getAssetMappingName(String assetType) {
		return NDL_ASSET_MAPPING.getProperty(assetType);
	}
	
	/**
	 * Adds asset to data item
	 * @param item item to add assets
	 * @param assetLocations asset location details
	 * @param assetID asset ID to find out the asset in asset locations
	 * @param defaultAssets default asset locations
	 * @return returns missing asset details
	 * @throws IOException throws exception if asset add fails
	 */
	public static List<String> addAsset(NDLDataItem item, Map<String, String> assetLocations, String assetID,
			Map<String, String> defaultAssets) throws IOException {
		List<String> missingAssets = new LinkedList<String>();
		NDLAssetType assetTypes[] = NDLAssetType.values(); // all asset values
		for(NDLAssetType assetType : assetTypes) {
			// each asset
			String type = assetType.getType();
			String location = assetLocations.get(type);
			if(StringUtils.isNotBlank(location)) {
				// location available
				String assetFileName = NDLDataUtils.getAssetMappingName(type); // file name
				int p = assetFileName.lastIndexOf('.');
				String assetExt = p != -1 ? assetFileName.substring(p) : ""; // file extension
				// location available
				File assetFile = new File(location, assetID + (StringUtils.isNotBlank(assetExt) ? assetExt : ""));
				boolean flag = false;
				if(assetFile.exists() && assetFile.isFile() && assetFile.length() != 0) {
					// exists, try to add asset
					// valid file, ignore invalid file
					if(NDLAssetFilterHandler.isIntialized()) {
						// if filter provided
						flag = NDLAssetFilterHandler.filter(new NDLAssetDetail(assetFile, assetType));
					} else {
						// normal case
						flag = true;
					}
				}
				if(flag) {
					// add asset
					item.addAsset(assetType, new FileInputStream(assetFile));
				} else {
					// missing case
					String defaultAsset = defaultAssets.get(type);
					if(defaultAsset != null) {
						// default asset mentioned
						assetFile = new File(defaultAsset);
						if(assetFile.exists() && assetFile.isFile() && assetFile.length() != 0) {
							// default/fall back asset
							// valid file, ignore invalid file
							item.addAsset(assetType, new FileInputStream(assetFile));
							missingAssets.add(assetType.getType() + ":" + DEFAULT_ASSET); // default asset track
						} else {
							// track missing details
							missingAssets.add(assetType.getType());
						}
					} else {
						// track missing details
						missingAssets.add(assetType.getType());
					}
				}
			}
		}
		return missingAssets;
	}
	
	/**
	 * Gets default CSV configuration
	 * @return returns default CSV configuration with comma and quote character
	 */
	public static CSVConfiguration getDefaultCSVConfiguration() {
		return new CSVConfiguration(DEFAULT_CSV_SEPARATOR, DEFAULT_CSV_QUOTE_CHARACTER);
	}
	
	/**
	 * Whether input is a valid month
	 * @param input given input
	 * @return return true if valid month
	 */
	public static boolean isMonth(String input) {
		if(input == null) {
			return false;
		}
		return MONTH_NAMES.containsKey(input.toLowerCase());
	}
	
	/**
	 * Normalizes date given tokens (at most 3)
	 * @param splitTokens at most 3 tokens
	 * @return returns normalized date
	 */
	public static String normalizeSimpleDate(List<String> splitTokens) {
		int l = splitTokens.size();
		if(l > 3) {
			// invalid arguments
			throw new IllegalArgumentException("At most 3 tokens should be provided.");
		}
		List<String> split = new ArrayList<String>(l);
		// copy original data
		for(String t : splitTokens) {
			split.add(t);
		}
		Iterator<String> tokens = split.iterator();
		int year = -1, month = -1, day = -1;
		// first pass
		while(tokens.hasNext()) {
			String token = tokens.next();
			if(token.length() == 4 && NumberUtils.isDigits(token)) {
				// year
				year = Integer.parseInt(token);
				tokens.remove();
			} else if(MONTH_NAMES.containsKey(token.toLowerCase())) {
				// month
				month = MONTH_NAMES.get(token.toLowerCase()).index;
				tokens.remove();
			}
		}
		if(year == -1) {
			// year should be found
			// invalid
			return null;
		}
		// second pass
		tokens = split.iterator();
		while(tokens.hasNext()) {
			String token = tokens.next();
			if(NumberUtils.isDigits(token)) {
				int i = Integer.parseInt(token);
				if(month == -1 && i < 13) {
					// month
					month = i;
				} else {
					day = i;
				}
			} else {
				// invalid
				return null;
			}
		}
		// cross-check
		if(l == 3) {
			if(year == -1 || month == -1 || day == -1) {
				// invalid
				return null;
			}
		}
		// default value of month and day
		if(month == -1) {
			month = 1;
		}
		if(day == -1) {
			day = 1;
		}
		if(month != -1) {
			if(month > 13) {
				// invalid month
				return null;
			}
			if(day != -1) {
				// days cross validation with month
				// others
				int required = (month == 2 && isLeapYear(year)) ? 29 : MONTH_DETAILS.get(month).days;
				if(day > required) {
					// invalid date
					return null;
				}
			}
		}
		// validation successful
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.YEAR, year);
		cal.set(Calendar.MONTH, month - 1); // 0 based
		cal.set(Calendar.DATE, day);
		return DateFormatUtils.format(cal.getTime(), NDL_DATE_FORMAT);
	}
	
	/**
	 * checks date format validity
	 * @param text text to check
	 * @return returns date format validity
	 */
	public static boolean isNDLValidDateFormat(String text) {
		return NDL_DATE_FORMAT_REGX.matches(text);
	}
	
	/**
	 * Normalizes date given a separator regular expression
	 * @param date given date
	 * @param separator given separator regular expression
	 * @return returns normalized date
	 */
	public static String normalizeSimpleDate(String date, String separator) {
		date = date.trim(); // normalize
		List<String> split = new LinkedList<String>();
		for(String t : date.split(separator)) {
			split.add(t);
		}
		String dt = normalizeSimpleDate(split);
		if(dt == null) {
			// invalid
			return date;
		}
		return dt;
	}
	
	/**
	 * Gets  ndl.sourceMeta.uniqueInfo JSON value
	 * @param key given key of JSON
	 * @param value corresponding value
	 * @return returns JSON
	 */
	public static String getUniqueInfoJSON(String key, String value) {
		UniqueInfoKeyValue kv = new UniqueInfoKeyValue();
		kv.setKey(key);
		kv.setValue(value);
		
		return NDLDataUtils.getJson(kv);
	}
	
	/**
	 * Creates new list
	 * @param values string values
	 * @return returns list
	 */
	public static List<String> createNewList(String ... values) {
		List<String> modified = new ArrayList<String>(values.length);
		for(String value : values) {
			modified.add(value);
		}
		return modified;
	}
	
	/**
	 * Creates empty string list
	 * @return returns empty string list
	 */
	public static List<String> createEmptyList() {
		return new LinkedList<String>();
	}
	
	/**
	 * Creates new set
	 * @param values string values
	 * @return returns set
	 */
	public static Set<String> createNewSet(String ... values) {
		Set<String> modified = new HashSet<String>(2);
		for(String value : values) {
			modified.add(value);
		}
		return modified;
	}
	
	/**
	 * Checks whether given text contains asset information
	 * @param text given text
	 * @param name asset name
	 * @param type asset type
	 * @return returns true if found otherwise false
	 */
	public static boolean containsAssetText(String text, String name, NDLAssetType type) {
		return text.contains(name + "\tbundle:" + type.getType());
	}
	
	/**
	 * Deletes asset information by given name and type
	 * @param text asset current information
	 * @param name asset name
	 * @param type asset type
	 * @return returns modified text
	 */
	public static String deleteAssetText(String text, String name, NDLAssetType type) {
		String t = name + "\tbundle:" + type.getType();
		StringTokenizer tokens = new StringTokenizer(text, NEW_LINE);
		StringBuilder modified = new StringBuilder();
		while(tokens.hasMoreTokens()) {
			String token = tokens.nextToken();
			if(!token.contains(t)) {
				// valid
				modified.append(token);
				modified.append(NEW_LINE);
			}
		}
		return modified.toString();
	}
	
	/**
	 * Extracts numbers from text for a given text
	 * @param text given text
	 * @return found numbers
	 */
	public static List<Long> extractNumbers(String text) {
		List<Long> numbers = new ArrayList<Long>(2);
		for(BigDecimal number : extractBigNumbers(text)) {
			numbers.add(number.longValue());
		}
		
		return numbers;
	}
	

	/**
	 * Extracts big numbers from text for a given text
	 * @param text given text
	 * @return found numbers
	 */
	public static List<BigDecimal> extractBigNumbers(String text) {
		if(StringUtils.isBlank(text)) {
			// invalid input
			return new LinkedList<BigDecimal>();
		}
		List<BigDecimal> numbers = new ArrayList<BigDecimal>(2);
		int l = text.length();
		StringBuilder t = new StringBuilder();
		for(int i = 0; i < l; i++) {
			char ch = text.charAt(i);
			if(CharUtils.isAsciiNumeric(ch)) {
				t.append(ch);
			} else {
				// break
				if(t.length() > 0) {
					numbers.add(new BigDecimal(t.toString()));
					t = new StringBuilder(); // reset
				}
			}
		}
		
		// last if any
		if(t.length() > 0) {
			numbers.add(new BigDecimal(t.toString()));
		}
		
		return numbers;
	}
	
	/**
	 * Gets file name and extension for a given file full name
	 * @param fileName given file full name
	 * @return returns 0 indexed file name and 1 indexed is extension
	 */
	public static String[] getFileNameAndExtension(String fileName) {
		int p = fileName.lastIndexOf('.');
		String details[] = new String[2];
		if(p != -1) {
			// extension
			details[0] = fileName.substring(0, p);
			details[1] = fileName.substring(p + 1);
		} else {
			details[0] = fileName;
		}
		return details;
	}
	
	/**
	 * Creates folder if not exists
	 * @param location folder location
	 * @return return created folder
	 */
	public static File createFolder(String location) {
		File folder = new File(location);
		if(!folder.exists()) {
			// make directory
			folder.mkdirs();
		}
		return folder;
	}
	
	/**
	 * Creates folder if not exists
	 * @param location folder location
	 * @return return created folder
	 */
	public static File createFolder(File location) {
		if(!location.exists()) {
			// make directory
			location.mkdirs();
		}
		return location;
	}
	
	/**
	 * Returns deserialized {@link IsPartOf} from given JSON
	 * @param json given JSON
	 * @return Returns deserialized {@link IsPartOf}
	 */
	public static IsPartOf deserializeIsPartOfJSON(String json) {
		if(StringUtils.isNotBlank(json)) {
			return HTML_ESCAPE_GSON.fromJson(json, IsPartOf.class);
		} else {
			return null;
		}
	}
	
	/**
	 * Returns deserialized {@link HasPart} from given JSON
	 * @param json given JSON
	 * @return Returns deserialized {@link HasPart}
	 */
	public static List<HasPart> deserializeHasPartJSON(String json) {
		if(StringUtils.isNotBlank(json)) {
			return HTML_ESCAPE_GSON.fromJson(json, HASPART_LIST_TYPE);
		} else {
			// blank list
			return new LinkedList<HasPart>();
		}
	}
	
	/**
	 * Returns json of {@link IsPartOf} from given object
	 * @param ispart given object
	 * @return Returns json
	 */
	public static String serializeIsPartOf(IsPartOf ispart) {
		return HTML_ESCAPE_GSON.toJson(ispart);
	}
	
	/**
	 * Returns json of {@link IsPartOf} from given object
	 * @param hasparts given object
	 * @return Returns json
	 */
	public static String serializeHasPart(List<HasPart> hasparts) {
		return HTML_ESCAPE_GSON.toJson(hasparts);
	}
	
	/**
	 * Returns value, if NULL then returns EMPTY
	 * @param value input value
	 * @return Returns value, if NULL then returns EMPTY
	 */
	public static String NVL(String value) {
		return value == null ? StringUtils.EMPTY : value;
	}
	
	/**
	 * Returns value, if NULL then returns given alternative value
	 * @param value input value
	 * @param altValue if input value is NULL then this is used
	 * @return Returns value, if NULL then returns given alternative value
	 */
	public static String NVL(String value, String altValue) {
		return value == null ? altValue : value;
	}
	
	/**
	 * Serializes language translation object into JSON
	 * @param detail detail to serialize
	 * @return returns language translation JSON
	 */
	public static String serializeLanguageTranslation(NDLLanguageTranslate detail) {
		if(!detail.getValues().isEmpty()) {
			return HTML_ESCAPE_GSON.toJson(detail);
		} else {
			return StringUtils.EMPTY;
		}
	}
	
	/**
	 * Deserializes language translation object from JSON
	 * @param json given json to deserialize
	 * @return returns desrialized object
	 */
	public static NDLLanguageTranslate desrializeLanguageTranslation(String json) {
		return HTML_ESCAPE_GSON.fromJson(json, NDLLanguageTranslate.class);
	}
	
	/**
	 * Determines whether a field to be deleted by given value
	 * <pre>if the values is 'delete' or blank then field gets deleted</pre>
	 * @param value given field value
	 * @return returns true if field to be deleted otherwise false
	 */
	public static boolean deleteField(String value) {
		return StringUtils.equalsIgnoreCase(value, FIELD_DELETE) || StringUtils.isBlank(value);
	}
	
	/**
	 * Splits full handle ID into prefix and suffix
	 * @param fullHandleID full handle id to split 
	 * @return returns split detail
	 */
	public static String[] splitHandleID(String fullHandleID) {
		StringTokenizer split = new StringTokenizer(fullHandleID, "/");
		if(split.countTokens() != 2) {
			// error
			throw new IllegalArgumentException("Invalid handle ID: " + fullHandleID);
		}
		return new String[]{split.nextToken(), split.nextToken()};
	}
	
	/**
	 * Returns handle ID suffix, <pre>for example: 123456789/xyz@1789 returns xyz@1789</pre>
	 * @param id handle id
	 * @return returns handle ID suffix
	 */
	public static String getHandleSuffixID(String id) {
		return id.substring(id.indexOf('/') + 1);
	}
	
	/**
	 * Returns handle ID prefix, <pre>for example: 123456789_pre/xyz@1789 returns 123456789_pre</pre>
	 * @param id handle id
	 * @return returns handle ID prefix
	 */
	public static String getHandlePrefixID(String id) {
		return id.substring(0, id.indexOf('/'));
	}
	
	/**
	 * Normalizes simple names
	 * @param name name to be normalized
	 * @param initCap flag whether initial letter to be capital
	 * @return returns normalized name
	 * @see #normalizeSimpleName(String, Set, Set, boolean)
	 */
	public static String normalizeSimpleName(String name, boolean initCap) {
		Set<String> t = NDLDataUtils.createNewSet();
		return normalizeSimpleName(name, t, t, initCap);
	}
	
	/**
	 * Normalizes simple names
	 * @param name name to be normalized
	 * @return returns normalized name
	 */
	public static String normalizeSimpleName(String name) {
		Set<String> t = NDLDataUtils.createNewSet();
		return normalizeSimpleName(name, t, t, true);
	}
	
	/**
	 * Normalizes simple names
	 * @param name name to be normalized
	 * @param wrongNames names to be discarded
	 * @param initCap flag whether initial letter to be capital
	 * @return returns normalized name
	 * @see #normalizeSimpleName(String, Set, Set, boolean)
	 */
	public static String normalizeSimpleNameByWrongNames(String name, Set<String> wrongNames, boolean initCap) {
		Set<String> t = NDLDataUtils.createNewSet();
		return normalizeSimpleName(name, t, wrongNames, initCap);
	}
	
	/**
	 * Normalizes simple names
	 * @param name name to be normalized
	 * @param wrongNames names to be discarded
	 * @return returns normalized name
	 */
	public static String normalizeSimpleNameByWrongNames(String name, Set<String> wrongNames) {
		Set<String> t = NDLDataUtils.createNewSet();
		return normalizeSimpleName(name, t, wrongNames, true);
	}
	
	/**
	 * Normalizes simple names
	 * @param name name to be normalized
	 * @param wrongNameTokens wrong token list
	 * @param initCap flag whether initial letter to be capital
	 * @return returns normalized name
	 * @see #normalizeSimpleNameByWrongNameTokens(String, Set)
	 * @see #normalizeSimpleNameByWrongNameTokens(String, Set, boolean)
	 */
	public static String normalizeSimpleNameByWrongNameTokens(String name, Set<String> wrongNameTokens,
			boolean initCap) {
		Set<String> t = NDLDataUtils.createNewSet();
		return normalizeSimpleName(name, wrongNameTokens, t, initCap);
	}
	
	/**
	 * Normalizes simple names
	 * @param name name to be normalized
	 * @param wrongNameTokens wrong token list
	 * @return returns normalized name
	 */
	public static String normalizeSimpleNameByWrongNameTokens(String name, Set<String> wrongNameTokens) {
		Set<String> t = NDLDataUtils.createNewSet();
		return normalizeSimpleName(name, wrongNameTokens, t, true);
	}
	
	/**
	 * It's required for name normalization, it checks whether input contains only initial letter
	 * @param input given input
	 * @return returns true if condition is satisfied otherwise false
	 */
	public static boolean isInitialLetter(String input) {
		String tokens[] = input.split(" +");
		for(String t : tokens) {
			if(t.replaceAll(",|\\.", "").length() > 1) {
				// no initials
				return false;
			}
		}
		return true;
	}
	
	// internal usage
	static boolean isAllowedSpecialNameChar(int ch) {
		return ch == 45 || ch == 46 || ch == 44 || ch == 39 || ch == 96;
	}
	
	/**
	 * It's required for name normalization, whether token contains special character
	 * @param token given token
	 * @return returns true if condition is satisfied otherwise false
	 */
	public static boolean isWrongNameToken(String token) {
		token = token.replaceFirst("(\\.|,)$", "");
		/*if(NumberUtils.isDigits(token)) {
			// only digits
			return true;
		}*/
		int l = token.length();
		if(l == 1 && isAllowedSpecialNameChar(token.charAt(0))) {
			// invalid if length is 1
			return true;
		}
		for(int i = 0; i < l; i++) {
			int ch = (int)token.charAt(i);
			if(isAllowedSpecialNameChar(ch)) {
				// valid character
				continue;
			}
			if((ch >= 33 && ch <= 47) || (ch >= 58 && ch <= 64) || (ch >= 92 && ch <= 96) || (ch >= 48 && ch <= 57)) {
				// supposed to be invalid character
				return true;
			}
		}
		return false; // no invalid character found
	}
	
	/**
	 * Normalizes simple names
	 * Assumptions: Wrong tokens are checked by case insensitivity
	 * @param name name to be normalized
	 * @param wrongNameTokens wrong token list
	 * @param wrongNames names to be discarded
	 * @param initCap flag whether initial letter to be capital
	 * @return returns normalized name
	 * @see #normalizeSimpleName(String)
	 */
	public static String normalizeSimpleName(String name, Set<String> wrongNameTokens, Set<String> wrongNames,
			boolean initCap) {
		if(wrongNames.contains(name) || StringUtils.isBlank(name) || isInitialLetter(name)) {
			// invalid name, empty string, only initials
			return StringUtils.EMPTY;
		}
		
		int maxTokens = 3;
		try {
			maxTokens = Integer.parseInt(NDL_NAME_NORMALIZATION_COFIGURATION.getProperty("max.name.tokens.length"));
		} catch(NumberFormatException ex) {
			// number format exception
			throw new DataNormalizationException("max.name.tokens.length: should be numeric.", ex);
		}
		name = name.trim();
		StringBuilder firstName = new StringBuilder();
		StringBuilder lastName = new StringBuilder();
		
		String tokens[] = name.split(",");
		if(tokens.length == 1) {
			List<String> list = getNameTokens(tokens[0], wrongNameTokens, initCap);
			if(list == null || list.size() > maxTokens) {
				// wrong name token found or max tokens limit exceed
				// let the name remain same
				return name;
			}
			int l = list.size();
			if(l == 0) {
				// invalid name
				return StringUtils.EMPTY;
			}
			String lt = list.get(l - 1).replaceFirst("\\.$", "");
			if(lt.length() > 1) {
				lastName.append(lt);
				for(int i = 0; i < l - 1; i++) {
					firstName.append(list.get(i)).append(" ");
				}
			} else {
				int i;
				for(i = 0; i < l; i++) {
					String t = list.get(i);
					if(t.replaceFirst("\\.$", "").length() == 1) {
						break;
					}
					lastName.append(t).append(" ");
				}
				for(int j = i; j < l; j++) {
					firstName.append(list.get(j)).append(" ");
				}
			}
		} else {
			List<String> list1 = getNameTokens(tokens[0], wrongNameTokens, initCap);
			List<String> list2 = getNameTokens(tokens[1], wrongNameTokens, initCap);
			if(list1 == null || list2 == null || (list1.size() + list2.size() > maxTokens)) {
				// wrong name token found or max tokens limit exceed
				// let the name remain same
				return name;
			}
			int l = list1.size();
			for(int i = 0; i < l; i++) {
				lastName.append(list1.get(i)).append(" ");
			}
			l = list2.size();
			for(int i = 0; i < l; i++) {
				firstName.append(list2.get(i)).append(" ");
			}
		}
		
		String lastNameText = lastName.toString().trim();
		String firstNameText = firstName.toString().trim();
		
		return lastNameText + (!firstNameText.isEmpty() ? ((!lastNameText.isEmpty() ? ", " : "") + firstNameText) : "");
	}
	
	// gets name tokens by text
	static List<String> getNameTokens(String text, Set<String> wrongNameTokens, boolean initCap) {
		String tokens[] = text.split("( | )+|\\.");
		int l = tokens.length;
		List<String> list = new ArrayList<String>(2);
		for(int i = 0; i < l; i++) {
			String token = tokens[i].replace(".", "");
			if(isWrongNameToken(token)) {
				// wrong name token then let the name remain same
				return null;
			}
			if (StringUtils.isBlank(token) || wrongNameTokens.contains(token.toLowerCase())) {
				// wrong tokens
				continue;
			}
			String t;
			if(initCap) {
				if(StringUtils.isAllUpperCase(token)) {
					t = NDLDataUtils.initCap(token);
				} else {
					t = NDLDataUtils.initCap(token, false);
				}
			} else {
				t = token;
			}
			list.add(t + (token.length() == 1 ? "." : ""));
		}
		return list;
	}
	
	/**
	 * Checks whether all values are blank
	 * @param values values to be checked
	 * @return returns true if all are blank, otherwise false
	 */
	public static boolean allBlank(String ... values) {
		for(String value : values) {
			if(StringUtils.isNotBlank(value)) {
				// all are not blank
				return false;
			}
		}
		return true; // all are blank
	}
	
	/**
	 * Checks whether all values are not blank
	 * @param values given values to be checked
	 * @return returns true if all are not blank otherwise false
	 */
	public static boolean allNotBlank(String ... values) {
		for(String value : values) {
			if(StringUtils.isBlank(value)) {
				// any is blank
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Checks whether at least one field is blank
	 * @param values values to be checked
	 * @return returns true if at least one field is blank, otherwise false
	 */
	public static boolean anyBlank(String ... values) {
		for(String value : values) {
			if(StringUtils.isBlank(value)) {
				// one field is blank
				return true;
			}
		}
		return false; // all not non-blank
	}
	
	/**
	 * Gets missing schema information display flag, default is false
	 * @return returns display flag
	 */
	public static boolean getMissingSchemaDisplayFlag() {
		return Boolean.valueOf(NDLConfigurationContext.getConfiguration("ndl.missing.schema.display.flag", "false"));
	}
	
	/**
	 * Gets Json parsing error information display flag, default is false
	 * @return returns display flag
	 */
	public static boolean getJsonParseErrorDisplayFlag() {
		return Boolean.valueOf(NDLConfigurationContext.getConfiguration("ndl.json.parse.error.display.flag", "false"));
	}
	
	/**
	 * Checks whether value partially belong to values
	 * <pre>brute force approach applied</pre>
	 * @param values values to check across
	 * @param value value to check with 'values'
	 * @param ignoreCase whether to check by ignore case or not
	 * @return returns true if partial matches found
	 */
	public static boolean partiallyBelongsTo(Collection<String> values, String value, boolean ignoreCase) {
		for(String val : values) {
			boolean f = ignoreCase ? StringUtils.containsIgnoreCase(value, val) : StringUtils.contains(value, val);
			if(f) {
				// match found
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Checks whether value partially belong to values
	 * <pre>brute force approach applied</pre>
	 * @param values values to check across
	 * @param value value to check with 'values'
	 * @return returns true if partial matches found
	 */
	public static boolean belongsTo(Collection<String> values, String value) {
		return partiallyBelongsTo(values, value, false); 
	}
	
	/**
	 * Checks whether value partially belong to values
	 * <pre>brute force approach applied</pre>
	 * @param values values to check across
	 * @param ignoreCase whether to check by ignore case or not
	 * @param tokens tokens to check with 'values'
	 * @return returns true if partial matches found
	 */
	public static boolean partiallyBelongsTo(Collection<String> values, boolean ignoreCase, String ... tokens) {
		for(String val : values) {
			for(String token : tokens) {
				boolean f = ignoreCase ? StringUtils.containsIgnoreCase(token, val) : StringUtils.contains(token, val);
				if(f) {
					// match found
					return true;
				}
			}
		}
		return false;
	}
	
	/**
	 * Checks whether value partially belong to values
	 * <pre>brute force approach applied</pre>
	 * @param values values to check across
	 * @param tokens tokens to check with 'values'
	 * @return returns true if partial matches found
	 */
	public static boolean partiallyBelongsTo(Collection<String> values, String ... tokens) {
		return partiallyBelongsTo(values, false, tokens);
	}
	
	/**
	 * Gets unique values by splitting
	 * @param value value to split
	 * @param multipleValueSeparator splitting by separator
	 * @return returns unique values
	 */
	public static Set<String> getUniqueValues(String value, char multipleValueSeparator) {
		StringTokenizer tokens = new StringTokenizer(value, String.valueOf(multipleValueSeparator));
		Set<String> values = new HashSet<String>(2);
		while(tokens.hasMoreTokens()) {
			values.add(tokens.nextToken());
		}
		return values;
	}
	
	/**
	 * Gets unique values by splitting
	 * @param value value to split
	 * @param regex splitting by separator
	 * @return returns unique values
	 */
	public static Set<String> getUniqueValues(String value, String regex) {
		String[] tokens = value.split(regex);
		Set<String> values = new HashSet<String>(2);
		for(String token : tokens) {
			values.add(token);
		}
		return values;
	}
	
	/**
	 * Removes multiple spaces and replace with given replace string
	 * @param input input to remove multiple spaces
	 * @param replace replace string
	 * @return returns modified string
	 */
	public static String removeMultipleSpaces(String input, String replace) {
		if(StringUtils.isBlank(input)) {
			// no change
			return input;
		}
		return input.replaceAll(SPACE_REGX, replace);
	}
	
	/**
	 * Removes multiple spaces
	 * @param input input to remove multiple spaces
	 * @return returns modified string
	 */
	public static String removeMultipleSpaces(String input) {
		return removeMultipleSpaces(input, " ");
	}
	
	/**
	 * Any space like character will be converted into 32 (normal space)
	 * @param input given input
	 * @return returns normalized input
	 */
	public static String normalizeSpace(String input) {
		return removeMultipleSpaces(input);
	}
	
	/**
	 * Removes new lines and replace with given replace string
	 * @param input input to remove new lines
	 * @param replace replace string
	 * @return returns modified string
	 */
	public static String removeNewLines(String input, String replace) {
		if(StringUtils.isBlank(input)) {
			// no change
			return input;
		}
		return input.replaceAll(NDLDataUtils.MULTILINE_REGX, replace);
	}
	
	/**
	 * Removes spaces/HTML/new lines etc.
	 * @param text given text
	 * @return returns normalized one
	 */
	public static String normalizeText(String text) {
		text = removeMultipleSpaces(text);
		text = removeNewLines(text);
		return removeHTMLTags(text);
	}
	
	/**
	 * Removes new lines and replace with space
	 * @param input input to remove new lines
	 * @return returns modified string
	 */
	public static String removeNewLines(String input) {
		return removeNewLines(input, " ");
	}
	
	/**
	 * Gets key and value from a given pair '&lt;key,value&gt;'
	 * @param pair given pair '&lt;key,value&gt;'
	 * @return returns key and value
	 */
	public static String[] getKeyValue(String pair) {
		pair = pair.trim();
		int l = pair.length();
		if(pair.charAt(0) == '<' && pair.charAt(l - 1) == '>') {
			String tokens[] = pair.substring(1, l - 1).split(",");
			if(tokens.length != 2) {
				// error
				throw new IllegalArgumentException(pair + "is expected in <key,value> form");
			} else {
				return new String[]{tokens[0].trim(), tokens[1].trim()};
			}
		} else {
			// error
			throw new IllegalArgumentException(pair + "is expected in <key,value> form");
		}
	}
	
	/**
	 * Checks invalid JSON key {@link #INVALID_JSON_KEY}
	 * @param key given key
	 * @return returns true if key is valid otherwise false
	 */
	public static boolean isInvalidJsonKey(String key) {
		return StringUtils.equals(key, INVALID_JSON_KEY);
	}
	
	/**
	 * Initial capital of words for a given input string, words separated by space
	 * @param input given input string
	 * @param lowercase words which will be in lower-case (ex: prepositions)
	 * <pre>these should be provided in lower-case</pre>
	 * @return returns modified string
	 */
	public static String wordInitCap(String input, String ... lowercase) {
		 return wordInitCap(input, Arrays.asList(lowercase));
	}
	
	/**
	 * Initial capital of words for a given input string, words separated by space
	 * @param input given input string
	 * @param lowercase words which will be in lower-case (ex: prepositions)
	 * <pre>these should be provided in lower-case</pre>
	 * @return returns modified string
	 */
	public static String wordInitCap(String input, Collection<String> lowercase) {
		input = input.toLowerCase();
		StringBuilder modified = new StringBuilder();
		String tokens[] = input.split(" +");
		for(String token : tokens) {
			if(lowercase.contains(token)) {
				// keep unchanged
				modified.append(token.toLowerCase());
			} else {
				modified.append(String.valueOf(token.charAt(0)).toUpperCase());
				modified.append(token.substring(1).toLowerCase());
			}
			modified.append(' ');
		}
		modified.deleteCharAt(modified.length() - 1);
		return modified.toString();
	}
	
	/**
	 * Splits an input string by regular expression and removes blank
	 * @param input given input
	 * @param regx given regular expression
	 * @return returns non-blank tokens
	 */
	public static String[] split(String input, String regx) {
		String tokens[] = input.split(regx);
		// remove blank tokens
		List<String> tl = new LinkedList<String>();
		for(String t : tokens) {
			if(StringUtils.isBlank(t)) {
				continue;
			}
			tl.add(t);
		}
		String newtokens[] = new String[tl.size()];
		return tl.toArray(newtokens);
	}
	
	/**
	 * Contains any of the given strings
	 * @param text given string to check
	 * @param ignoreCase whether case sensitivity to be checked
	 * @param check given strings to check with
	 * @return returns true if condition is true otherwise returns false
	 */
	public static boolean containsAny(String text, boolean ignoreCase, String ... check) {
		for(String c : check) {
			boolean f = ignoreCase ? StringUtils.containsIgnoreCase(text, c) : StringUtils.contains(text, c);
			if(f) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Contains any of the given strings
	 * @param text given string to check
	 * @param ignoreCase whether case sensitivity to be checked
	 * @param check given collection strings to check with
	 * @return returns true if condition is true otherwise returns false
	 */
	public static boolean containsAny(String text, boolean ignoreCase, Collection<String> check) {
		for(String c : check) {
			boolean f = ignoreCase ? StringUtils.containsIgnoreCase(text, c) : StringUtils.contains(text, c);
			if(f) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Removes HTML tag from given input text
	 * @param text given input text
	 * @return returns modified text
	 */
	public static String removeHTMLTags(String text) {
		if(StringUtils.isNotBlank(text)) {
			return Jsoup.parse(text).text();
		} else {
			return text;
		}
	}
	
	// byte array 2 hex string
	static String byte2hex(byte[] bytes) {
		StringBuilder checksum = new StringBuilder();
		int l = bytes.length;
		for(int i = 0; i < l; i++) {
			checksum.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
		}
		return checksum.toString();
	}
	
	/**
	 * Gets message digest checksum by given input and message digest algorithm
	 * @param input given input byte array
	 * @param algorithm digest algorithm
	 * @return returns message hash key, for details see {@link MessageDigest#update(byte)}
	 * @throws NoSuchAlgorithmException throws exception in case of invalid algorithm provided
	 */
	public static String messageDigest(byte[] input, String algorithm) throws NoSuchAlgorithmException {
		MessageDigest md = MessageDigest.getInstance(algorithm);
		md.update(input);
		return byte2hex(md.digest());
	}
	
	/**
	 * Gets message digest checksum by given input and message digest algorithm
	 * @param input given input stream
	 * @param algorithm digest algorithm
	 * @return returns message hash key, for details see {@link MessageDigest#update(byte)}
	 * @throws NoSuchAlgorithmException throws exception in case of invalid algorithm provided
	 * @throws IOException throws exception if input stream processing error occurs
	 */
	public static String messageDigest(InputStream input, String algorithm)
			throws IOException, NoSuchAlgorithmException {
		return messageDigest(IOUtils.toByteArray(input), algorithm);
	}
	
	/**
	 * Gets message digest checksum by given input and message digest algorithm
	 * @param input given input file
	 * @param algorithm digest algorithm
	 * @return returns message hash key, for details see {@link MessageDigest#update(byte)}
	 * @throws IOException throws exception if input stream processing error occurs
	 * @throws NoSuchAlgorithmException throws exception in case of invalid algorithm provided
	 */
	public static String messageDigest(File input, String algorithm) throws IOException, NoSuchAlgorithmException {
		return messageDigest(FileUtils.readFileToByteArray(input), algorithm);
	}
	
	/**
	 * gets PDF page count
	 * @param pdf PDF file
	 * @return returns page count
	 * @throws FileNotFoundException throws error if file not found
	 * @throws IOException throws error if I/O related error occurs
	 */
	public static int getPDFPageCount(File pdf) throws FileNotFoundException, IOException {
		PdfReader pdfreader = new PdfReader(new FileInputStream(pdf));
		return pdfreader.getNumberOfPages();
	}
	
	/**
	 * gets PDF page count
	 * @param pdf PDF file contents
	 * @return returns page count
	 * @throws IOException throws error if I/O related error occurs
	 */
	public static int getPDFPageCount(byte[] pdf) throws IOException {
		PdfReader pdfreader = new PdfReader(pdf);
		return pdfreader.getNumberOfPages();
	}
	
	/**
	 * gets PDF page count
	 * @param pdf PDF file input stream
	 * @return returns page count
	 * @throws IOException throws error if I/O related error occurs
	 */
	public static int getPDFPageCount(InputStream pdf) throws IOException {
		PdfReader pdfreader = new PdfReader(pdf);
		return pdfreader.getNumberOfPages();
	}
	
	/**
	 * Validates NDL data against a given validator
	 * @param data data to validate
	 * @param validator given validator
	 * @return returns false if validation fails otherwise true
	 * @throws Exception throws error if logging fails
	 */
	public static boolean validateNDLData(NDLDataItem data, AbstractNDLDataValidationBox validator) throws Exception {
		if(validator == null) {
			// no validation required
			return true;
		}
		boolean f = true;
		Map<String, Collection<String>> values = data.getAllValues();
		// for each fields
		for(String field : values.keySet()) {
			Collection<String> value = values.get(field);
			boolean flag = validator.validate(field, value, data.getId());
			if(!flag) {
				// validation fails
				f = false;
			}
		}
		
		// TODO handle mandate fields
		
		return f;
	}
	
	/**
	 * Gets PT format for a given time
	 * @param time given time (YYY:MM:DD:HH24:MM:SS)
	 * @return returns format as PxxYyyMzzDTxxHyyMzzS
	 */
	public static String formatAsPT(String time) {
		String tokens[] = time.split(":");
		int l = tokens.length;
		int c = 6;
		LinkedList<String> list = new LinkedList<String>();
		boolean f = false;
		for(int i = l - 1; i >= 0; i--) {
			list.addFirst(Integer.parseInt(tokens[i]) + String.valueOf(PT_MAPPING_SYMBOLS[--c]));
			if(c == 3) {
				f = true;
				list.addFirst(String.valueOf('T'));
			}
		}
		if(!f) {
			list.addFirst(String.valueOf('T'));
		}
		list.addFirst(String.valueOf('P'));
		StringBuilder format = new StringBuilder();
		for(String v : list) {
			format.append(v);
		}
		return format.toString();
	}
	
	/**
	 * Returns true if given input is a roman number
	 * @param input given input
	 * @return Returns true if given input is a roman number otherwise false
	 */
	public static boolean isRoman(String input) {
		Matcher m = ROMAN_NUMBER_REGEX.matcher(input);
		return m.matches();
	}
	
	// for internal usage
	static int romanValue(char r) {
		return ROMAN_VALUE_MAPPING.get(r);
	}
	
	/**
	 * Converts a given roman number into decimal
	 * @param input given roman number
	 * @return converted decimal number
	 * @throws IllegalArgumentException this exception is thrown if input is not a roman number
	 */
	public static int romanToDecimal(String input) {
	    int res = 0; 
	  
	    input = input.toLowerCase();
	    // given input
	    int l = input.length();
	    for (int i = 0; i < l; i++) { 
	        int s1 = romanValue(input.charAt(i)); 	  
	        if (i+1 < l) { 
	            int s2 = romanValue(input.charAt(i + 1)); 
	            // Comparing both values 
	            if (s1 >= s2) { 
	                // Value of current symbol is greater 
	                // or equal to the next symbol 
	                res = res + s1; 
	            }  else {
	            	// Value of current symbol is 
                    // less than the next symbol
	                res = res + s2 - s1; 
	                i++; 
	            } 
	        } else { 
	            res = res + s1; 
	            i++; 
	        } 
	    } 
	    return res;
	}
	
	/**
	 * Words to decimal for a given input
	 * @param input given input
	 * @return returns words
	 */
	public static long wordsToDecimal(String input) {
		
		if(StringUtils.isBlank(input)) {
			// error
			throw new IllegalArgumentException(input + " is blank.");
		}
		
		long n = 0;
		
		input = input.toLowerCase().replaceAll("-", " ").replaceAll(" and", " ");
		String[] tokens = input.trim().split("\\s+");
		long rt = 0;
		for(String t : tokens) {
			if(!WORDS2NUMBER_MAPPING.containsKey(t)) {
				// error
				throw new IllegalArgumentException(t + " is an invalid number.");
			}
			// calculate
			NDLDataPair<String> dp = WORDS2NUMBER_MAPPING.get(t);
			char op = dp.second().charAt(0);
			long f = Long.valueOf(dp.first());
			if(op == '+') {
				// add
				rt += f;
			} else {
				// mul
				rt *= f;
				if(!StringUtils.equals(t, "hundred")) {
					// special case
					n += rt;
					rt = 0;
				}
			}
		}
		n += rt;
		
		return n;
	}
	
	/**
	 * whether words describe decimal or not for given input
	 * @param input given input
	 * @return returns true if so otherwise false
	 */
	public static boolean isDecimalWords(String input) {
		for(String t : input.split(" +")) {
			if(!WORDS2NUMBER_MAPPING.containsKey(input.toLowerCase())) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Gets parent folder for a given path
	 * @param folder given folder/path
	 * @return returns parent if exists otherwise blank
	 */
	public static String getParentFolder(String folder) {
		String t;
		if(folder.endsWith("/")) {
			t = folder.substring(0, folder.length() - 1);
		} else {
			t = folder;
		}
		int p = t.lastIndexOf('/');
		if(p != -1) {
			return t.substring(0, p);
		} else {
			// not parent exists
			return "";
		}
	}
	
	/**
	 * Removes HTML tag and tries to fix latex simple sup/sub tags
	 * <pre>Note: If complex situation occurs then it fails to do conversion and returns original data</pre>
	 * @param input given input
	 * @return returns modified text
	 */
	public static String removeHTMLTagsAndFixLatex(String input) {
		
		if(!StringUtils.containsIgnoreCase(input, "<sup>") && !StringUtils.containsIgnoreCase(input, "<sub>")) {
			// invalid text tried to fix
			return removeHTMLTags(input);
		}
		
		String original = input;
		// processing removal of spaces in between sup sub tags
		input = input.replaceAll("(?i)<sup>", "__SUPS__").replaceAll("(?i)</sup>", "__SUPE__")
				.replaceAll("(?i)<sub>", "__SUBS__").replaceAll("(?i)</sub>", "__SUBE__").replaceAll("&", "&amp;")
				.replaceAll("<", "&lt;").replaceAll(">", "&gt;");
		input = input.replaceAll("__SUPS__", "<sup>").replaceAll("__SUPE__", "</sup>").replaceAll("__SUBS__", "<sub>")
				.replaceAll("__SUBE__", "</sub>");
		SAXPHandler4SimpleSupSubLatex h = new SAXPHandler4SimpleSupSubLatex();
		try {
			SAXP.parse(new ByteArrayInputStream(("<ROOT>" + input + "</ROOT>").getBytes()), h);
		} catch(Exception ex) {
			// suppress error
			return removeHTMLTags(original); // returns original string
		}
		input = h.mtext.toString(); // modified text
		
		// next processing
		StringBuilder t = new StringBuilder();
		String tokens[] = input.split(" +");
		for(String token : tokens) {
			if(token.contains("<sup>") || token.contains("<sub>")) {
				// normal SUP SUB handle
				token = token.replaceAll("(?i)<sup>", "^{").replaceAll("(?i)</sup>", "}").replaceAll("(?i)<sub>", "_{")
						.replaceAll("(?i)</sub>", "}");
				t.append('$').append(token).append('$');
			} else {
				// normal token
				t.append(token);
			}
			if(t.charAt(t.length() - 1) != ' ') {
				// last character is not space
				t.append(' ');
			}
		}
		
		// remove HTML tags if any
		return removeHTMLTags(t.toString());
	}
	
	/**
	 * gets comparable object by given text and type
	 * @param text given text
	 * @param type given type
	 * @return returns comparable object
	 */
	public static Comparable getByDataType(String text, DataType type) {
		switch (type) {
		case INTEGER:
			return Integer.valueOf(text);
		case LONG:
			return Long.valueOf(text);
		case REAL:
			return Double.valueOf(text);
		case TEXT:
			return text;
		default:
			throw new UnsupportedOperationException(type + " not defined");
		}
	}
	
	/**
	 * validation of handle ID
	 * @param handle handle id to validate
	 * @return returns handle validity
	 */
	public static boolean validateHandle(String handle) {
		return HANDLE_PATTERN.matcher(handle).matches();
	}
	
	/**
	 * normalize as handle ID (removes special characters and replace with '_')
	 * @param text text to normalize
	 * @param retainHandleIDCharacters retain specific characters for handle ID
	 * @return returns normalized as handle ID
	 */
	public static String normalizeAsHandleID(String text, Set<Character> retainHandleIDCharacters) {
		StringBuilder handle = new StringBuilder();
		int l = text.length();
		for(int i = 0; i < l; i++) {
			char ch = text.charAt(i);
			if(CharUtils.isAsciiAlpha(ch) || CharUtils.isAsciiNumeric(ch) || retainHandleIDCharacters.contains(ch)) {
				// allowable
				handle.append(ch);
			} else {
				// special character replace with '_'
				handle.append('_');
			}
		}
		
		return handle.toString();
	}
	
	/**
	 * normalize as handle ID (removes special characters and replace with '_')
	 * @param text text to normalize
	 * @return returns normalized as handle ID
	 */
	public static String normalizeAsHandleID(String text) {
		Set<Character> retainHandleIDCharacters = new HashSet<>(2);
		retainHandleIDCharacters.add('_'); // default retain underscore in handle ID
		
		return normalizeAsHandleID(text, retainHandleIDCharacters);
	}
	
	/**
	 * Splits by space and take initial letters(if alphanumeric but skip for numeric) for given text
	 * @param text given text
	 * @return return abbreviated text
	 */
	public static String splitByInitialLetter(String text) {
		// split by punctuation
		String tokens[] = text.split("( |:|,|-|_|;)+");
		
		StringBuilder mtext = new StringBuilder();
		for(String t : tokens) {
			if(NumberUtils.isDigits(t)) {
				// skip for number
				mtext.append(t).append('_');
			} else {
				mtext.append(t.charAt(0)).append('_');
			}
		}
		
		return mtext.toString();
	}
	
	
	/**
	 * removes invalid characters for a given text and invalid character set
	 * @param text given text
	 * @param invalid given invalid character set
	 * @return returns modified text
	 */
	public static String removeInvalidCharacters(String text, int ... invalid) {
		Set<Integer> inv = new HashSet<>(2);
		for(int i : invalid) {
			inv.add(i);
		}
		return removeInvalidCharacters(text, inv);
	}
	
	/**
	 * removes invalid characters for a given text and invalid character set
	 * @param text given text
	 * @param invalid given invalid character set
	 * @return returns modified text
	 */
	public static String removeInvalidCharacters(String text, Set<Integer> invalid) {
		if(StringUtils.isBlank(text)) {
			// blank handle
			return text;
		}
		StringBuilder mt = new StringBuilder();
		int l = text.length();
		for(int i = 0; i < l; i++) {
			int ch = (int)text.charAt(i);
			if(!invalid.contains(ch)) {
				mt.append((char)ch);
			}
		}
		return mt.toString();
	}
	
	/**
	 * Gets file name by logical file (source name) name.
	 * @param sourceName logical name or source name
	 * @param filename returned file name is prefixed with logical name (if provided)
	 * @param dateOnly only date flag tells whether to consider time-stamp
	 * @param dateFormatter date formatter
	 * @param dateOnlyFormatter date only formatter
	 * @return returns resultant file name
	 */
	public static String getSourceFullFileName(String sourceName, String filename, SimpleDateFormat dateFormatter,
			SimpleDateFormat dateOnlyFormatter, boolean dateOnly) {
		// formatters
		SimpleDateFormat df = dateFormatter == null ?  DATE_FORMATTER : dateFormatter;
		SimpleDateFormat dof = dateFormatter == null ?  DATE_ONLY_FORMATTER : dateOnlyFormatter;
		
		if(StringUtils.isNotBlank(sourceName)) {
			return (dateOnly ? dof.format(new Date()) : df.format(new Date())) + "." + sourceName
					+ (StringUtils.isNotBlank(filename) ? ("." + filename) : "");
		} else {
			return DATE_FORMATTER.format(new Date()) + (StringUtils.isNotBlank(filename) ? ("." + filename) : "");
		}
	}
	
	/**
	 * Gets file name by logical file (source name) name.
	 * @param sourceName logical name or source name
	 * @param filename returned file name is prefixed with logical name (if provided)
	 * @param dateOnly only date flag tells whether to consider time-stamp
	 * @return returns resultant file name
	 */
	public static String getSourceFullFileName(String sourceName, String filename, boolean dateOnly) {
		return getSourceFullFileName(sourceName, filename, null, null, dateOnly);
	}
	
	/**
	 * Converts text to from-character-set to to-character-set
	 * @param text given text
	 * @param fromCharset from-character-set
	 * @param toCharset to-character-set
	 * @return returns modified string
	 */
	public static String convertText(String text, String fromCharset, String toCharset) {
		// TODO write explanation how it works
		Charset fc = Charset.forName(fromCharset);
		Charset tc = Charset.forName(toCharset);

		ByteBuffer inputBuffer = ByteBuffer.wrap(text.getBytes(fc));
		// decode
		CharBuffer data = tc.decode(inputBuffer);
		// encode
		ByteBuffer outputBuffer = fc.encode(data);
		byte[] outputData = outputBuffer.array();
		
		// modified string
		return new String(outputData, fc);
	}
	
	/**
	 * removes control characters from given text
	 * @param text given text
	 */
	public static String removeControlCharaters(String text) {
		int l = text.length();
		StringBuilder modified = new StringBuilder();
		for(int i = 0; i < l; i++) {
			char ch = text.charAt(i);
			if(!CharUtils.isAsciiControl(ch)) {
				// valid character
				modified.append(ch);
			}
		}
		
		return modified.toString();
	}
	
	/**
	 * gets all original asset details for a given SIP item
	 * @param sip given SIP item
	 * @return returns all original asset details if found otherwise empty list
	 * @throws IOException throws exception in case of processing error
	 */
	public static List<AssetDetail> getOriginalAssets4SIP(SIPDataItem sip) throws IOException {
		List<AssetDetail> found = new ArrayList<>(2);
		
		List<AssetDetail> assets = sip.readAllAssets();
		for(AssetDetail asset : assets) {
			if(asset.getType() == NDLAssetType.ORIGINAL) {
				found.add(asset);
			}
		}
		return found;
	}
}