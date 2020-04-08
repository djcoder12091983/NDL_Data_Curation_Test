package org.iitkgp.ndl.util;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.data.duplicate.checker.DuplicateDocumentsOutput;
import org.iitkgp.ndl.service.exception.DDCNormalizationException;
import org.iitkgp.ndl.service.exception.DateNormalizationException;
import org.iitkgp.ndl.service.exception.LanguageNormalizationException;
import org.iitkgp.ndl.service.exception.ServiceRequestException;
import org.iitkgp.ndl.service.exception.TextNormalizationException;
import org.iitkgp.ndl.service.exception.URLNormalizationException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * NDL service utilities, for more details see API.
 * @author Debasis
 */
public class NDLServiceUtils {
	
	public static String BASE_SERVICE_URL;
	public static String BASE_SERVICE_URL1;
	static String CLASS_HIERARCHY_URL;
	static String ACCESS_RIGHTS_URL;
	static String NORMALIZE_DATE_URL;
	static String NORMALIZE_LANGUAGE_URL;
	static String NORMALIZE_TEXT_URL;
	static String NORMALIZE_URL_URL;
	static String DUPLICATE_CHECKER_URL;
	
	/**
	 * This data comparator used to sort DDC text
	 */
	public static Comparator<String> DDC_COMAPRATOR = new Comparator<String>() {
		/**
		 * comparison logic
		 */
		@Override
		public int compare(String ddc1, String ddc2) {
			// ordering based on prefix numeric value
			int p1 = ddc1.indexOf(':');
			if(p1 == -1) {
				p1 = ddc1.length();
			}
			int p2 = ddc2.indexOf(':');
			if(p2 == -1) {
				p2 = ddc2.length();
			}
			return Integer.valueOf(ddc1.substring(0, p2)).compareTo(Integer.valueOf(ddc2.substring(0, p2)));
		}
	};
	
	static {
		// context startup
		NDLConfigurationContext.init();
		BASE_SERVICE_URL = NDLConfigurationContext.getConfiguration("ndl.service.base.url");
		BASE_SERVICE_URL1 = NDLConfigurationContext.getConfiguration("ndl.service.base.url1"); // another server
		CLASS_HIERARCHY_URL = BASE_SERVICE_URL
				+ NDLConfigurationContext.getConfiguration("ndl.service.classhierarchy.url");
		ACCESS_RIGHTS_URL = BASE_SERVICE_URL
				+ NDLConfigurationContext.getConfiguration("ndl.service.accessRights.url");
		NORMALIZE_DATE_URL = BASE_SERVICE_URL
				+ NDLConfigurationContext.getConfiguration("ndl.service.date.normalization.url");
		NORMALIZE_LANGUAGE_URL = BASE_SERVICE_URL
				+ NDLConfigurationContext.getConfiguration("ndl.service.language.normalization.url");
		NORMALIZE_TEXT_URL = BASE_SERVICE_URL
				+ NDLConfigurationContext.getConfiguration("ndl.service.text.normalization.url");
		NORMALIZE_URL_URL = BASE_SERVICE_URL
				+ NDLConfigurationContext.getConfiguration("ndl.service.url.normalization.url");
		DUPLICATE_CHECKER_URL = BASE_SERVICE_URL1
				+ NDLConfigurationContext.getConfiguration("ndl.service.duplicate.checker.url");
	}
	
	/**
	 * Language normalization service by single language code
	 * @param language language to be normalized 
	 * @return returns returns normalized language, NULL if invalid language code is provided
	 * @throws LanguageNormalizationException throws error when service request could not made
	 * @throws ParseException throws error when response parsing error occurs 
	 */
	public static String normalilzeLanguage(String language) throws LanguageNormalizationException, ParseException {
		List<String> languages = normalilzeLanguage(Arrays.asList(language));
		if(languages.isEmpty()) {
			// conversion failed
			return null;
		} else {
			return languages.get(0);
		}
	}
	
	/**
	 * Language normalization service by multiple languages
	 * @param languages language set to be normalized 
	 * @return returns returns normalized languages, NULL if invalid language code is provided
	 * @throws LanguageNormalizationException throws error when service request could not made
	 * @throws ParseException throws error when response parsing error occurs 
	 */
	public static List<String> normalilzeLanguage(Collection<String> languages)
			throws LanguageNormalizationException, ParseException {
		Map<String, Collection<String>> params = new HashMap<String, Collection<String>>();
		params.put("codes[]", languages);
		// response processing
		String response = null;
		try {
			response = request(NORMALIZE_LANGUAGE_URL, "POST", params);
		} catch(Exception ex) {
			ex.printStackTrace(System.err); // print error details
			// error
			throw new LanguageNormalizationException(ex.getMessage(), ex.getCause());
		}
		List<String> normalizedLanguages = new LinkedList<String>();
		JSONArray data = getArrayDataFromJsonResponse(response, "dc.language.iso");
		if(data != null) {
			// success
			for(Object d : data) {
				if(d != null) {
					normalizedLanguages.add(d.toString());
				}
			}
		}
		return normalizedLanguages;
	}
	
	/**
	 * Date normalization service by single date
	 * @param date date to be normalized 
	 * @return returns returns normalized date, NULL if invalid date is provided
	 * @throws LanguageNormalizationException throws error when service request could not made
	 * @throws ParseException throws error when response parsing error occurs 
	 */
	public static String normalilzeDate(String date) throws DateNormalizationException, ParseException {
		List<String> dates = normalilzeDate(Arrays.asList(date));
		if(dates.isEmpty()) {
			// conversion failed
			return null;
		} else {
			return dates.get(0);
		}
	}
	
	/**
	 * Language normalization service by multiple dates
	 * @param dates date set to be normalized 
	 * @return returns returns normalized dates, NULL if invalid date is provided
	 * @throws LanguageNormalizationException throws error when service request could not made
	 * @throws ParseException throws error when response parsing error occurs 
	 */
	public static List<String> normalilzeDate(Collection<String> dates)
			throws DateNormalizationException, ParseException {
		Map<String, Collection<String>> params = new HashMap<String, Collection<String>>();
		params.put("format", Arrays.asList("auto"));
		params.put("dates[]", dates);
		// response processing
		String response = null;
		try {
			response = request(NORMALIZE_DATE_URL, "POST", params);
		} catch(Exception ex) {
			// error
			ex.printStackTrace(System.out);
			throw new DateNormalizationException(dates + ": " + ex.getMessage(), ex.getCause());
		}
		List<String> newdates = new LinkedList<String>();
		JSONArray data = getArrayDataFromJsonResponse(response, "dates");
		if(data != null) {
			// success
			for(Object d : data) {
				if(d != null) {
					newdates.add(d.toString());
				}
			}
		}
		return newdates;
	}
	
	/**
	 * DDC normalization service by multiple DDC codes
	 * @param codes DDC code set to be normalized 
	 * @return returns normalized DDC codes, NULL if invalid DDC code is provided
	 * @throws DDCNormalizationException throws error when DDC normalization error occurs
	 * @throws ParseException throws error when response parsing error occurs 
	 */
	public static Set<String> normalizeDDCAndSort(String[] codes) throws DDCNormalizationException, ParseException {
		List<String> codeList = Arrays.asList(codes);
		return normalizeDDCAndSort(codeList);
	}
	
	/**
	 * DDC normalization service by multiple DDC codes
	 * @param codes DDC code set to be normalized 
	 * @return returns normalized DDC codes, NULL if invalid DDC code is provided
	 * @throws DDCNormalizationException throws error when DDC normalization error occurs
	 * @throws ParseException throws error when response parsing error occurs 
	 */
	public static Set<String> normalizeDDCAndSort(Collection<String> codes) throws DDCNormalizationException, ParseException {
		Set<String> sortedcodes = new TreeSet<String>(DDC_COMAPRATOR);
		sortedcodes.addAll(getClassHierarchy(codes, "ddc"));
		return sortedcodes;
	}
	
	/**
	 * DDC normalization service by multiple DDC codes
	 * @param codes DDC code set to be normalized 
	 * @return returns normalized DDC codes, NULL if invalid DDC code is provided
	 * @throws DDCNormalizationException throws error when DDC normalization error occurs
	 * @throws ParseException throws error when response parsing error occurs 
	 */
	public static Set<String> normalizeDDC(String[] codes) throws DDCNormalizationException, ParseException {
		List<String> codeList = Arrays.asList(codes);
		return normalizeDDC(codeList);
	}
	
	/**
	 * DDC normalization service by multiple DDC codes
	 * @param codes DDC code set to be normalized 
	 * @return returns normalized DDC codes, NULL if invalid DDC code is provided
	 * @throws DDCNormalizationException throws error when DDC normalization error occurs
	 * @throws ParseException throws error when response parsing error occurs 
	 */
	public static Set<String> normalizeDDC(Collection<String> codes) throws DDCNormalizationException, ParseException {
		return getClassHierarchy(codes, "ddc");
	}
	
	/**
	 * DDC normalization service by multiple DDC codes
	 * @param codes DDC code set to be normalized, as code only (no text) 
	 * @return returns normalized DDC codes, NULL if invalid DDC code is provided
	 * @throws DDCNormalizationException throws error when DDC normalization error occurs
	 * @throws ParseException throws error when response parsing error occurs 
	 */
	public static Set<Long> normalizeDDC2Codes(Collection<String> codes) throws DDCNormalizationException, ParseException {
		Set<String> normalizedCodes =  getClassHierarchy(codes, "ddc");
		Set<Long> codesList = new HashSet<>(2);
		for(String c : normalizedCodes) {
			// codes only (no text)
			codesList.add(Long.valueOf(c.substring(0, c.indexOf(':'))));
		}
		
		return codesList;
	}
	
	/**
	 * DDC normalization service by single DDC code
	 * @param code DDC code to be normalized 
	 * @return returns normalized DDC, NULL if invalid DDC code is provided
	 * @throws DDCNormalizationException throws error when DDC normalization error occurs
	 * @throws ParseException throws error when response parsing error occurs 
	 */
	public static Set<String> normalizeDDC(String code) throws DDCNormalizationException, ParseException {
		List<String> codes = new LinkedList<String>();
		codes.add(code); // single code
		return getClassHierarchy(codes, "ddc");
	}
	
	/**
	 * DDC normalization service by single DDC code
	 * @param code DDC code to be normalized 
	 * @return returns normalized DDC as code only (no text), NULL if invalid DDC code is provided
	 * @throws DDCNormalizationException throws error when DDC normalization error occurs
	 * @throws ParseException throws error when response parsing error occurs 
	 */
	public static Set<String> normalizeDDC2Codes(String code) throws DDCNormalizationException, ParseException {
		List<String> codes = new LinkedList<String>();
		codes.add(code); // single code
		Set<String> normalizedCodes = getClassHierarchy(codes, "ddc");
		Set<String> codesList = new TreeSet<>(DDC_COMAPRATOR); // ascending order
		for(String c : normalizedCodes) {
			// codes only (no text)
			codesList.add(c.substring(0, c.indexOf(':')));
		}
		
		return codesList;
	}
	
	/**
	 * Normalizes text, removes unwanted characters
	 * @param text given text to normalize
	 * @param configuration parameter configuration to normalize in a custom way
	 * @return returns normalized version, if fails returns null
	 * @throws TextNormalizationException throws error if normalization fails
	 * @throws ParseException throws exception if response parsing error happens
	 */
	public static String normalizeText(String text, NDLTextNormalizationConfiguration configuration)
			throws TextNormalizationException, ParseException {
		String normalized[] = normalizeText(new String[]{text}, configuration);
		return normalized[0];
	}
	
	/**
	 * Normalizes text, removes unwanted characters
	 * @param texts given texts to normalize
	 * @param configuration parameter configuration to normalize in a custom way
	 * @return returns normalized version, if fails returns null
	 * @throws TextNormalizationException throws error if normalization fails
	 * @throws ParseException throws exception if response parsing error happens
	 * @see #normalizeText(String, NDLTextNormalizationConfiguration)
	 */
	public static String[] normalizeText(Collection<String> texts, NDLTextNormalizationConfiguration configuration)
			throws TextNormalizationException, ParseException {
		String input[] = new String[texts.size()];
		return normalizeText(texts.toArray(input), configuration);
	}
	
	/**
	 * Normalizes text, removes unwanted characters
	 * @param url given url to normalize
	 * @return returns normalized version, if fails returns null
	 * @throws TextNormalizationException throws error if normalization fails
	 * @throws ParseException throws exception if response parsing error happens
	 */
	public static String normalizeURL(String url)
			throws TextNormalizationException, ParseException {
		String normalized[] = normalizeURL(new String[]{url});
		return normalized[0];
	}
	
	/**
	 * Normalizes text, removes unwanted characters
	 * @param urls given urls to normalize
	 * @return returns normalized version, if fails returns null
	 * @throws TextNormalizationException throws error if normalization fails
	 * @throws ParseException throws exception if response parsing error happens
	 * @see #normalizeURL(String)
	 */
	public static String[] normalizeURL(Collection<String> urls) throws URLNormalizationException, ParseException {
		String input[] = new String[urls.size()];
		return normalizeURL(urls.toArray(input));
	}
	
	/**
	 * Normalizes text, removes unwanted characters
	 * @param urls given urls to normalize
	 * @return returns normalized version, if fails returns null
	 * @throws TextNormalizationException throws error if normalization fails
	 * @throws ParseException throws exception if response parsing error happens
	 * @see #normalizeURL(String)
	 */
	public static String[] normalizeURL(String urls[]) throws URLNormalizationException, ParseException {
		if(urls == null) {
			// invalid URL
			return null;
		}
		
		// params
		Map<String, Collection<String>> params = new HashMap<String, Collection<String>>();
		params.put("url[]", Arrays.asList(urls));
		// response
		String response = null;
		try {
			response = request(NORMALIZE_URL_URL, "POST", params);
		} catch(ServiceRequestException ex) {
			// error
			throw new TextNormalizationException(ex.getMessage(), ex.getCause());
		}
		JSONArray normalizedUrls = getArrayDataFromJsonResponse(response, "url");
		if(normalizedUrls != null) {
			// response available
			String result[] = new String[urls.length];
			int c = 0;
			for(Object t : normalizedUrls) {
				if(t != null) {
					result[c++] = t.toString();
				} else {
					result[c++] = null;
				}
			}
			return result;
		} else {
			return null;
		}
	}
	
	/**
	 * Normalizes text, removes unwanted characters
	 * @param texts given texts to normalize
	 * @param configuration parameter configuration to normalize in a custom way
	 * @return returns normalized version, if fails returns null
	 * @throws TextNormalizationException throws error if normalization fails
	 * @throws ParseException throws exception if response parsing error happens
	 * @see #normalizeText(String, NDLTextNormalizationConfiguration)
	 */
	public static String[] normalizeText(String texts[], NDLTextNormalizationConfiguration configuration)
			throws TextNormalizationException, ParseException {
		if(texts == null) {
			// invalid text
			return null;
		}
		
		// params
		Map<String, Collection<String>> params = new HashMap<String, Collection<String>>();
		params.put("text[]", Arrays.asList(texts));
		// default params
		configuration.loadParameters(params);
		// response
		String response = null;
		try {
			response = request(NORMALIZE_TEXT_URL, "POST", params);
		} catch(ServiceRequestException ex) {
			// error
			throw new TextNormalizationException(ex.getMessage(), ex.getCause());
		}
		JSONArray normalizedTexts = getArrayDataFromJsonResponse(response, "text");
		if(normalizedTexts != null) {
			// response available
			String result[] = new String[texts.length];
			int c = 0;
			for(Object t : normalizedTexts) {
				if(t != null) {
					result[c++] = t.toString();
				} else {
					result[c++] = null;
				}
			}
			return result;
		} else {
			return null;
		}
	}
	
	/**
	 * Gets access rights detail by given URL and URL type
	 * @param url given URL
	 * @param urlType URL type
	 * @return returns access rights detail
	 * @throws ParseException throws error if response couldn't be parsed
	 */
	public static NDLAccessRightsDetail getAccessRightsDetail(String url, NDLURLType urlType) throws ParseException {
		if(StringUtils.isBlank(url)) {
			// invalid URL
			return null;
		}
		// params
		Map<String, Collection<String>> params = new HashMap<String, Collection<String>>();
		params.put("url", Arrays.asList(url));
		params.put("ui", Arrays.asList(urlType.getType()));
		// response
		String response = null;
		response = request(ACCESS_RIGHTS_URL, "POST", params);
		JSONObject r = getJsonResponse(response);
		if(r != null) {
			// result
			NDLAccessRightsDetail detail = new NDLAccessRightsDetail(r.get("dc.rights.accessRights").toString());
			detail.setNumFiles(Integer.parseInt(r.get("numFiles").toString()));
			detail.setStatusCode(Integer.parseInt(r.get("statusCode").toString()));
			return detail;
		} else {
			// error
			return new NDLAccessRightsDetail(null);
		}
	}
	
	/**
	 * Gets access rights by given URL and URL type
	 * @param url given URL
	 * @param urlType URL type
	 * @return returns access rights
	 * @throws ParseException throws error if response couldn't be parsed
	 */
	public static String getAccessRights(String url, NDLURLType urlType) throws ParseException {
		return getAccessRightsDetail(url, urlType).getAccessRights();
	}
	
	/**
	 * Class hierarchy service by multiple codes (DDC/Mesh etc.) and type
	 * @param codes codes set to be normalized 
	 * @param type type is required for code type- mesh, DDC etc.
	 * @return returns returns normalized codes, NULL if invalid code is provided
	 * @throws LanguageNormalizationException throws error when service request could not made
	 * @throws ParseException throws error when response parsing error occurs 
	 */
	static Set<String> getClassHierarchy(Collection<String> codes, String type) throws DDCNormalizationException, ParseException {
		if(codes.isEmpty()) {
			// invalid codes
			return new HashSet<String>(2);
		}
		List<String> newCodes = new LinkedList<String>();
		for(String c : codes) {
			String newCode = StringUtils.leftPad(c, 3, '0');
			newCodes.add(newCode);
		}
		// params
		Map<String, Collection<String>> params = new HashMap<String, Collection<String>>();
		params.put("type", Arrays.asList(type));
		params.put("codes[]", newCodes);
		// response
		String response = null;
		try {
			response = request(CLASS_HIERARCHY_URL, "POST", params);
		} catch(ServiceRequestException ex) {
			// error
			throw new DDCNormalizationException(ex.getMessage(), ex.getCause());
		}
		return parseHierarchy(response);
	}
	
	// JSON response array data from NDL service request 
	static JSONArray getArrayDataFromJsonResponse(String response, String key) throws ParseException {
		JSONObject r = getJsonResponse(response);
		if(r != null) {
			// success
			return (JSONArray)r.get(key);
		} else {
			// error
			return null;
		}
	}
	
	// JSON response data from NDL service request
	static JSONObject getDataFromJsonResponse(String response, String key) throws ParseException {
		JSONObject r = getJsonResponse(response);
		if(r != null) {
			// success
			return (JSONObject)r.get(key);
		} else {
			// error
			return null;
		}
	}
	
	// gets request service JSON response
	static JSONObject getJsonResponse(String response) throws ParseException {
		// TODO object mapping should be done using generic API
		JSONParser parser = new JSONParser();
		JSONObject json = (JSONObject)parser.parse(response);
		JSONArray errorDetails = ((JSONArray)json.get("errors"));
		if(errorDetails.isEmpty()) {
			// success
			return (JSONObject)json.get("response");
		} else {
			// error
			return null;
		}
	}
	
	// parse JSON response obtained from NDL request
	static Set<String> parseHierarchy(String response) throws ParseException {
		Set<String> codeDetails = new HashSet<String>();
		JSONArray data = getArrayDataFromJsonResponse(response, "dc.subject.ddc");
		if(data != null) {
			// success
			for(Object d : data) {
				String ddc = d.toString();
				codeDetails.add(ddc);
			}
		}
		return codeDetails;
	}
	
	// duplicate check query input
	static class DuplicateCheckerInput {
		String type;
		Collection<String> values;
		
		public DuplicateCheckerInput(String type, Collection<String> values) {
			this.type = type;
			this.values = values;
		}
		
		public DuplicateCheckerInput(String type, int size) {
			this.type = type;
			this.values = new ArrayList<String>(size);
		}
		
		// adds value
		public void add(String value) {
			values.add(value);
		}
	}
	
	/**
	 * Checks duplicate by type(DOI etc.)
	 * @param type type (DOI etc.)
	 * @param values values to check for duplicates
	 * @return returns duplicate documents detail
	 * @throws ParseException throws error in case of exception
	 */
	public static DuplicateDocumentsOutput duplicateChecker(String type, Collection<String> values) throws ParseException {
		DuplicateCheckerInput qinput = new DuplicateCheckerInput(type, values);
		String joutput = getJsonResponse(request(DUPLICATE_CHECKER_URL, "POST", qinput)).toJSONString();
		return NDLDataUtils.HTML_ESCAPE_GSON.fromJson(joutput, DuplicateDocumentsOutput.class);
	}
	
	/**
	 * Makes a web-request and gets JSON response
	 * @param baseURL Base URL to make web-request
	 * @param method web-request HTTP method
	 * @param parameters web-request parameters
	 * @return returns JSON response text
	 * @throws ServiceRequestException throws exception when request fails to respond
	 */
	public static String request(String baseURL, String method, Map<String, Collection<String>> parameters)
			throws ServiceRequestException {
		boolean error = false;
		StringBuilder response = new StringBuilder();
		try {
			// query string
			Iterator<String> keys = parameters.keySet().iterator();
			StringBuilder query  = new StringBuilder();
			while(keys.hasNext()) {
				// remaining parameters
				String key = keys.next();
				Collection<String> values = parameters.get(key);
				for(String value : values) {
					query.append(key).append("=").append(URLEncoder.encode(value, "UTF-8")).append("&");
				}
			}
			// remove last &
			query.deleteCharAt(query.length()-1);
			
			error = processRequest(baseURL, null, method, query, response);
			
		} catch(Exception ex) {
			ex.printStackTrace(System.err); // print errors detail
			// error
			boolean ioerror = ex instanceof IOException;
			throw new ServiceRequestException(baseURL,
					(ioerror ? ("Failed to connect: " + baseURL + " Error: ") : "") + ex.getMessage(), ex.getCause());
		}
		
		if(error) {
			// print error message
			throw new ServiceRequestException(baseURL, "[ERROR] " + response.toString());
		}
		
		// return response
		return response.toString();
	}
	
	/**
	 * Makes a web-request with JSON input and gets JSON response
	 * @param <Q> This Q describes the type of parameter details
	 * @param baseURL Base URL to make web-request
	 * @param method web-request HTTP method
	 * @param parameters web-request parameters
	 * @return returns JSON response text
	 * @throws ServiceRequestException throws exception when request fails to respond
	 */
	public static <Q> String request(String baseURL, String method, Q parameters)
			throws ServiceRequestException {
		boolean error = false;
		StringBuilder response = new StringBuilder();
		try {
			// JSON query string
			StringBuilder query  = new StringBuilder(NDLDataUtils.HTML_ESCAPE_GSON.toJson(parameters));
			Map<String, String> headers = new HashMap<String, String>(2);
			headers.put("content-type", "application/json");
			error = processRequest(baseURL, headers, method, query, response);
		} catch(Exception ex) {
			// error
			boolean ioerror = ex instanceof IOException;
			throw new ServiceRequestException(baseURL,
					(ioerror ? ("Failed to connect: " + baseURL + " Error: ") : "") + ex.getMessage(), ex.getCause());
		}
		
		if(error) {
			// print error message
			throw new ServiceRequestException(baseURL, "[ERROR] " + response.toString());
		}
		
		// return response
		return response.toString();
	}
	
	// process request (TEXT output)
	static boolean processRequest(String baseURL, Map<String, String> headers, String method, StringBuilder query,
			StringBuilder response) throws Exception {
		boolean error = false;
		boolean get = method.equals("GET");
		boolean post = method.equals("POST");
		String finalURL = null;
		if(get) {
			// GET
			finalURL = baseURL + "?" + query.toString();
		} else if(post) {
			// POST
			finalURL = baseURL;
		}
		URL url = new URL(finalURL);
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setRequestMethod(method);
		connection.setDoInput(true);
		connection.setDoOutput(true);
		byte[] postData = null;
		if(post) {
			// POST
			postData = query.toString().getBytes();
			connection.setRequestProperty("Content-Length", String.valueOf(postData.length));
		}
		// extra headers if any
		if(headers != null) {
			for(String h : headers.keySet()) {
				connection.setRequestProperty(h, headers.get(h));
			}
		}
		if(post) {
			DataOutputStream writer = new DataOutputStream(connection.getOutputStream());
			writer.write(postData);
			writer.flush();
			writer.close();
		}
		
		// response
		int responseCode = connection.getResponseCode();
		BufferedReader input = null;
		if(responseCode == 200) {
			// success
			input = new BufferedReader(new InputStreamReader(connection.getInputStream()));
		} else {
			// error
			input = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
			error = true;
		}
		String inputLine;
		while((inputLine = input.readLine()) != null) {
			response.append(inputLine);
		}
		input.close();
		// connection disconnects
		connection.disconnect();
		
		return error;
	}
}