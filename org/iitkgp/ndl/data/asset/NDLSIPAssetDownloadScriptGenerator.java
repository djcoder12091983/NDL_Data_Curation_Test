package org.iitkgp.ndl.data.asset;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.context.NDLConfigurationContext;
import org.iitkgp.ndl.data.Filter;
import org.iitkgp.ndl.data.NDLAssetType;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.container.NDLSIPDataContainer;
import org.iitkgp.ndl.util.NDLDataUtils;

/**
 * NDL asset download script file (UNIX shell script) generator
 * @author Debasis
 */
public class NDLSIPAssetDownloadScriptGenerator extends NDLSIPDataContainer {
	
	// asset url link field
	static String ASSET_LINK_FIELD_NAME = "ndl.sourceMeta.additionalInfo";
	
	// asset key details by which URL can be extracted
	Map<String, String> assetKeys = new HashMap<String, String>(2);
	Map<String, Long> assetCounter = new HashMap<String, Long>(2);
	String baseLocation;
	String urlPrefix;
	Filter<String> urlFilter;
	long skipped = 0;
	
	/**
	 * Constructor
	 * @param input input source
	 * @param logLocation log location to generate validation logs
	 * @param name logical data source name, by which logging file(s) etc. prefixed
	 */
	public NDLSIPAssetDownloadScriptGenerator(String input, String logLocation, String name) {
		super(input, logLocation, name);
		baseLocation = "/";
	}
	
	/**
	 * This is the download base location which will be used to download assets
	 * @param baseLocation base location, default is '/'
	 */
	public void setBaseLocation(String baseLocation) {
		this.baseLocation = baseLocation;
	}
	
	/**
	 * Adds key field for a given asset type
	 * @param type asset type
	 * @param key associated <pre>ndl.sourceMeta.additionalInfo</pre>:key from which URL can be extracted
	 * @throws IOException throws exception if addition fails
	 */
	public void addAssetKey(NDLAssetType type, String key) throws IOException {
		String t = type.getType();
		assetKeys.put(t, ASSET_LINK_FIELD_NAME + ':' + key);
		// add loggers
		addTextLogger(t);
	}
	
	/**
	 * Adds key field for a given asset type
	 * @param type asset type
	 * @param field associated field from which URL can be extracted
	 * @throws IOException throws exception if addition fails
	 */
	public void addAssetField(NDLAssetType type, String field) throws IOException {
		String t = type.getType();
		assetKeys.put(t, field);
		// add loggers
		addTextLogger(t);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean readItem(SIPDataItem item) throws Exception {
		for(String key : assetKeys.keySet()) {
			String url = item.getSingleValue(assetKeys.get(key));
			if(StringUtils.isNotBlank(url)) {
				// filter logic
				if(urlFilter != null) {
					if(!urlFilter.filter(url)) {
						// no need to include
						skipped++;
						continue;
					}
				}
				String ext;
				int idx = url.lastIndexOf('.');
				// extension
				if(idx != -1) {
					ext = url.substring(idx);
					// skip ?
					idx = ext.indexOf('?');
					if(idx != -1) {
						// remove ?
						ext = ext.substring(0, idx);
					}
				} else {
					ext = "";
				}
				String downloadLink = "wget \"" + (StringUtils.isNotBlank(urlPrefix) ? urlPrefix : "") + url + "\" -O \""
						+ new File(baseLocation, key + "/" + NDLDataUtils.getHandleSuffixID(item.getId()) + ext)
								.getAbsolutePath() + "\"";
				log(key, downloadLink); // write
				// asset counter
				Long c = assetCounter.get(key);
				if(c == null) {
					assetCounter.put(key, 1L);
				} else {
					assetCounter.put(key, c.longValue() + 1);
				}
			}
		}
		return true;
	}
	
	@Override
	public void postProcessData() throws Exception {
		// super call
		super.postProcessData();
		for(String key : assetCounter.keySet()) {
			System.out.println(key + " found: " + assetCounter.get(key) + " Skipped: " + skipped);
		}
	}
	
	/**
	 * Sets URL prefix if any
	 * @param urlPrefix url prefix to set
	 */
	public void setUrlPrefix(String urlPrefix) {
		this.urlPrefix = urlPrefix;
	}
	
	/**
	 * Sets URL filter if any
	 * @param urlFilter URL filter
	 */
	public void setUrlFilter(Filter<String> urlFilter) {
		this.urlFilter = urlFilter;
	}
	
	// test
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/zenodo/2019.May.15.15.01.47.SLM.data.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/zenodo/logs";
		String name = "sl.asset";

		//NDLConfigurationContext.addConfiguration("compressed.data.process.buffer.size", "10");
		NDLConfigurationContext.addConfiguration("text.log.write.line.threshold.limit", "100000");
		
		NDLSIPAssetDownloadScriptGenerator g = new NDLSIPAssetDownloadScriptGenerator(input, logLocation, name);
		g.setBaseLocation("/home/tilak/Downloads/assets/");
		g.addAssetKey(NDLAssetType.THUMBNAIL, "thumbnail");
		g.setUrlPrefix("https://archives.lib.state.ma.us");
		g.setUrlFilter(new Filter<String>() {
			// URL filter
			@Override
			public boolean filter(String data) {
				boolean skip = data.endsWith("images/mimeicons/plain.png") || data.endsWith("images/mimeicons/html.png")
						|| data.endsWith("images/mimeicons/mime.png") || data.endsWith("images/mimeicons/pdf.png")
						|| data.endsWith("images/mimeicons/vnd.ms-excel.png")
						|| data.endsWith("images/mimeicons/vnd.ms-powerpoint.png")
						|| data.endsWith("images/mimeicons/msword.png");
				return !skip;
			}
		});
		g.processData();
		
		System.out.println("Done.");
	}
}