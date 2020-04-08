package org.iitkgp.ndl.test.source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.iitkgp.ndl.core.NDLDataNode;
import org.iitkgp.ndl.data.SIPDataItem;
import org.iitkgp.ndl.data.correction.NDLSIPCorrectionContainer;
import org.iitkgp.ndl.data.normalizer.NDLDataNormalizer;
import org.iitkgp.ndl.util.NDLDataUtils;

public class OAAuthorCurationV1 extends NDLSIPCorrectionContainer {
	
	static List<String> ORGANIZATION_TOKENS = new ArrayList<String>();
	
	static {
		ORGANIZATION_TOKENS.add("study");
		ORGANIZATION_TOKENS.add("collaborat");
		ORGANIZATION_TOKENS.add("research");
		ORGANIZATION_TOKENS.add("group");
		ORGANIZATION_TOKENS.add("laboratory");
		ORGANIZATION_TOKENS.add("project");
		ORGANIZATION_TOKENS.add("editor");
		ORGANIZATION_TOKENS.add("consortium");
		ORGANIZATION_TOKENS.add("committee");
		ORGANIZATION_TOKENS.add("product");
		ORGANIZATION_TOKENS.add("authority");
		ORGANIZATION_TOKENS.add("staff");
		ORGANIZATION_TOKENS.add("society");
		ORGANIZATION_TOKENS.add("office");
		ORGANIZATION_TOKENS.add("department");
	}
	
	Set<String> delete;
	Set<String> wrongTokens;
	Map<String, String> authorCorrectionMap;

	public OAAuthorCurationV1(String input, String logLocation, String outputLocation, String name) {
		super(input, logLocation, outputLocation, name);
	}

	@Override
	protected boolean correctTargetItem(SIPDataItem target) throws Exception {
		
		// delete case
		deleteIfContains("dc.contributor.editor", delete, true);
		
		List<NDLDataNode> nodes = target.getNodes("dc.contributor.editor");
		// organization move
		for(NDLDataNode node : nodes) {
			String v = node.getTextContent();
			boolean d = false;
			if(StringUtils.startsWithIgnoreCase(v, "the ") || StringUtils.startsWithIgnoreCase(v, "national ")
					|| StringUtils.startsWithIgnoreCase(v, "on behalf of ")) {
				v = v.replaceFirst("^((for|and|(on behalf of)) (T|t)he)", "");
				target.add("dc.contributor.other:organization", v);
				d = true;
			} else {
				for(String org : ORGANIZATION_TOKENS) {
					if(StringUtils.containsIgnoreCase(v, org)) {
						v = v.replaceFirst("^((for|and|(on behalf of)) (T|t)he)", "");
						target.add("dc.contributor.other:organization", v);
						d = true;
						break;
					}
				}
			}
			if(d) {
				// move case
				node.remove(); // remove node
			}
		}
		
		// merge tokens
		merge(target);
		
		// correction
		nodes = target.getNodes("dc.contributor.editor");
		List<String> newnames = new LinkedList<>();
		for(NDLDataNode node : nodes) {
			String v = node.getTextContent();
			//System.out.println("T: " + v);
			//log("temp_test", target.getId() + " = > " + v);
			if(authorCorrectionMap.containsKey(v)) {
				node.remove();
				// add new names
				newnames.add(authorCorrectionMap.get(v));
			}
		}
		for(String newname : newnames) {
			target.add("dc.contributor.editor", newname.split(";"));
		}
		
		/*if(target.getId().equals("oalib/2149016")) {
			System.out.println("TEST: " + target.getValue("dc.contributor.editor"));
		}*/
		
		// final normalization
		normalize("dc.contributor.editor");
		
		//System.out.println("A1: " + target.getId() + " => " + NDLDataUtils.join(target.getValue("dc.contributor.editor"), '|'));
		
		// delete case
		deleteIfContains("dc.contributor.editor", delete, true);
		
		// again merge
		merge(target);
		
		//System.out.println("A: " + target.getId() + " => " + NDLDataUtils.join(target.getValue("dc.contributor.editor"), '|'));
		
		return true;
	}
	
	void merge(SIPDataItem target) {
		// merge tokens
		List<NDLDataNode> nodes = target.getNodes("dc.contributor.editor");
		NDLDataNode pnode = null;
		for(NDLDataNode node : nodes) {
			// special token remove
			// System.out.println("Value: " + node.getTextContent());
			String v = node.getTextContent().replaceFirst(" et al\\.?$", "").replaceFirst("^Dr\\.", "").replaceAll("\\(.+\\)", "").trim();
			node.setTextContent(v);
			if(v.equals(",")) {
				// wrong value
				node.remove();
				pnode = null;
				continue;
			}
			if(v.matches("^[A-Za-z]$") && v.length() <= 2) {
				// merge case
				if(pnode != null) {
					pnode.setTextContent(pnode.getTextContent() + " " + v);
				}
				// remove and reset
				node.remove();
				pnode = null;
				continue;
			}
			if(v.contains(".")) {
				String v1 = v.replace(".", "");
				//System.out.println("T: " + v1);
				if(v1.matches("^[A-Za-z ]+$") && v1.length() <= 5) {
					// merge case
					if(pnode != null) {
						StringBuilder t = new StringBuilder();
						String t1[] = v.split("\\.|( +)");
						for(String t11 : t1) {
							if(StringUtils.isNotBlank(t11) && !StringUtils.equals(t11, "-")) {
								t.append(t11).append(t11.length() == 1 ? "." : "").append(" ");
							}
						}
						pnode.setTextContent(pnode.getTextContent() + " " + t.toString().trim());
					}
					// remove and reset
					node.remove();
					pnode = null;
					continue;
				}
			}
			pnode = node;
		}
	}
	
	// correct
	public static void main(String[] args) throws Exception {
		String input = "/home/dspace/debasis/NDL/NDL_sources/OA/input/2019.Jul.11.10.55.38.OpenAccess.V6.Corrected.tar.gz";
		String logLocation = "/home/dspace/debasis/NDL/NDL_sources/OA/logs";
		String outputLocation = "/home/dspace/debasis/NDL/NDL_sources/OA/out";
		String name = "oa.author.v1";
		
		String deleteFile = "/home/dspace/debasis/NDL/NDL_sources/OA/conf/delete";
		String wrongTokensFile = "/home/dspace/debasis/NDL/NDL_sources/OA/conf/wrong.tokens";
		String authorCorrectionFile = "/home/dspace/debasis/NDL/NDL_sources/OA/conf/Author.correction.csv";
		
		OAAuthorCurationV1 p = new OAAuthorCurationV1(input, logLocation, outputLocation, name);
		p.delete = NDLDataUtils.loadSet(deleteFile, true);
		p.wrongTokens = NDLDataUtils.loadSet(wrongTokensFile, true);
		p.authorCorrectionMap = NDLDataUtils.loadKeyValue(authorCorrectionFile);
		p.turnOffLoadHierarchyFlag();
		p.addNormalizer("dc.contributor.editor", new NDLDataNormalizer() {
			
			@Override
			public Collection<String> transform(String input) {
				//System.out.println("Input: " + input);
				input = input.trim().replaceAll("^((AND)|(and)|(Asst)|(Reviewed by))|((AND)|(and))$", "");
				String tokens[] = input.split(" +[0-9]?(and|AND|And|Dr|&|MSc|PhD|MD|Prof|Professor)\\.? +");
				List<String> names = new LinkedList<String>();
				for(String token : tokens) {
					String tokens1[] = token.split("( |\\.)+");
					StringBuilder mtokens1 = new StringBuilder();
					int l = tokens1.length;
					for(int i = 0; i < l; i++) {
						String t1 = tokens1[i];
						if(StringUtils.isBlank(t1) || StringUtils.equals(t1, "-")) {
							continue;
						}
						//t1 = t1.replaceAll("[^A-Za-z,-]", "");
						t1 = t1.replaceAll("#|\\*|[0-9]|\\(|\\)|:", ""); // remove wrong characters
						//System.out.println("T: " + t1.toLowerCase() + " F: " + p.wrongTokens.contains(t1.toLowerCase()));
						if (i == l - 1
								&& (StringUtils.equalsIgnoreCase(t1, "ma") || StringUtils.equalsIgnoreCase(t1, "ba"))
								|| (i == 0 && (StringUtils.equalsIgnoreCase(t1, "ma")
										|| StringUtils.equalsIgnoreCase(t1, "ba")))) {
							// special case
							mtokens1.append(t1).append(" ");
						} else if(!p.wrongTokens.contains(t1.toLowerCase())) {
							// System.out.println("T: " + t1);
							mtokens1.append(t1).append(t1.length() == 1 ? "." : "").append(" ");
						}
					}
					String name = mtokens1.toString().trim();
					if(StringUtils.isNotBlank(name)) {
						//System.out.println("Output: " + name);
						names.add(name);
					} else {
						names.add(""); // blank add to delete
					}
				}
				return names;
			}
		});
		//p.addTextLogger("temp_test");
		p.correctData();
		
		System.out.println("Done.");
	}
}