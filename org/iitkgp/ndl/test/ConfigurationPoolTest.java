package org.iitkgp.ndl.test;

import static org.junit.Assert.assertEquals;

import org.iitkgp.ndl.data.container.ConfigurationData;
import org.iitkgp.ndl.data.container.ConfigurationPool;
import org.iitkgp.ndl.data.exception.InvalidConfigurationKeyException;
import org.iitkgp.ndl.util.NDLDataUtils;
import org.junit.Test;

/**
 * Test cases of {@link ConfigurationPool}
 * @author Aurghya, Debasis
 */
public class ConfigurationPoolTest {
	
	/**
	 * test
	 * @throws Exception throws error if resource loading error occurs
	 */
	@Test
	public void test() throws Exception {
		
		String file1 = "/data/configuration.pool.test.data1.csv";
		String file2 = "/data/configuration.pool.test.data2.csv";
		String file3 = "/data/configuration.pool.test.data3.csv";
		String file4 = "/data/configuration.pool.test.data4.csv";
		String file5 = "/data/configuration.pool.test.data5.csv";
		
		ConfigurationPool conf = new ConfigurationPool();
		// loads resources to pool
		conf.addResource(NDLDataUtils.loadResource(file1), "Journal", "journal");
		conf.addResource(NDLDataUtils.loadResource(file2), "ID", "lrt");
		conf.addResource(NDLDataUtils.loadResource(file3), "delete");
		conf.addResource(NDLDataUtils.loadResource(file4), "Subject", "ddc", true); // ignore case
		conf.addResource(NDLDataUtils.loadResource(file5), "Subject", "ddc", true); // ignore case
		// test
		assertEquals("https://journals.lww.com/aacr/pages/default.aspx",
				conf.get("journal.A & A Case Reports.Journal URL"));
		assertEquals("2325-7237", conf.get("journal.A & A Case Reports.E-ISSN"));
		assertEquals(true, conf.contains("journal.AACN Advanced Critical Care")); // pk exist test
		assertEquals(false, conf.contains("journal.xxx"));
		assertEquals(false, conf.contains("xxx")); // logical name exist test
		assertEquals(false, conf.contains("journal.AACN Advanced Critical Care.xxx")); // attribute exist test
		assertEquals("", conf.get("journal.A & A Case Reports.P-ISSN")); // blank value check
		
		// check dot primary key
		assertEquals(true, conf.contains("journal." + ConfigurationData.escapeDot("Asian.Nursing.Research")));
		assertEquals(false, conf.contains("journal." + ConfigurationData.escapeDot("Asian.Nursing.Research1")));
		assertEquals("https://www.sciencedirect.com/journal/asian-nursing-research",
				conf.get("journal." + ConfigurationData.escapeDot("Asian.Nursing.Research") + ".Journal URL"));
		
		assertEquals("discussion", conf.get("lrt.mdl-27097995"));
		assertEquals(true, conf.contains("lrt.mdl-18641482"));
		assertEquals(false, conf.contains("lrt.mdl-18641482xxx"));
		assertEquals(true, conf.contains("delete.mdl-22785537"));
		assertEquals(false, conf.contains("delete.mdl-22785537xxx"));
		assertEquals(true, conf.contains("delete"));
		// test exceptions
		boolean error = false;
		try {
			conf.get("journal.A & A Case Reports.E-ISSN.xxx"); // invalid expression
		} catch(InvalidConfigurationKeyException ex) {
			// error
			error = true;
		}
		assertEquals(true, error);
		error = false;
		try {
			conf.get("journal.A & A Case Reports.xxx"); // invalid data access
		} catch(InvalidConfigurationKeyException ex) {
			// error
			error = true;
		}
		assertEquals(true, error);
		error = false;
		try {
			conf.get("journal.xxx.E-ISSN"); // invalid data access
		} catch(InvalidConfigurationKeyException ex) {
			// error
			error = true;
		}
		assertEquals(true, error);
		
		// ignore case test
		assertEquals("633", conf.get("ddc.tea"));
		assertEquals("634", conf.get("ddc.forest"));
	}
	
	/*@Test
	public void test1() throws Exception {
		String file1 = "/data/core.thesus.language.csv";
		
		ConfigurationPool conf = new ConfigurationPool();
		// loads resources to pool
		conf.addResource(NDLDataUtils.loadResource(file1), "Journal", "journal");
	}*/
}