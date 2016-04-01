/**
 *
 */
package mil.nga.giat.geowave.test;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Map;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import com.google.common.io.Files;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.core.cli.GenericStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.GeoWaveMain;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStoreFactory;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.hbase.HBaseDataStoreFactory;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

/**
 * @author viggy
 *
 */
public class GeoWaveHBaseTestEnvironment extends
		GeoWaveTestEnvironment
{

	private final static Logger LOGGER = Logger.getLogger(
			GeoWaveHBaseTestEnvironment.class);
	protected static BasicHBaseOperations operations;
	protected static String zookeeper;
	protected static File TEMP_DIR = new File(
			"./target/hbase_temp"); // breaks on windows if temp directory
									// isn't on same drive as project
	private static HBaseTestingUtility utility;
	private static MiniHBaseCluster hbaseInstance;

	@BeforeClass
	public static void setup()
			throws IOException {
		synchronized (MUTEX) {
			TimeZone.setDefault(
					TimeZone.getTimeZone(
							"GMT"));
			if (operations == null) {
				zookeeper = System.getProperty(
						"zookeeperUrl");
				if (!isSet(
						zookeeper)) {

					if (!TEMP_DIR.exists()) {
						if (!TEMP_DIR.mkdirs()) {
							throw new IOException(
									"Could not create temporary directory");
						}
					}
//					TEMP_DIR.deleteOnExit();

					// if (SystemUtils.IS_OS_WINDOWS && isYarn()) {
					// // this must happen before the cluster is started
					// // because yarn
					// // expects winutils.exe to exist within a bin
					// // directory in the hbase cluster directory
					// // (hbase will always set this
					// // directory as hadoop_home)
					// LOGGER.info(
					// "Running YARN on windows requires a local installation of
					// Hadoop");
					// LOGGER.info(
					// "'HADOOP_HOME' must be set and 'PATH' must contain
					// %HADOOP_HOME%/bin");
					//
					// final Map<String, String> env = System.getenv();
					// String hadoopHome = System.getProperty(
					// "hadoop.home.dir");
					// if (hadoopHome == null) {
					// hadoopHome = env.get(
					// "HADOOP_HOME");
					// }
					// boolean success = false;
					// if (hadoopHome != null) {
					// final File hadoopDir = new File(
					// hadoopHome);
					// if (hadoopDir.exists()) {
					// final File binDir = new File(
					// TEMP_DIR,
					// "bin");
					// if (binDir.mkdir()) {
					// FileUtils.copyFile(
					// new File(
					// hadoopDir + File.separator + "bin",
					// HADOOP_WINDOWS_UTIL),
					// new File(
					// binDir,
					// HADOOP_WINDOWS_UTIL));
					// success = true;
					// }
					// }
					// }
					// if (!success) {
					// LOGGER.error(
					// "'HADOOP_HOME' environment variable is not set or
					// <HADOOP_HOME>/bin/winutils.exe does not exist");
					// return;
					// }
					// }

					Configuration conf = new Configuration();
					System.setProperty(
							"test.build.data",
							TEMP_DIR.getAbsolutePath());
					conf.set(
							"test.build.data",
							new File(
									TEMP_DIR,
									"zookeeper").getAbsolutePath());
					conf.set(
							"fs.default.name",
							"file:///");
					conf.set(
							"zookeeper.session.timeout",
							"180000");
					conf.set(
							"hbase.zookeeper.peerport",
							"2888");
					conf.set(
							"hbase.zookeeper.property.clientPort",
							"2181");
					conf.set("hbase.zookeeper.quorum", "127.0.0.1");
					conf.addResource(
							new Path(
									"conf/hbase-site1.xml"));
					try {
						File masterDir = new File(
								TEMP_DIR,
								"hbase");
						conf.set(
								HConstants.HBASE_DIR,
								masterDir.toURI().toURL().toString());
					}
					catch (MalformedURLException e1) {
						LOGGER.error(
								e1.getMessage());
					}

					Configuration hbaseConf = HBaseConfiguration.create(
							conf);
					hbaseConf.set("hbase.zookeeper.quorum", "127.0.0.1");
					utility = new HBaseTestingUtility(
							hbaseConf);

					// utility = new HBaseTestingUtility();
					try {
						utility.startMiniCluster(
								2);
					}
					catch (Exception e) {
						LOGGER.error(
								e);
						e.printStackTrace();
						Assert.fail(
								"Could not start HBaseMiniCluster");
					}

					zookeeper = utility.getZooKeeperWatcher().getBaseZNode();
					hbaseInstance = utility.getMiniHBaseCluster();


					
					Connection conn = utility.getConnection();
//					Connection conn = ConnectionFactory.createConnection(
//							hbaseConf);

					operations = new BasicHBaseOperations(
							TEST_NAMESPACE,
							conn);

				}
				else {
					try {
						operations = new BasicHBaseOperations(
								zookeeper,
								TEST_NAMESPACE);
					}
					catch (final IOException e) {
						LOGGER.warn(
								"Unable to connect to HBase",
								e);
						Assert.fail(
								"Could not connect to HBase instance: '" + e.getLocalizedMessage() + "'");
					}
				}
			}
		}
	}

	@SuppressFBWarnings(value = {
		"SWL_SLEEP_WITH_LOCK_HELD"
	}, justification = "Sleep in lock while waiting for external resources")
	@AfterClass
	public static void cleanup() {
		synchronized (MUTEX) {
			if (!DEFER_CLEANUP.get()) {

				if (operations == null) {
					Assert.fail(
							"Invalid state <null> for hbase operations during CLEANUP phase");
				}
				try {
					operations.deleteAll();
				}
				catch (final IOException ex) {
					LOGGER.error(
							"Unable to clear hbase namespace",
							ex);
					Assert.fail(
							"Index not deleted successfully");
				}

				operations = null;
				zookeeper = null;

				if (TEMP_DIR != null) {
					try {
						Thread.sleep(
								1000);
						FileUtils.deleteDirectory(
								TEMP_DIR);
						TEMP_DIR = null;
					}
					catch (final IOException | InterruptedException e) {
						LOGGER.warn(
								"Unable to delete mini hbase temporary directory",
								e);
					}
				}
			}
		}
	}

	public BasicHBaseOperations getOperations() {
		return operations;
	}

	@Override
	protected void testLocalIngest(
			final DimensionalityType dimensionalityType,
			final String ingestFilePath ) {
		// ingest a shapefile (geotools type) directly into GeoWave using the
		// ingest framework's main method and pre-defined commandline arguments
		LOGGER.warn(
				"Ingesting '" + ingestFilePath + "' - this may take several minutes...");
//		final String[] args = StringUtils.split(
//				"-localhbaseingest -f geotools-vector -b " + ingestFilePath + " -z " + zookeeper + " -n " + TEST_NAMESPACE + " -dim " + dimensionalityType.getDimensionalityArg(),
//				' ');
		final String[] args = StringUtils.split(
				"-localingest -datastore " + new HBaseDataStoreFactory().getName() + " -f geotools-vector -b " + ingestFilePath + " -" + GenericStoreCommandLineOptions.NAMESPACE_OPTION_KEY + " " + TEST_NAMESPACE + " -dim " + dimensionalityType.getDimensionalityArg() + " -" + BasicHBaseOperations.ZOOKEEPER_INSTANCES_NAME + " " + zookeeper,
				' ');
		GeoWaveMain.main(
				args);
		verifyStats();
	}

	private void verifyStats() {
		GeoWaveMain.main(
				new String[] {
					"-hbasestatsdump",
					"-z",
					zookeeper,
					"-n",
					TEST_NAMESPACE
		});
	}

}
