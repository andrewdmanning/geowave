package mil.nga.giat.geowave.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite.SuiteClasses;

import mil.nga.giat.geowave.test.config.ConfigCacheIT;
import mil.nga.giat.geowave.test.kafka.BasicKafkaIT;
import mil.nga.giat.geowave.test.landsat.LandsatIT;
import mil.nga.giat.geowave.test.mapreduce.BasicMapReduceIT;
import mil.nga.giat.geowave.test.mapreduce.BulkIngestInputGenerationIT;
import mil.nga.giat.geowave.test.mapreduce.DBScanIT;
import mil.nga.giat.geowave.test.mapreduce.GeoWaveKMeansIT;
import mil.nga.giat.geowave.test.mapreduce.GeoWaveNNIT;
import mil.nga.giat.geowave.test.mapreduce.KDERasterResizeIT;
import mil.nga.giat.geowave.test.query.AttributesSubsetQueryIT;
import mil.nga.giat.geowave.test.query.PolygonDataIdQueryIT;
import mil.nga.giat.geowave.test.query.SecondaryIndexingQueryIT;
import mil.nga.giat.geowave.test.query.SpatialTemporalQueryIT;
import mil.nga.giat.geowave.test.service.GeoServerIT;
import mil.nga.giat.geowave.test.service.GeoWaveIngestGeoserverIT;
import mil.nga.giat.geowave.test.service.GeoWaveServicesIT;
import mil.nga.giat.geowave.test.store.DataStatisticsStoreIT;

@RunWith(GeoWaveITSuiteRunner.class)
@SuiteClasses({
	GeoWaveBasicIT.class,
	BasicKafkaIT.class,
	BasicMapReduceIT.class,
	GeoWaveRasterIT.class,

	LandsatIT.class,
	
	// BulkIngestInputGenerationIT.class, // Accumulo-dependent

	KDERasterResizeIT.class,
	GeoWaveKMeansIT.class,
	GeoWaveNNIT.class,

	// GeoServerIT.class, // Fails in HBase:
	// net.sf.json.JSONException: A JSONObject text must begin with '{' at
	// character 1 of No such datastore: geowave_test,mil_nga_giat_geowave_test
	// at net.sf.json.util.JSONTokener.syntaxError(JSONTokener.java:499)
	// at net.sf.json.JSONObject._fromJSONTokener(JSONObject.java:972)
	// at net.sf.json.JSONObject._fromString(JSONObject.java:1201)
	// at net.sf.json.JSONObject.fromObject(JSONObject.java:165)
	// at net.sf.json.JSONObject.fromObject(JSONObject.java:134)
	// at
	// mil.nga.giat.geowave.service.client.GeoserverServiceClient.getDatastore(GeoserverServiceClient.java:112)
	// at
	// mil.nga.giat.geowave.test.service.GeoServerIT.initialize(GeoServerIT.java:130)

	// GeoWaveServicesIT.class, // ???
	// GeoWaveIngestGeoserverIT.class, // ???

	// AttributesSubsetQueryIT.class, // Fails in HBase - doesn't do field
	// subsetting in testServerSideFiltering - need to add support for that

	SecondaryIndexingQueryIT.class,
	DBScanIT.class,
	SpatialTemporalQueryIT.class,

	// PolygonDataIdQueryIT.class, // Fails in HBase - returns extra results

	// ConfigCacheIT.class, // Accumulo-dependent - change from Accumulo*** to
	// Memory*** and put in core-store

	DataStatisticsStoreIT.class, // HBase-dependent
})
public class GeoWaveITSuite
{
	@BeforeClass
	public static void setupSuite() {
		synchronized (GeoWaveITRunner.MUTEX) {
			GeoWaveITRunner.DEFER_CLEANUP.set(true);
		}
	}

	@AfterClass
	public static void tearDownSuite() {
		synchronized (GeoWaveITRunner.MUTEX) {
			GeoWaveITRunner.DEFER_CLEANUP.set(false);
		}
	}
}