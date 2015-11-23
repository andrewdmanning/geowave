package mil.nga.giat.geowave.examples.query;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.google.common.io.Files;
import com.vividsolutions.jts.geom.Coordinate;

import mil.nga.giat.geowave.adapter.vector.KryoFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;

public class EndToEndKryoExample
{
	private static final Logger LOGGER = Logger.getLogger(
			EndToEndKryoExample.class);
	private static File tempAccumuloDir;
	private static MiniAccumuloCluster accumulo;
	private static DataStore dataStore;
	private static SimpleFeatureType schema;
	private static SimpleFeatureBuilder builder;
	private static String TYPE_NAME = "stateCapitalData";
	private static String BASE_DIR = "/src/main/resources/";
	private static String FILE = "stateCapitals.csv";
	private static WritableDataAdapter<SimpleFeature> dataAdapter;
	private static PrimaryIndex index;

	public static void main(
			final String[] args )
					throws AccumuloException,
					AccumuloSecurityException,
					IOException,
					InterruptedException,
					SchemaException,
					CQLException {

		setup();
		ingest();
		executeSpatialOnlyQuery();
		executeSpatialWithFilterQuery();
		cleanup();

	}

	private static void setup()
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			InterruptedException,
			SchemaException {

		final String ACCUMULO_USER = "root";
		final String ACCUMULO_PASSWORD = "Ge0wave";
		final String TABLE_NAMESPACE = "";

		tempAccumuloDir = Files.createTempDir();
		tempAccumuloDir.deleteOnExit();

		accumulo = new MiniAccumuloCluster(
				new MiniAccumuloConfig(
						tempAccumuloDir,
						ACCUMULO_PASSWORD));

		accumulo.start();

		dataStore = new AccumuloDataStore(
				new BasicAccumuloOperations(
						accumulo.getZooKeepers(),
						accumulo.getInstanceName(),
						ACCUMULO_USER,
						ACCUMULO_PASSWORD,
						TABLE_NAMESPACE));

		index = IndexType.SPATIAL_VECTOR.createDefaultIndex();

		schema = DataUtilities.createType(
				TYPE_NAME,
				"location:Geometry," + "city:String," + "state:String," + "since:Date," + "landArea:Double," + "munincipalPop:Integer," + "notes:String");

		dataAdapter = new KryoFeatureDataAdapter(
				schema);

		builder = new SimpleFeatureBuilder(
				schema);

		LOGGER.info(
				"Setup completed.");
	}

	public static void ingest()
			throws FileNotFoundException,
			IOException {
		final List<SimpleFeature> features = loadStateCapitalData();

		try (@SuppressWarnings("unchecked")
		IndexWriter writer = dataStore.createIndexWriter(
				index,
				DataStoreUtils.DEFAULT_VISIBILITY)) {
			for (final SimpleFeature aFeature : features) {
				writer.write(
						dataAdapter,
						aFeature);
			}
		}

		LOGGER.info(
				"Ingest completed.");
	}

	public static void executeSpatialOnlyQuery()
			throws IOException,
			CQLException {
		final Query query = new CQLQuery(
				"BBOX(location,-79.9704779,32.8210454,-87.96743,43.0578914)",
				dataAdapter);
		int numMatches = 0;
		try (final CloseableIterator<SimpleFeature> matches = dataStore.query(
				new QueryOptions(
						dataAdapter,
						index),
				query)) {
			while (matches.hasNext()) {
				final SimpleFeature currFeature = matches.next();
				if (currFeature.getFeatureType().getTypeName().equals(
						TYPE_NAME)) {
					numMatches++;
					System.out.println(
							currFeature.getAttribute(
									"city"));
				}
			}
		}
		LOGGER.info(
				"***** NUM MATCHES + " + numMatches); // expect 8
	}

	public static void executeSpatialWithFilterQuery()
			throws CQLException,
			IOException {
		final Query query = new CQLQuery(
				"BBOX(location,-79.9704779,32.8210454,-87.96743,43.0578914) and notes like 'scala'",
				dataAdapter);
		int numMatches = 0;
		try (final CloseableIterator<SimpleFeature> matches = dataStore.query(
				new QueryOptions(
						dataAdapter,
						index),
				query)) {
			while (matches.hasNext()) {
				final SimpleFeature currFeature = matches.next();
				if (currFeature.getFeatureType().getTypeName().equals(
						TYPE_NAME)) {
					numMatches++;
					System.out.println(
							currFeature.getAttribute(
									"city"));
				}
			}
		}
		LOGGER.info(
				"***** NUM MATCHES + " + numMatches); // expect 4
	}

	public static List<SimpleFeature> loadStateCapitalData()
			throws FileNotFoundException,
			IOException {
		final List<SimpleFeature> features = new ArrayList<>();
		final String fileName = System.getProperty(
				"user.dir") + BASE_DIR + FILE;
		try (final BufferedReader br = new BufferedReader(
				new FileReader(
						fileName))) {
			for (String line = br.readLine(); line != null; line = br.readLine()) {
				final String[] vals = line.split(
						",");
				final String state = vals[0];
				final String city = vals[1];
				final double lng = Double.parseDouble(
						vals[2]);
				final double lat = Double.parseDouble(
						vals[3]);
				@SuppressWarnings("deprecation")
				final Date since = new Date(
						Integer.parseInt(
								vals[4]) - 1900,
						0,
						1);
				final double landArea = Double.parseDouble(
						vals[5]);
				final int munincipalPop = Integer.parseInt(
						vals[6]);
				final String notes = (vals.length > 7) ? vals[7] : null;
				features.add(
						buildSimpleFeature(
								state,
								city,
								lng,
								lat,
								since,
								landArea,
								munincipalPop,
								notes));
			}
		}
		return features;
	}

	private static SimpleFeature buildSimpleFeature(
			final String state,
			final String city,
			final double lng,
			final double lat,
			final Date since,
			final double landArea,
			final int munincipalPop,
			final String notes ) {
		builder.set(
				"location",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(
						new Coordinate(
								lng,
								lat)));
		builder.set(
				"state",
				state);
		builder.set(
				"city",
				city);
		builder.set(
				"since",
				since);
		builder.set(
				"landArea",
				landArea);
		builder.set(
				"munincipalPop",
				munincipalPop);
		builder.set(
				"notes",
				notes);
		return builder.buildFeature(
				UUID.randomUUID().toString());
	}

	private static void cleanup()
			throws IOException,
			InterruptedException {

		try {
			accumulo.stop();
		}
		finally {
			FileUtils.deleteDirectory(
					tempAccumuloDir);
		}

		LOGGER.info(
				"Cleanup completed.");
	}

}
