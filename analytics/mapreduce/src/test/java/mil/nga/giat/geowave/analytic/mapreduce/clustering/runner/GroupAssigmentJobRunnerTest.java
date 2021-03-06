package mil.nga.giat.geowave.analytic.mapreduce.clustering.runner;

import java.io.IOException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.util.HashMap;

import javax.security.auth.Subject;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.ScopedJobConfiguration;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.distance.GeometryCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveAnalyticJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceIntegration;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.clustering.GroupAssignmentMapReduce;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters.MRConfig;
import mil.nga.giat.geowave.analytic.param.ParameterHelper;
import mil.nga.giat.geowave.analytic.param.StoreParameters.StoreParam;
import mil.nga.giat.geowave.analytic.store.PersistableAdapterStore;
import mil.nga.giat.geowave.analytic.store.PersistableDataStore;
import mil.nga.giat.geowave.analytic.store.PersistableIndexStore;
import mil.nga.giat.geowave.core.cli.AdapterStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.IndexStoreCommandLineOptions;
import mil.nga.giat.geowave.core.store.memory.MemoryAdapterStoreFactory;
import mil.nga.giat.geowave.core.store.memory.MemoryDataStoreFactory;
import mil.nga.giat.geowave.core.store.memory.MemoryIndexStoreFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.geotools.feature.type.BasicFeatureTypes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeatureType;

public class GroupAssigmentJobRunnerTest
{

	final GroupAssigmentJobRunner runner = new GroupAssigmentJobRunner();
	final PropertyManagement runTimeProperties = new PropertyManagement();
	private static final String TEST_NAMESPACE = "test";

	@Before
	public void init() {
		final SimpleFeatureType ftype = AnalyticFeature.createGeometryFeatureAdapter(
				"centroidtest",
				new String[] {
					"extra1"
				},
				BasicFeatureTypes.DEFAULT_NAMESPACE,
				ClusteringUtils.CLUSTERING_CRS).getType();

		runner.setMapReduceIntegrater(new MapReduceIntegration() {
			@Override
			public int submit(
					final Configuration configuration,
					final PropertyManagement runTimeProperties,
					final GeoWaveAnalyticJobRunner tool )
					throws Exception {
				tool.setConf(configuration);
				((ParameterHelper<Object>) StoreParam.ADAPTER_STORE.getHelper()).setValue(
						configuration,
						GroupAssignmentMapReduce.class,
						StoreParam.ADAPTER_STORE.getHelper().getValue(
								runTimeProperties));
				return tool.run(new String[] {});
			}

			@Override
			public Counters waitForCompletion(
					final Job job )
					throws ClassNotFoundException,
					IOException,
					InterruptedException {

				Assert.assertEquals(
						SequenceFileInputFormat.class,
						job.getInputFormatClass());
				Assert.assertEquals(
						10,
						job.getNumReduceTasks());
				final ScopedJobConfiguration configWrapper = new ScopedJobConfiguration(
						job.getConfiguration(),
						GroupAssignmentMapReduce.class);
				Assert.assertEquals(
						"file://foo/bin",
						job.getConfiguration().get(
								"mapred.input.dir"));

				Assert.assertEquals(
						3,
						configWrapper.getInt(
								CentroidParameters.Centroid.ZOOM_LEVEL,
								-1));
				Assert.assertEquals(
						"b1234",
						configWrapper.getString(
								GlobalParameters.Global.PARENT_BATCH_ID,
								""));
				Assert.assertEquals(
						"b12345",
						configWrapper.getString(
								GlobalParameters.Global.BATCH_ID,
								""));

				try {
					final AnalyticItemWrapperFactory<?> wrapper = configWrapper.getInstance(
							CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
							AnalyticItemWrapperFactory.class,
							SimpleFeatureItemWrapperFactory.class);

					Assert.assertEquals(
							SimpleFeatureItemWrapperFactory.class,
							wrapper.getClass());

					final DistanceFn<?> distancFn = configWrapper.getInstance(
							CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
							DistanceFn.class,
							GeometryCentroidDistanceFn.class);

					Assert.assertEquals(
							FeatureCentroidDistanceFn.class,
							distancFn.getClass());

				}
				catch (final InstantiationException e) {
					throw new IOException(
							"Unable to configure system",
							e);
				}
				catch (final IllegalAccessException e) {
					throw new IOException(
							"Unable to configure system",
							e);
				}

				return new Counters();
			}

			@Override
			public Job getJob(
					final Tool tool )
					throws IOException {
				return new Job(
						tool.getConf());
			}

			@Override
			public Configuration getConfiguration(
					final PropertyManagement runTimeProperties )
					throws IOException {
				return new Configuration();
			}
		});
		runner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(
				new Path(
						"file://foo/bin")));
		runner.setZoomLevel(3);
		runner.setReducerCount(10);

		runTimeProperties.store(
				MRConfig.HDFS_BASE_DIR,
				"/");

		runTimeProperties.store(
				GlobalParameters.Global.BATCH_ID,
				"b12345");
		runTimeProperties.store(
				GlobalParameters.Global.PARENT_BATCH_ID,
				"b1234");

		runTimeProperties.store(
				CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
				FeatureCentroidDistanceFn.class);

		runTimeProperties.store(
				StoreParam.DATA_STORE,
				new PersistableDataStore(
						new DataStoreCommandLineOptions(
								new MemoryDataStoreFactory(),
								new HashMap<String, Object>(),
								TEST_NAMESPACE)));
		final MemoryAdapterStoreFactory adapterStoreFactory = new MemoryAdapterStoreFactory();
		runTimeProperties.store(
				StoreParam.ADAPTER_STORE,
				new PersistableAdapterStore(
						new AdapterStoreCommandLineOptions(
								adapterStoreFactory,
								new HashMap<String, Object>(),
								TEST_NAMESPACE)));

		runTimeProperties.store(
				StoreParam.INDEX_STORE,
				new PersistableIndexStore(
						new IndexStoreCommandLineOptions(
								new MemoryIndexStoreFactory(),
								new HashMap<String, Object>(),
								TEST_NAMESPACE)));
		adapterStoreFactory.createStore(
				new HashMap<String, Object>(),
				TEST_NAMESPACE).addAdapter(
				new FeatureDataAdapter(
						ftype));
	}

	@Test
	public void test()
			throws Exception {

		runner.run(runTimeProperties);
	}
}
