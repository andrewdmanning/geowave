package mil.nga.giat.geowave.adapter.vector.plugin;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.GtFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.plugin.transaction.GeoWaveTransaction;
import mil.nga.giat.geowave.adapter.vector.plugin.transaction.TransactionsAllocator;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

import org.opengis.feature.simple.SimpleFeature;

public class GeoWaveDataStoreComponents
{
	private final GtFeatureDataAdapter adapter;
	private final DataStore dataStore;
	private final IndexStore indexStore;
	private final DataStatisticsStore dataStatisticsStore;
	private final GeoWaveGTDataStore gtStore;
	private final TransactionsAllocator transactionAllocator;

	private final PrimaryIndex currentIndex;

	public GeoWaveDataStoreComponents(
			final DataStore dataStore,
			final DataStatisticsStore dataStatisticsStore,
			final IndexStore indexStore,
			final GtFeatureDataAdapter adapter,
			final GeoWaveGTDataStore gtStore,
			final TransactionsAllocator transactionAllocator ) {
		this.adapter = adapter;
		this.dataStore = dataStore;
		this.indexStore = indexStore;
		this.dataStatisticsStore = dataStatisticsStore;
		this.gtStore = gtStore;
		currentIndex = gtStore.getIndex(adapter);
		this.transactionAllocator = transactionAllocator;
	}

	public IndexStore getIndexStore() {
		return indexStore;
	}

	public GtFeatureDataAdapter getAdapter() {
		return adapter;
	}

	public DataStore getDataStore() {
		return dataStore;
	}

	public GeoWaveGTDataStore getGTstore() {
		return gtStore;
	}

	public PrimaryIndex getCurrentIndex() {
		return currentIndex;
	}

	public DataStatisticsStore getStatsStore() {
		return dataStatisticsStore;
	}

	public CloseableIterator<Index<?, ?>> getIndices(
			Constraints timeConstraints,
			Constraints geoConstraints ) {
		return getGTstore().getIndexQueryStrategy().getIndices(
				timeConstraints,
				geoConstraints,
				getIndexStore().getIndices());
	}

	public void remove(
			final SimpleFeature feature,
			final GeoWaveTransaction transaction )
			throws IOException {

		dataStore.delete(
				new QueryOptions(
						adapter,
						currentIndex,
						transaction.composeAuthorizations()),
				new DataIdQuery(
						adapter.getAdapterId(),
						adapter.getDataId(feature)));
	}

	public void remove(
			final String fid,
			final GeoWaveTransaction transaction )
			throws IOException {

		dataStore.delete(
				new QueryOptions(
						adapter,
						currentIndex,
						transaction.composeAuthorizations()),
				new DataIdQuery(
						new ByteArrayId(
								StringUtils.stringToBinary(fid)),
						adapter.getAdapterId()));

	}

	@SuppressWarnings("unchecked")
	public void write(
			final Iterator<SimpleFeature> featureIt,
			final Set<String> fidList,
			final GeoWaveTransaction transaction )
			throws IOException {
		try (IndexWriter indexWriter = dataStore.createIndexWriter(
				currentIndex,
				new UniformVisibilityWriter<SimpleFeature>(
						new GlobalVisibilityHandler(
								transaction.composeVisibility())))) {
			while (featureIt.hasNext()) {
				final SimpleFeature feature = featureIt.next();
				fidList.add(feature.getID());
				indexWriter.write(
						adapter,
						feature);
			}
		}
	}

	public List<ByteArrayId> writeCommit(
			final SimpleFeature feature,
			final GeoWaveTransaction transaction )
			throws IOException {
		try (IndexWriter indexWriter = dataStore.createIndexWriter(
				currentIndex,
				new UniformVisibilityWriter<SimpleFeature>(
						new GlobalVisibilityHandler(
								transaction.composeVisibility())))) {
			return indexWriter.write(
					adapter,
					feature);
		}
	}

	public String getTransaction()
			throws IOException {
		return transactionAllocator.getTransaction();
	}

	public void releaseTransaction(
			final String txID )
			throws IOException {
		transactionAllocator.releaseTransaction(txID);
	}
}
