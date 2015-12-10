package mil.nga.giat.geowave.adapter.vector.plugin.transaction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveDataStoreComponents;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;

import org.opengis.feature.simple.SimpleFeature;

public abstract class AbstractTransactionManagement implements
		GeoWaveTransaction
{

	protected final GeoWaveDataStoreComponents components;

	public AbstractTransactionManagement(
			final GeoWaveDataStoreComponents components ) {
		super();
		this.components = components;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Map<ByteArrayId, DataStatistics<SimpleFeature>> getDataStatistics() {
		final Map<ByteArrayId, DataStatistics<SimpleFeature>> stats = new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>();
		final GeotoolsFeatureDataAdapter adapter = components.getAdapter();
		final ByteArrayId[] ids = adapter.getSupportedStatisticsIds();
		final Set<ByteArrayId> idSet = new HashSet<ByteArrayId>();
		for (final ByteArrayId id : ids) {
			idSet.add(id);
		}
		try (CloseableIterator<DataStatistics<?>> it = components.getStatsStore().getDataStatistics(
				adapter.getAdapterId(),
				composeAuthorizations())) {
			while (it.hasNext()) {
				final DataStatistics<?> stat = it.next();
				if (idSet.contains(stat.getStatisticsId())) {
					stats.put(
							stat.getStatisticsId(),
							(DataStatistics<SimpleFeature>) stat);
				}
			}

		}
		catch (final Exception e) {
			GeoWaveTransactionManagement.LOGGER.error(
					"Failed to access statistics from data store",
					e);
		}
		return stats;
	}

}
