package mil.nga.giat.geowave.core.ingest;

import java.util.Collection;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

import org.apache.log4j.Logger;

/**
 * This models any information that is necessary to ingest an entry into
 * GeoWave: the adapter and index you wish to use as well as the actual data
 * 
 * @param <T>
 *            The java type for the actual data being ingested
 */
public class GeoWaveData<T>
{
	private final static Logger LOGGER = Logger.getLogger(GeoWaveData.class);
	private final ByteArrayId adapterId;
	private final Collection<ByteArrayId> indexIds;
	private final WritableDataAdapter<T> adapter;
	private final T data;

	public GeoWaveData(
			final ByteArrayId adapterId,
			final Collection<ByteArrayId> indexIds,
			final T data ) {
		this.adapterId = adapterId;
		this.indexIds = indexIds;
		this.data = data;

		// in this case the actual adapter is meant to be looked up using the ID
		this.adapter = null;
	}

	public GeoWaveData(
			final WritableDataAdapter<T> adapter,
			final Collection<ByteArrayId> indexIds,
			final T data ) {
		this.adapter = adapter;
		this.data = data;
		this.indexIds = indexIds;

		this.adapterId = adapter.getAdapterId();
	}

	public WritableDataAdapter<T> getAdapter(
			final AdapterStore adapterCache ) {
		if (adapter != null) {
			return adapter;
		}
		final DataAdapter<?> adapter = adapterCache.getAdapter(adapterId);
		if (adapter instanceof WritableDataAdapter) {
			return (WritableDataAdapter<T>) adapter;
		}
		LOGGER.warn("Adapter is not writable");
		return null;
	}

	public Collection<ByteArrayId> getIndexIds() {
		return indexIds;
	}

	public GeoWaveOutputKey getKey() {
		return new GeoWaveOutputKey(
				adapterId,
				indexIds);
	}

	public T getValue() {
		return data;
	}
}
