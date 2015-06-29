package mil.nga.giat.geowave.datastore.hbase.util;

import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.IngestCallback;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;

public class AltIndexIngestCallback<T> implements
		IngestCallback<T>
{
	final private HBaseWriter writer;
	final WritableDataAdapter<T> writeableAdapter;

	public AltIndexIngestCallback(
			final HBaseWriter writer,
			final WritableDataAdapter<T> writeableAdapter ) {
		this.writer = writer;
		this.writeableAdapter = writeableAdapter;
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		HBaseUtils.writeAltIndex(
				writeableAdapter,
				entryInfo,
				entry,
				writer);
	}
}
