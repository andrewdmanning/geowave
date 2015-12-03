package mil.nga.giat.geowave.core.ingest;

import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public interface IndexConverterSpi extends
		GeoWaveCLIOptionsProvider
{
	public PrimaryIndex getIndex(
			PrimaryIndex index );
}
