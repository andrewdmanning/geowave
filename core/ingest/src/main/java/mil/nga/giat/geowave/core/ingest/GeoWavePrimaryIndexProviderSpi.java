package mil.nga.giat.geowave.core.ingest;

import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public interface GeoWavePrimaryIndexProviderSpi extends
		GeoWaveCLIOptionsProvider
{
	public PrimaryIndex getPrimaryIndex();
}
