package mil.nga.giat.geowave.core.ingest;

import org.apache.commons.cli.Options;

import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public interface GeoWavePrimaryIndexProviderSpi extends GeoWaveCLIOptionsProvider
{
	public PrimaryIndex getPrimaryIndex();
}
