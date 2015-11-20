package mil.nga.giat.geowave.core.geotime.ingest;

import mil.nga.giat.geowave.core.geotime.DimensionalityType;
import mil.nga.giat.geowave.core.ingest.IndexCompatibilityVisitor;
import mil.nga.giat.geowave.core.ingest.IngestDimensionalityTypeProviderSpi;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class SpatialTemporalByYearDimensionalityTypeProvider implements
		IngestDimensionalityTypeProviderSpi
{
	@Override
	public IndexCompatibilityVisitor getCompatibilityVisitor() {
		return new SpatialTemporalIndexCompatibilityVisitor();
	}

	@Override
	public String getDimensionalityTypeName() {
		return "spatial-temporal-by-year";
	}

	@Override
	public String getDimensionalityTypeDescription() {
		return "This dimensionality type matches all indices that only require Latitude, Longitude, and Time definitions using a year periodicity.";
	}

	@Override
	public int getPriority() {
		// arbitrary - just lower than spatial-temporal
		return 5;
	}

	private static class SpatialTemporalIndexCompatibilityVisitor implements
			IndexCompatibilityVisitor
	{

		@Override
		public boolean isCompatible(
				final PrimaryIndex index ) {
			return DimensionalityType.SPATIAL_TEMPORAL_YEAR.isCompatible(index);
		}

	}
}
