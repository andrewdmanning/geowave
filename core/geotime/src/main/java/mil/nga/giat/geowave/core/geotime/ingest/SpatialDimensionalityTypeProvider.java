package mil.nga.giat.geowave.core.geotime.ingest;

import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.dimension.LatitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.LongitudeField;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import mil.nga.giat.geowave.core.ingest.index.IngestDimensionalityTypeProviderSpi;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.BasicIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

import com.beust.jcommander.Parameter;

public class SpatialDimensionalityTypeProvider implements
		IngestDimensionalityTypeProviderSpi
{
	private final SpatialOptions options = new SpatialOptions();
	private static final int LONGITUDE_BITS = 31;
	private static final int LATITUDE_BITS = 31;
	public static final int[] DEFINED_BITS_OF_PRECISION = new int[] {
		0,
		1,
		2,
		3,
		4,
		5,
		6,
		7,
		8,
		9,
		10,
		11,
		13,
		18,
		31
	};
	private static final int[] DEFINED_BITS_OF_PRECISION_POINT_ONLY = new int[] {
		0,
		31
	};
	protected static final NumericDimensionField[] SPATIAL_DIMENSIONS = new NumericDimensionField[] {
		new LongitudeField(),
		new LatitudeField(
				true)
	// just use the same range for latitude to make square sfc values in
	// decimal degrees (EPSG:4326)
	};

	@Override
	public String getDimensionalityTypeName() {
		return "spatial";
	}

	@Override
	public String getDimensionalityTypeDescription() {
		return "This dimensionality type matches all indices that only require Geometry.";
	}

	@Override
	public int getPriority() {
		// arbitrary - just higher than spatial temporal so that the default
		// will be spatial over spatial-temporal
		return 10;
	}

	@Override
	public Object getOptions() {
		return options;
	}

	@Override
	public PrimaryIndex createPrimaryIndex() {
		// TODO when we have stats to determine which tiers are filled, we can
		// use the full incremental tiering
		// return new PrimaryIndex(
		// TieredSFCIndexFactory.createFullIncrementalTieredStrategy(
		// SPATIAL_DIMENSIONS,
		// new int[] {
		// LONGITUDE_BITS,
		// LATITUDE_BITS
		// },
		// SFCType.HILBERT),
		// new BasicIndexModel(
		// new NumericDimensionField[] {
		// new LongitudeField(),
		// new LatitudeField(
		// true)
		// }));

		// but for now use predefined tiers to limit the query decomposition in
		// cases where we are unlikely to use certain tiers
		return new PrimaryIndex(
				TieredSFCIndexFactory.createDefinedPrecisionTieredStrategy(
						SPATIAL_DIMENSIONS,
						options.pointOnly ? new int[][] {
							DEFINED_BITS_OF_PRECISION_POINT_ONLY.clone(),
							DEFINED_BITS_OF_PRECISION_POINT_ONLY.clone()
						} : new int[][] {
							DEFINED_BITS_OF_PRECISION.clone(),
							DEFINED_BITS_OF_PRECISION.clone()
						},
						SFCType.HILBERT),
				new BasicIndexModel(
						new NumericDimensionField[] {
							new LongitudeField(),
							new LatitudeField(
									true)
						}));
	}

	private static class SpatialOptions
	{
		@Parameter(names = {
			"pointOnly"
		}, required = false, description = "The index will only be good at handling pointsand will not be optimized for handling lines/polys.  The default behavior is to handle any geometry.")
		protected boolean pointOnly = false;
	}

	@Override
	public Class<? extends CommonIndexValue>[] getRequiredIndexTypes() {
		return new Class[] {
			GeometryWrapper.class
		};
	}

}
