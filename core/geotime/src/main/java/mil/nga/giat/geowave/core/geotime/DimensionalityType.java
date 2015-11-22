package mil.nga.giat.geowave.core.geotime;

import mil.nga.giat.geowave.core.geotime.index.NumericIndexStrategyFactory;
import mil.nga.giat.geowave.core.geotime.index.NumericIndexStrategyFactory.SpatialFactory;
import mil.nga.giat.geowave.core.geotime.index.NumericIndexStrategyFactory.SpatialTemporalFactory;
import mil.nga.giat.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import mil.nga.giat.geowave.core.geotime.store.dimension.LatitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.LongitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.TimeField;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.BasicIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public enum DimensionalityType {
	SPATIAL(
			new BasicIndexModel(
					new NumericDimensionField[] {
						new LongitudeField(),
						new LatitudeField()
					}),
			new SpatialFactory()),
	SPATIAL_TEMPORAL_YEAR(
			new BasicIndexModel(
					new NumericDimensionField[] {
						new LongitudeField(),
						new LatitudeField(),
						new TimeField(
								Unit.YEAR)
					}),
			new SpatialTemporalFactory(
					Unit.YEAR)),
	SPATIAL_TEMPORAL_MONTH(
			new BasicIndexModel(
					new NumericDimensionField[] {
						new LongitudeField(),
						new LatitudeField(),
						new TimeField(
								Unit.MONTH)
					}),
			new SpatialTemporalFactory(
					Unit.MONTH)),
	SPATIAL_TEMPORAL_DAY(
			new BasicIndexModel(
					new NumericDimensionField[] {
						new LongitudeField(),
						new LatitudeField(),
						new TimeField(
								Unit.DAY)
					}),
			new SpatialTemporalFactory(
					Unit.DAY)),
	OTHER(
			null,
			null);
	private final CommonIndexModel defaultIndexModel;
	private final NumericIndexStrategyFactory indexStrategyFactory;

	private DimensionalityType(
			final CommonIndexModel defaultIndexModel,
			final NumericIndexStrategyFactory indexStrategyFactory ) {
		this.defaultIndexModel = defaultIndexModel;
		this.indexStrategyFactory = indexStrategyFactory;
	}

	public CommonIndexModel getDefaultIndexModel() {
		return defaultIndexModel;
	}

	public NumericIndexStrategyFactory getIndexStrategyFactory() {
		return indexStrategyFactory;
	}

	public boolean isCompatible(
			final PrimaryIndex index ) {
		if ((index == null) || (index.getIndexStrategy() == null) || (indexStrategyFactory == null)) {
			return false;
		}
		final NumericDimensionDefinition[] factoryDimensions = indexStrategyFactory.getFactoryDefinition();
		final NumericDimensionDefinition[] indexDimensions = index.getIndexStrategy().getOrderedDimensionDefinitions();
		if (indexDimensions.length != factoryDimensions.length) {
			return false;
		}
		for (final NumericDimensionDefinition fd : factoryDimensions) {
			boolean dimensionFound = false;
			for (final NumericDimensionDefinition id : indexDimensions) {
				if (fd.isCompatibleDefinition(id)) {
					dimensionFound = true;
				}
			}
			if (!dimensionFound) {
				return false;
			}
		}
		return true;

	}

}
