package mil.nga.giat.geowave.adapter.vector.index;

import java.io.IOException;
import java.util.NoSuchElementException;

import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;

public class ChooseBestMatchIndexQueryStrategy implements
		IndexQueryStrategySPI
{
	public static final String NAME = "Best Match";

	@Override
	public String toString() {
		return NAME;
	}

	@Override
	public CloseableIterator<Index<?, ?>> getIndices(
			final Constraints timeConstraints,
			final Constraints geoConstraints,
			final CloseableIterator<Index<?, ?>> indices ) {
		return new CloseableIterator<Index<?, ?>>() {
			PrimaryIndex nextIdx = null;
			boolean done = false;

			@Override
			public boolean hasNext() {
				while (!done && indices.hasNext()) {
					final Index<?, ?> nextChoosenIdx = indices.next();
					if (nextChoosenIdx instanceof PrimaryIndex) {
						nextIdx = (PrimaryIndex) nextChoosenIdx;
						if (!timeConstraints.isEmpty() && isSpatialTemporal(nextIdx)) {
							break;
						}

						if (timeConstraints.isEmpty() && isSpatial(nextIdx)) {
							break;
						}
					}
				}
				done = true;
				return nextIdx != null;
			}

			@Override
			public Index<?, ?> next()
					throws NoSuchElementException {
				if (nextIdx == null) {
					throw new NoSuchElementException();
				}
				final Index<?, ?> returnVal = nextIdx;
				nextIdx = null;
				return returnVal;
			}

			@Override
			public void remove() {}

			@Override
			public void close()
					throws IOException {
				indices.close();
			}
		};
	}

	private static boolean isSpatialTemporal(
			final PrimaryIndex index ) {
		if ((index == null) || (index.getIndexStrategy() == null) || (index.getIndexStrategy().getOrderedDimensionDefinitions() == null)) {
			return false;
		}
		final NumericDimensionDefinition[] dimensions = index.getIndexStrategy().getOrderedDimensionDefinitions();
		if (dimensions.length != 3) {
			return false;
		}
		boolean hasLat = false, hasLon = false, hasTime = false;
		for (final NumericDimensionDefinition definition : dimensions) {
			if (definition instanceof TimeDefinition) {
				hasTime = true;
			}
			else if (definition instanceof LatitudeDefinition) {
				hasLat = true;
			}
			else if (definition instanceof LongitudeDefinition) {
				hasLon = true;
			}
		}
		return hasTime && hasLat && hasLon;
	}

	private static boolean isSpatial(
			final PrimaryIndex index ) {
		if ((index == null) || (index.getIndexStrategy() == null) || (index.getIndexStrategy().getOrderedDimensionDefinitions() == null)) {
			return false;
		}
		final NumericDimensionDefinition[] dimensions = index.getIndexStrategy().getOrderedDimensionDefinitions();
		if (dimensions.length != 2) {
			return false;
		}
		boolean hasLat = false, hasLon = false;
		for (final NumericDimensionDefinition definition : dimensions) {
			if (definition instanceof LatitudeDefinition) {
				hasLat = true;
			}
			else if (definition instanceof LongitudeDefinition) {
				hasLon = true;
			}
		}
		return hasLat && hasLon;
	}
}
