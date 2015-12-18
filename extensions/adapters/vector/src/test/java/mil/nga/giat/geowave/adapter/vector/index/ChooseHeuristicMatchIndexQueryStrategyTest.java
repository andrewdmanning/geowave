package mil.nga.giat.geowave.adapter.vector.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.BasicQuery.ConstraintData;
import mil.nga.giat.geowave.core.store.query.BasicQuery.ConstraintSet;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;

import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

public class ChooseHeuristicMatchIndexQueryStrategyTest
{

	@Test
	public void testChooseTemporalWithoutStats() {
		ChooseHeuristicMatchIndexQueryStrategy strategy = new ChooseHeuristicMatchIndexQueryStrategy();

		final ConstraintSet cs1 = new ConstraintSet();
		cs1.addConstraint(
				LatitudeDefinition.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								0.3,
								0.5),
						true));

		cs1.addConstraint(
				LongitudeDefinition.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								0.4,
								0.7),
						true));

		final ConstraintSet cs2a = new ConstraintSet();
		cs2a.addConstraint(
				TimeDefinition.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								0.1,
								0.2),
						true));

		final Constraints constraints = new Constraints(
				Arrays.asList(cs2a)).merge(Collections.singletonList(cs1));

		final BasicQuery query = new BasicQuery(
				constraints);

		Iterator<Index<?, ?>> it = getIndices(
				new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>(),
				query,
				strategy);
		assertTrue(it.hasNext());
		assertEquals(
				"SPATIAL_TEMPORAL_VECTOR_IDX",
				it.next().getId().getString());
		assertFalse(it.hasNext());

	}

	@Test
	public void testChooseSpatialWithoutStats() {
		ChooseHeuristicMatchIndexQueryStrategy strategy = new ChooseHeuristicMatchIndexQueryStrategy();

		final ConstraintSet cs1 = new ConstraintSet();
		cs1.addConstraint(
				LatitudeDefinition.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								0.3,
								0.5),
						true));

		cs1.addConstraint(
				LongitudeDefinition.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								0.4,
								0.7),
						true));

		final ConstraintSet cs2a = new ConstraintSet();
		cs2a.addConstraint(
				TimeDefinition.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								1,
								86400000),
						true));

		final Constraints constraints = new Constraints(
				Arrays.asList(cs2a)).merge(Collections.singletonList(cs1));

		final BasicQuery query = new BasicQuery(
				constraints);

		Iterator<Index<?, ?>> it = getIndices(
				new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>(),
				query,
				strategy);
		assertTrue(it.hasNext());
		assertEquals(
				"SPATIAL_VECTOR_IDX",
				it.next().getId().getString());
		assertFalse(it.hasNext());

	}

	public Iterator<Index<?, ?>> getIndices(
			final Map<ByteArrayId, DataStatistics<SimpleFeature>> stats,
			final BasicQuery query,
			final ChooseHeuristicMatchIndexQueryStrategy strategy ) {
		return strategy.getIndices(
				stats,
				query,
				new CloseableIteratorWrapper(
						new Closeable() {
							@Override
							public void close()
									throws IOException {

							}
						},
						Arrays.asList(
								IndexType.SPATIAL_TEMPORAL_VECTOR.createDefaultIndex(),
								IndexType.SPATIAL_VECTOR.createDefaultIndex()).iterator()));
	}

	public static class ConstrainedIndexValue extends
			NumericRange implements
			CommonIndexValue
	{

		/**
 *
 */
		private static final long serialVersionUID = 1L;

		public ConstrainedIndexValue(
				final double min,
				final double max ) {
			super(
					min,
					max);
			//
		}

		@Override
		public byte[] getVisibility() {
			return new byte[0];
		}

		@Override
		public void setVisibility(
				final byte[] visibility ) {

		}

		@Override
		public boolean overlaps(
				final NumericDimensionField[] field,
				final NumericData[] rangeData ) {
			return false;
		}

	}
}
