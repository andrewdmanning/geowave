package mil.nga.giat.geowave.adapter.vector.query.cql;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.GtFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.plugin.ExtractGeometryFilterVisitor;
import mil.nga.giat.geowave.adapter.vector.plugin.ExtractTimeFilterVisitor;
import mil.nga.giat.geowave.adapter.vector.util.QueryIndexHelper;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.store.filter.SpatialQueryFilter.CompareOperation;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialTemporalQuery;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalConstraints;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalConstraintsSet;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalQuery;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.Query;

import org.apache.log4j.Logger;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

import com.vividsolutions.jts.geom.Geometry;

public class CQLQuery implements
		DistributableQuery
{
	private final static Logger LOGGER = Logger.getLogger(CQLQuery.class);
	private Query baseQuery;
	private CQLQueryFilter filter;
	private Filter cqlFilter;

	protected CQLQuery() {}

	public CQLQuery(
			final String cql,
			final DataAdapter<SimpleFeature> adapter )
			throws CQLException {
		this(
				cql,
				CompareOperation.OVERLAPS,
				adapter);
	}

	public CQLQuery(
			final String cql,
			final CompareOperation geoCompareOp,
			final DataAdapter<SimpleFeature> adapter )
			throws CQLException {
		cqlFilter = CQL.toFilter(cql);
		this.filter = new CQLQueryFilter(
				cqlFilter,
				adapter);
		final Geometry geometry = ExtractGeometryFilterVisitor.getConstraints(
				cqlFilter,
				((GtFeatureDataAdapter) adapter).getType().getCoordinateReferenceSystem());
		final TemporalConstraintsSet timeConstraintSet = new ExtractTimeFilterVisitor().getConstraints(cqlFilter);

		// determine which time constraints are associated with an indexable
		// field
		final TemporalConstraints temporalConstraints = QueryIndexHelper.getTemporalConstraintsForDescriptors(
				((GtFeatureDataAdapter) adapter).getTimeDescriptors(),
				timeConstraintSet);
		// convert to constraints
		final Constraints timeConstraints = SpatialTemporalQuery.createConstraints(
				temporalConstraints,
				false);
		if (geometry != null) {
			Constraints constraints = GeometryUtils.basicConstraintsFromGeometry(geometry);

			if (timeConstraintSet != null && !timeConstraintSet.isEmpty()) {
				constraints = constraints.merge(timeConstraints);
			}
			baseQuery = new SpatialQuery(
					constraints,
					geometry,
					geoCompareOp);
		}
		else if (timeConstraintSet != null && !timeConstraintSet.isEmpty()) {
			baseQuery = new TemporalQuery(
					temporalConstraints);
		}
		// default case is to leave base query null.
	}

	public CQLQuery(
			final Query baseQuery,
			final Filter filter,
			final DataAdapter<SimpleFeature> adapter ) {
		this.baseQuery = baseQuery;
		this.cqlFilter = filter;
		this.filter = new CQLQueryFilter(
				filter,
				adapter);
	}

	@Override
	public List<QueryFilter> createFilters(
			final CommonIndexModel indexModel ) {
		List<QueryFilter> queryFilters;
		// if (baseQuery != null) {
		// queryFilters = baseQuery.createFilters(indexModel);
		// }
		// else {
		queryFilters = new ArrayList<QueryFilter>();
		// }
		if (filter != null) {
			queryFilters = new ArrayList<QueryFilter>(
					queryFilters);
			queryFilters.add(filter);
		}
		return queryFilters;
	}

	@Override
	public boolean isSupported(
			final Index<?, ?> index ) {
		if (baseQuery != null) {
			return baseQuery.isSupported(index);
		}
		// TODO: this should interrogate strategy to see if constraint are
		// processed by stratey
		return true;
	}

	@Override
	public List<MultiDimensionalNumericData> getIndexConstraints(
			final NumericIndexStrategy indexStrategy ) {
		if (baseQuery != null) {
			return baseQuery.getIndexConstraints(indexStrategy);
		}
		return Collections.emptyList();
	}

	@Override
	public byte[] toBinary() {
		byte[] baseQueryBytes;
		if (baseQuery != null) {
			if (!(baseQuery instanceof DistributableQuery)) {
				throw new IllegalArgumentException(
						"Cannot distribute CQL query with base query of type '" + baseQuery.getClass() + "'");
			}
			else {
				baseQueryBytes = PersistenceUtils.toBinary((DistributableQuery) baseQuery);
			}
		}
		else {
			// base query can be null, no reason to log a warning
			baseQueryBytes = new byte[] {};
		}
		final byte[] filterBytes;
		if (filter != null) {
			filterBytes = filter.toBinary();
		}
		else {
			LOGGER.warn("Filter is null");
			filterBytes = new byte[] {};
		}

		final ByteBuffer buf = ByteBuffer.allocate(filterBytes.length + baseQueryBytes.length + 4);
		buf.putInt(filterBytes.length);
		buf.put(filterBytes);
		buf.put(baseQueryBytes);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int filterBytesLength = buf.getInt();
		final int baseQueryBytesLength = bytes.length - filterBytesLength - 4;
		if (filterBytesLength > 0) {
			final byte[] filterBytes = new byte[filterBytesLength];

			filter = new CQLQueryFilter();
			filter.fromBinary(filterBytes);
		}
		else {
			LOGGER.warn("CQL filter is empty bytes");
			filter = null;
		}
		if (baseQueryBytesLength > 0) {
			final byte[] baseQueryBytes = new byte[baseQueryBytesLength];

			try {
				baseQuery = PersistenceUtils.fromBinary(
						baseQueryBytes,
						DistributableQuery.class);
			}
			catch (final Exception e) {
				throw new IllegalArgumentException(
						e);
			}
		}
		else {
			// base query can be null, no reason to log a warning
			baseQuery = null;
		}
	}

	@Override
	public List<ByteArrayRange> getSecondaryIndexConstraints(
			SecondaryIndex<?> index ) {
		PropertyFilterVisitor visitor = new PropertyFilterVisitor();
		PropertyConstraintSet constraints = (PropertyConstraintSet) cqlFilter.accept(
				visitor,
				null);
		return constraints.getRangesFor(index);
	}

	@Override
	public List<DistributableQueryFilter> getSecondaryQueryFilter(
			SecondaryIndex<?> index ) {
		PropertyFilterVisitor visitor = new PropertyFilterVisitor();
		PropertyConstraintSet constraints = (PropertyConstraintSet) cqlFilter.accept(
				visitor,
				null);
		return constraints.getFiltersFor(index);
	}
}
