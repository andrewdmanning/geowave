/**
 *
 */
package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseCloseableIteratorWrapper;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseCloseableIteratorWrapper.ScannerClosableWrapper;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseEntryIteratorWrapper;

/**
 * @author viggy Functionality similar to
 *         <code> AccumuloFilteredIndexQuery </code>
 */
public abstract class HBaseFilteredIndexQuery extends
		HBaseQuery
{

	protected final ScanCallback<?> scanCallback;
	protected List<QueryFilter> clientFilters;
	private final static Logger LOGGER = Logger.getLogger(
			HBaseFilteredIndexQuery.class);
	private Collection<String> fieldIds = null;

	public HBaseFilteredIndexQuery(
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final ScanCallback<?> scanCallback,
			final String... authorizations ) {
		super(
				adapterIds,
				index,
				authorizations);
		this.scanCallback = scanCallback;
	}

	protected void setClientFilters(
			final List<QueryFilter> clientFilters ) {
		this.clientFilters = clientFilters;
	}

	public void setFieldIds(
			final Collection<String> fieldIds ) {
		this.fieldIds = fieldIds;
	}

	public CloseableIterator<?> query(
			final BasicHBaseOperations operations,
			final AdapterStore adapterStore,
			final Integer limit ) {
		return query(
				operations,
				adapterStore,
				limit,
				false);
	}

	@SuppressWarnings("rawtypes")
	public CloseableIterator<?> query(
			final BasicHBaseOperations operations,
			final AdapterStore adapterStore,
			final Integer limit,
			final boolean withKeys ) {
		try {
			if (!operations.tableExists(
					StringUtils.stringFromBinary(
							index.getId().getBytes()))) {
				LOGGER.warn(
						"Table does not exist " + StringUtils.stringFromBinary(
								index.getId().getBytes()));
				return new CloseableIterator.Empty();
			}
		}
		catch (final IOException ex) {
			LOGGER.warn(
					"Unabe to check if " + StringUtils.stringFromBinary(
							index.getId().getBytes()) + " table exists");
			return new CloseableIterator.Empty();
		}
		final String tableName = StringUtils.stringFromBinary(
				index.getId().getBytes());
		final Scan scanner = getScanner(
				limit);

		final List<Filter> distributableFilters = getDistributableFilter();
		if ((distributableFilters != null) && (distributableFilters.size() > 0)) {
			final FilterList filterList = new FilterList();
			for (final Filter filter : distributableFilters) {
				filterList.addFilter(
						filter);
			}
			scanner.setFilter(
					filterList);
		}

		// a subset of fieldIds is being requested
		if ((fieldIds != null) && !fieldIds.isEmpty()) {
			// configure scanner to fetch only the fieldIds specified
			handleSubsetOfFieldIds(
					scanner,
					adapterStore.getAdapters());
		}

		ResultScanner results = null;
		try {
			results = operations.getScannedResults(
					scanner,
					tableName);

			if (results.iterator().hasNext()) {
				Iterator it = initIterator(
						adapterStore,
						results.iterator());
				if ((limit != null) && (limit > 0)) {
					it = Iterators.limit(
							it,
							limit);
				}
				return new HBaseCloseableIteratorWrapper(
						new ScannerClosableWrapper(
								results),
						it);
			}
			else {
				LOGGER.error(
						"Results were empty");
				return null;
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"Could not get the results from scanner");
			return null;
		}
	}

	protected abstract List<Filter> getDistributableFilter();

	private Scan getScanner(
			final Integer limit ) {
		final List<ByteArrayRange> ranges = getRanges();
		final Scan scanner = new Scan();
		if ((ranges != null) && (ranges.size() == 1)) {
			final ByteArrayRange r = ranges.get(
					0);
			scanner.setStartRow(
					r.getStart().getBytes());
			if (!r.isSingleValue()) {
				scanner.setStopRow(
						r.getEnd().getBytes());
			}

			if ((limit != null) && (limit > 0) && (limit < scanner.getBatch())) {
				scanner.setBatch(
						limit);
			}
		}

		if ((adapterIds != null) && !adapterIds.isEmpty()) {
			for (final ByteArrayId adapterId : adapterIds) {
				scanner.addFamily(
						adapterId.getBytes());
			}
		}
		return scanner;

	}

	private void handleSubsetOfFieldIds(
			final Scan scanner,
			final CloseableIterator<DataAdapter<?>> dataAdapters ) {

		final Set<ByteArrayId> uniqueDimensions = new HashSet<>();
		for (final NumericDimensionField<? extends CommonIndexValue> dimension : index.getIndexModel().getDimensions()) {
			uniqueDimensions.add(
					dimension.getFieldId());
		}

		while (dataAdapters.hasNext()) {

			// dimension fields must be included
			final DataAdapter<?> next = dataAdapters.next();
			for (final ByteArrayId dimension : uniqueDimensions) {
				scanner.addColumn(
						next.getAdapterId().getBytes(),
						dimension.getBytes());
			}

			// configure scanner to fetch only the specified fieldIds
			for (final String fieldId : fieldIds) {
				scanner.addColumn(
						next.getAdapterId().getBytes(),
						StringUtils.stringToBinary(
								fieldId));
			}
		}

		try {
			dataAdapters.close();
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to close iterator",
					e);
		}
	}

	protected Iterator initIterator(
			final AdapterStore adapterStore,
			final Iterator<Result> iterator ) {
		// TODO Fix #406 Since currently we are not supporting server side
		// iterator/coprocessors, we also cannot run
		// server side filters and hence they have to run on clients itself. So
		// need to add server side filters also in list of client filters.
		final List<QueryFilter> filters = getAllFiltersList();
		return new HBaseEntryIteratorWrapper(
				adapterStore,
				index,
				iterator,
				new mil.nga.giat.geowave.core.store.filter.FilterList<QueryFilter>(
						filters),
				scanCallback);
	}

	protected List<QueryFilter> getAllFiltersList() {
		// This method is so that it can be overridden to also add distributed
		// filter list
		final List<QueryFilter> filters = new ArrayList<QueryFilter>();
		filters.addAll(
				clientFilters);
		return filters;
	}
}
