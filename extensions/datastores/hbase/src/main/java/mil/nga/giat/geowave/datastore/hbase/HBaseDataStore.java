/**
 *
 */
package mil.nga.giat.geowave.datastore.hbase;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CastIterator;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatsCompositionTool;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.core.store.memory.MemoryAdapterStore;
import mil.nga.giat.geowave.core.store.query.AdapterIdQuery;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.PrefixIdQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.hbase.entities.HBaseRowId;
import mil.nga.giat.geowave.datastore.hbase.index.secondary.HBaseSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseAdapterStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseDataStatisticsStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseIndexStore;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.query.HBaseConstraintsQuery;
import mil.nga.giat.geowave.datastore.hbase.query.HBaseFilteredIndexQuery;
import mil.nga.giat.geowave.datastore.hbase.query.SingleEntryFilter;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;
import mil.nga.giat.geowave.mapreduce.MapReduceDataStore;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

/**
 * @author viggy Functionality similar to <code> AccumuloDataStore </code>
 */
public class HBaseDataStore implements
		MapReduceDataStore
{

	private final static Logger LOGGER = Logger.getLogger(HBaseDataStore.class);
	private final IndexStore indexStore;
	private final AdapterStore adapterStore;
	private final BasicHBaseOperations operations;
	private final DataStatisticsStore statisticsStore;
	protected final SecondaryIndexDataStore secondaryIndexDataStore;
	protected final HBaseOptions options;

	public HBaseDataStore(
			final BasicHBaseOperations operations ) {
		this(
				new HBaseIndexStore(
						operations),
				new HBaseAdapterStore(
						operations),
				new HBaseDataStatisticsStore(
						operations),
				operations);
	}

	public HBaseDataStore(
			final BasicHBaseOperations operations,
			final HBaseOptions options ) {
		this(
				new HBaseIndexStore(
						operations),
				new HBaseAdapterStore(
						operations),
				new HBaseDataStatisticsStore(
						operations),
				new HBaseSecondaryIndexDataStore(
						operations,
						options),
				operations,
				options);
	}

	public HBaseDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final BasicHBaseOperations operations ) {
		// TODO Fix all constructor calls to pass secondary index store and get rid of this constructor
		this(
				indexStore,
				adapterStore,
				statisticsStore,
				null,
				operations,
				new HBaseOptions());
	}

	public HBaseDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final BasicHBaseOperations operations,
			final HBaseOptions options ) {
		// TODO Fix all constructor calls to pass secondary index store and get rid of this constructor
		this(
				indexStore,
				adapterStore,
				statisticsStore,
				null,
				operations,
				options);
	}

	public HBaseDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final SecondaryIndexDataStore secondaryIndexDataStore,
			final BasicHBaseOperations operations ) {
		this(
				indexStore,
				adapterStore,
				statisticsStore,
				secondaryIndexDataStore,
				operations,
				new HBaseOptions());
	}

	public HBaseDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final SecondaryIndexDataStore secondaryIndexDataStore,
			final BasicHBaseOperations operations,
			final HBaseOptions options ) {
		this.indexStore = indexStore;
		this.adapterStore = adapterStore;
		this.statisticsStore = statisticsStore;
		this.secondaryIndexDataStore = secondaryIndexDataStore;
		this.operations = operations;
		this.options = options;
	}

	@Override
	public <T> IndexWriter createIndexWriter(
			final PrimaryIndex index,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		return new HBaseIndexWriter(
				index,
				operations,
				options,
				this,
				customFieldVisibilityWriter);
	}

	public CloseableIterator<?> query(
			final HBaseFilteredIndexQuery query ) {
		return query.query(
				operations,
				adapterStore,
				0);
	}

	private boolean deleteAltIndexEntry(
			final String tableName,
			final ByteArrayId dataId,
			final ByteArrayId adapterId ) {
		final boolean success = true;
		// TODO #406 Need to Fix this later, currently have not coded it
		LOGGER.warn("This is not implemeted yet. Need to fix");

		return success;
	}

	private boolean deleteRowsForSingleEntry(
			final String tableName,
			final List<KeyValue> rows,
			final DeleteRowObserver deleteRowObserver,
			final String... authorizations ) {

		try {
			final HBaseWriter deleter = operations.createWriter(
					tableName,
					"",
					false);
			for (final KeyValue rowData : rows) {
				final byte[] id = rowData.getRow();
				final Delete d = new Delete(
						id);
				deleter.delete(d);
			}
			return true;
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to delete row from table [" + tableName + "].",
					e);
			return false;
		}

	}

	private DeleteRowObserver createDecodingDeleteObserver(
			final StatsCompositionTool<Object> stats,
			final DataAdapter<Object> adapter,
			final PrimaryIndex index ) {

		return stats.isPersisting() ? new DeleteRowObserver() {
			// many rows can be associated with one entry.
			// need a control to delete only one.
			boolean foundOne = false;

			@Override
			public void deleteRow(
					final KeyValue keyValue ) {
				if (!foundOne) {
					final HBaseRowId rowId = new HBaseRowId(
							keyValue.getRow());
					final List<KeyValue> list = new ArrayList<KeyValue>();
					list.add(keyValue);
					final Result r = new Result(
							list);
					HBaseUtils.decodeRow(
							r,
							rowId,
							adapter,
							null,
							null,
							index,
							new ScanCallback<Object>() {

								@Override
								public void entryScanned(
										final DataStoreEntryInfo entryInfo,
										final Object entry ) {
									stats.entryDeleted(
											entryInfo,
											entry);
								}

							});

				}
				foundOne = true;
			}
		} : null;
	}

	private interface DeleteRowObserver
	{
		public void deleteRow(
				KeyValue keyValye );
	}

	private List<KeyValue> getEntryRowWithRowIds(
			final String tableName,
			final List<ByteArrayId> rowIds,
			final ByteArrayId adapterId,
			final String... authorizations ) {

		final List<KeyValue> resultList = new ArrayList<KeyValue>();
		if ((rowIds == null) || rowIds.isEmpty()) {
			return resultList;
		}
		/*
		 * final List<ByteArrayRange> ranges = new ArrayList<ByteArrayRange>();
		 * for (final ByteArrayId row : rowIds) { ranges.add(new ByteArrayRange(
		 * row, row)); }
		 */
		try {
			final Scan scanner = new Scan();
			scanner.setStartRow(rowIds.get(
					0).getBytes());
			final ResultScanner results = operations.getScannedResults(
					scanner,
					tableName);
			/*
			 * ((BatchScanner) scanner).setRanges(HBaseUtils.
			 * byteArrayRangesToAccumuloRanges(ranges));
			 */
			/*
			 * final IteratorSetting iteratorSettings = new IteratorSetting(
			 * QueryFilterIterator.WHOLE_ROW_ITERATOR_PRIORITY,
			 * QueryFilterIterator.WHOLE_ROW_ITERATOR_NAME,
			 * WholeRowIterator.class);
			 * scanner.addScanIterator(iteratorSettings);
			 */

			final Iterator<Result> iterator = results.iterator();
			while (iterator.hasNext()) {
				final Cell cell = iterator.next().listCells().get(
						0);
				resultList.add(new KeyValue(
						cell));
			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to query table '" + tableName + "'.  Table does not exist.",
					e);
		}

		return resultList;
	}

	private List<KeyValue> getEntryRows(
			final String tableName,
			final ByteArrayId dataId,
			final ByteArrayId adapterId,
			final int limit,
			final String... authorizations ) {

		/*
		 * final List<KeyValue> resultList = new ArrayList<KeyValue>(); Scan
		 * scanner = new Scan(); try { scanner.addFamily(adapterId.getBytes());
		 * ResultScanner results = operations.getScannedResults( scanner,
		 * tableName);
		 *
		 * #406 Need to see how to add these iterators to fine grain the
		 * results final IteratorSetting rowIteratorSettings = new
		 * IteratorSetting(
		 * SingleEntryFilterIterator.WHOLE_ROW_ITERATOR_PRIORITY,
		 * SingleEntryFilterIterator.WHOLE_ROW_ITERATOR_NAME,
		 * WholeRowIterator.class);
		 * scanner.addScanIterator(rowIteratorSettings);
		 *
		 * final IteratorSetting filterIteratorSettings = new IteratorSetting(
		 * SingleEntryFilterIterator.ENTRY_FILTER_ITERATOR_PRIORITY,
		 * SingleEntryFilterIterator.ENTRY_FILTER_ITERATOR_NAME,
		 * SingleEntryFilterIterator.class);
		 *
		 * filterIteratorSettings.addOption(
		 * SingleEntryFilterIterator.ADAPTER_ID,
		 * ByteArrayUtils.byteArrayToString(adapterId.getBytes()));
		 *
		 * filterIteratorSettings.addOption( SingleEntryFilterIterator.DATA_ID,
		 * ByteArrayUtils.byteArrayToString(dataId.getBytes()));
		 * scanner.addScanIterator(filterIteratorSettings);
		 *
		 * final Iterator<Result> iterator = results.iterator(); int i = 0;
		 * while (iterator.hasNext() && (i < limit)) { // FB supression as FB //
		 * not // detecting i reference // here Cell cell =
		 * iterator.next().listCells().get( 0); resultList.add(new KeyValue(
		 * cell)); i++; } } catch (final IOException e) { LOGGER.warn(
		 * "Unable to query table '" + tableName + "'. Table does not exist.",
		 * e); } return resultList;
		 */
		final List<KeyValue> resultList = new ArrayList<KeyValue>();
		final Scan scanner = new Scan();
		try {

			scanner.setFilter(new SingleEntryFilter(
					dataId.getBytes(),
					adapterId.getBytes()));
			final ResultScanner results = operations.getScannedResults(
					scanner,
					tableName);

			final Iterator<Result> iterator = results.iterator();
			int i = 0;
			while (iterator.hasNext() && (i < limit)) { // FB supression as FB
														// not
														// detecting i reference
														// here
				final Cell cell = iterator.next().listCells().get(
						0);
				resultList.add(new KeyValue(
						cell));
				i++;
			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to query table '" + tableName + "'.  Table does not exist.",
					e);
		}
		return resultList;
	}

	private List<ByteArrayId> getAltIndexRowIds(
			final String tableName,
			final ByteArrayId dataId,
			final ByteArrayId adapterId,
			final int limit ) {

		final List<ByteArrayId> result = new ArrayList<ByteArrayId>();
		try {
			if (options.isUseAltIndex() && operations.tableExists(tableName)) {
				final Scan scanner = new Scan();
				scanner.setStartRow(dataId.getBytes());
				scanner.setStopRow(dataId.getBytes());
				scanner.addFamily(adapterId.getBytes());

				final ResultScanner results = operations.getScannedResults(
						scanner,
						tableName);
				final Iterator<Result> iterator = results.iterator();
				int i = 0;
				while (iterator.hasNext() && (i < limit)) {
					result.add(new ByteArrayId(
							CellUtil.cloneQualifier(iterator.next().listCells().get(
									0))));
					i++;
				}

			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to query table '" + tableName + "'.  Table does not exist.",
					e);
		}

		return result;
	}

	private <T> StatsCompositionTool<T> getStatsCompositionTool(
			final DataAdapter<T> adapter ) {
		return new StatsCompositionTool<T>(
				adapter,
				options.isPersistDataStatistics() ? statisticsStore : null);
	}

	private <T> void synchronizeStatsWithStore(
			final StatsCompositionTool<T> compositionTool,
			final boolean commitStats ) {
		if (commitStats) {
			compositionTool.flush();
		}
		else {
			compositionTool.reset();
		}
	}

	public void store(
			final PrimaryIndex index ) {
		if (options.isPersistIndex() && !indexStore.indexExists(index.getId())) {
			indexStore.addIndex(index);
		}
	}

	protected synchronized void store(
			final DataAdapter<?> adapter ) {
		if (options.isPersistAdapter() && !adapterStore.adapterExists(adapter.getAdapterId())) {
			adapterStore.addAdapter(adapter);
		}
	}

	@Override
	public <T> CloseableIterator<T> query(
			final QueryOptions queryOptions,
			final Query query ) {

		final List<CloseableIterator<Object>> results = new ArrayList<CloseableIterator<Object>>();
		// all queries will use the same instance of the dedupe filter for
		// client side filtering because the filter needs to be applied across
		// indices
		final QueryOptions sanitizedQueryOptions = (queryOptions == null) ? new QueryOptions() : queryOptions;
		final Query sanitizedQuery = (query == null) ? new EverythingQuery() : query;

		int indexCount = 0;
		final DedupeFilter filter = new DedupeFilter();
		MemoryAdapterStore tempAdapterStore;
		try {
			tempAdapterStore = new MemoryAdapterStore(
					sanitizedQueryOptions.getAdaptersArray(adapterStore));

			try (CloseableIterator<Index<?, ?>> indexIt = sanitizedQueryOptions.getIndices(indexStore)) {
				while (indexIt.hasNext()) {
					final PrimaryIndex index = (PrimaryIndex) indexIt.next();
					indexCount++;
					// TODO add back in RowIdQuery support
//					if (sanitizedQuery instanceof RowIdQuery) {
//						final HBaseRowIdQuery q = new HBaseRowIdQuery(
//								sanitizedQueryOptions.getAdapterIds(adapterStore),
//								index,
//								((RowIdQuery) sanitizedQuery).getRowIds(),
//								(ScanCallback<Object>) sanitizedQueryOptions.getScanCallback(),
//								filter,
//								sanitizedQueryOptions.getAuthorizations());
//
//						results.add(q.query(
//								operations,
//								tempAdapterStore,
//								sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
//								-1));
//						continue;
//					}
//					else
						if (sanitizedQuery instanceof DataIdQuery) {
						final DataIdQuery idQuery = (DataIdQuery) sanitizedQuery;
						results.add(getEntries(
								index,
								idQuery.getDataIds(),
								(DataAdapter<Object>) adapterStore.getAdapter(idQuery.getAdapterId()),
								filter,
								(ScanCallback<Object>) sanitizedQueryOptions.getScanCallback(),
								sanitizedQueryOptions.getAuthorizations(),
								sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
								true));
						continue;
					}
					else if (sanitizedQuery instanceof PrefixIdQuery) {
//						final PrefixIdQuery prefixIdQuery = (PrefixIdQuery) sanitizedQuery;
//						final AccumuloRowPrefixQuery<Object> prefixQuery = new AccumuloRowPrefixQuery<Object>(
//								index,
//								prefixIdQuery.getRowPrefix(),
//								(ScanCallback<Object>) sanitizedQueryOptions.getScanCallback(),
//								sanitizedQueryOptions.getLimit(),
//								sanitizedQueryOptions.getAuthorizations());
//						results.add(prefixQuery.query(
//								accumuloOperations,
//								sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
//								tempAdapterStore));
						// TODO??
						continue;
					}
					else if (sanitizedQuery instanceof AdapterIdQuery) {
						// TODO Update constructor
//						final HBaseConstraintsQuery hbaseQuery = new HBaseConstraintsQuery(
//								Collections.singletonList(((AdapterIdQuery) sanitizedQuery).getAdapterId()),
//								index,
//								sanitizedQuery,
//								filter,
//								sanitizedQueryOptions.getScanCallback(),
//								queryOptions.getAggregation(),
//								sanitizedQueryOptions.getAuthorizations());
						final HBaseConstraintsQuery hbaseQuery = new HBaseConstraintsQuery(
								Collections.singletonList(((AdapterIdQuery) sanitizedQuery).getAdapterId()),
								index,
								filter,
								sanitizedQueryOptions.getScanCallback(),
								sanitizedQueryOptions.getAuthorizations());
						// TODO update query
//						results.add(hbaseQuery.query(
//								operations,
//								tempAdapterStore,
//								sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
//								sanitizedQueryOptions.getLimit(),
//								true));
						results.add(hbaseQuery.query(
								operations,
								tempAdapterStore,
								sanitizedQueryOptions.getLimit(),
								true));
						continue;

					}

					HBaseConstraintsQuery hbaseQuery;
					try {
						List<ByteArrayId> adapterIds = sanitizedQueryOptions.getAdapterIds(adapterStore);
						// only narrow adapter Ids if the set of adapter id's is
						// resolved
						try (CloseableIterator<DataAdapter<?>> adapters = sanitizedQueryOptions.getAdapters(getAdapterStore())) {
							adapterIds = ((adapterIds != null) && options.persistDataStatistics && (adapterIds.size() > 0)) ? DataStoreUtils.trimAdapterIdsByIndex(
									statisticsStore,
									index.getId(),
									adapters,
									sanitizedQueryOptions.getAuthorizations()) : adapterIds;
						}
						// the null case should not happen, but the findbugs
						// seems to like it.
						if ((adapterIds == null) || (adapterIds.size() > 0)) {
							// TODO Update constructor
//							hbaseQuery = new HBaseConstraintsQuery(
//									adapterIds,
//									index,
//									sanitizedQuery,
//									filter,
//									sanitizedQueryOptions.getScanCallback(),
//									queryOptions.getAggregation(),
//									sanitizedQueryOptions.getAuthorizations());
							hbaseQuery = new HBaseConstraintsQuery(
									adapterIds,
									index,
									filter,
									sanitizedQueryOptions.getScanCallback(),
									sanitizedQueryOptions.getAuthorizations());

							// TODO update query
//							results.add(hbaseQuery.query(
//									operations,
//									tempAdapterStore,
//									sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
//									sanitizedQueryOptions.getLimit(),
//									true));
							results.add(hbaseQuery.query(
									operations,
									tempAdapterStore,
									sanitizedQueryOptions.getLimit(),
									true));
						}
					}
					catch (final IOException e) {
						LOGGER.error("Cannot resolve adapter Ids " + sanitizedQueryOptions.toString());

					}
				}
			}

		}
		catch (final IOException e1) {
			LOGGER.error(
					"Failed to resolve adapter or index for query",
					e1);
		}

		if (sanitizedQueryOptions.isDedupAcrossIndices() && (indexCount > 1)) {
			filter.setDedupAcrossIndices(true);
		}

		return new CloseableIteratorWrapper<T>(
				new Closeable() {
					@Override
					public void close()
							throws IOException {
						for (final CloseableIterator<Object> result : results) {
							result.close();
						}
					}
				},
				Iterators.concat(new CastIterator<T>(
						results.iterator())));
	}










	private CloseableIterator<Object> getEntries(
			final PrimaryIndex index,
			final List<ByteArrayId> dataIds,
			final DataAdapter<Object> adapter,
			final DedupeFilter filter,
			final ScanCallback<Object> scanCallback,
			final String[] authorizations,
			final double[] maxResolutionSubsamplingPerDimension,
			final boolean b ) {
		// TODO Auto-generated method stub
		return null;
	}

	public AdapterStore getAdapterStore() {
		return adapterStore;
	}








	// OLD version of query()
//	{
//
//		// query the indices that are supported for this query object, and these
//		// data adapter Ids
//		final List<CloseableIterator<Object>> results = new ArrayList<CloseableIterator<Object>>();
//		int indexCount = 0;
//		// all queries will use the same instance of the dedupe filter for
//		// client side filtering because the filter needs to be applied across
//		// indices
//		final DedupeFilter clientDedupeFilter = new DedupeFilter();
//		while (indices.hasNext()) {
//			final PrimaryIndex index = indices.next();
//			final HBaseConstraintsQuery hbaseQuery;
//			if (query == null) {
//				hbaseQuery = new HBaseConstraintsQuery(
//						adapterIds,
//						index,
//						clientDedupeFilter,
//						scanCallback,
//						authorizations);
//			}
//			else if (query.isSupported(index)) {
//				// construct the query
//				hbaseQuery = new HBaseConstraintsQuery(
//						adapterIds,
//						index,
//						query.getIndexConstraints(index.getIndexStrategy()),
//						query.createFilters(index.getIndexModel()),
//						clientDedupeFilter,
//						scanCallback,
//						authorizations);
//			}
//			else {
//				continue;
//			}
//			if ((queryOptions != null) && (!queryOptions.getFieldIds().isEmpty())) {
//				// results should contain subset of fieldIds
//				hbaseQuery.setFieldIds(queryOptions.getFieldIds());
//			}
//			results.add(hbaseQuery.query(
//					operations,
//					adapterStore,
//					limit,
//					true));
//			indexCount++;
//		}
//		// if there aren't multiple indices, the client-side dedupe filter can
//		// just cache rows that are duplicated within the index and not
//		// everything
//		clientDedupeFilter.setMultiIndexSupportEnabled(indexCount > 1);
//
//		// concatenate iterators
//		return new CloseableIteratorWrapper<T>(
//				new Closeable() {
//					@Override
//					public void close()
//							throws IOException {
//						for (final CloseableIterator<Object> result : results) {
//							result.close();
//						}
//					}
//				},
//				Iterators.concat(new CastIterator<T>(
//						results.iterator())));
//	}

	@Override
	public boolean delete(
			final QueryOptions queryOptions,
			final Query query ) {
		// TODO
				return false;
	}

	@Override
	public RecordReader<GeoWaveInputKey, ?> createRecordReader(
			final DistributableQuery query,
			final QueryOptions queryOptions,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final IndexStore indexStore,
			final boolean isOutputWritable,
			final InputSplit inputSplit )
			throws IOException,
			InterruptedException {
		// TODO Implement HBase record reader
		LOGGER.error("This method createRecordReader9 is not yet coded. Need to fix it");
		return null;
	}

	@Override
	public List<InputSplit> getSplits(
			final DistributableQuery query,
			final QueryOptions queryOptions,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final IndexStore indexStore,
			final Integer minSplits,
			final Integer maxSplits )
			throws IOException,
			InterruptedException {
		// TODO Implement
		LOGGER.error("This method getSplits9 is not yet coded. Need to fix it");
		return null;
	}

//	@Override
//	public <T> List<ByteArrayId> ingest(
//			WritableDataAdapter<T> writableAdapter,
//			PrimaryIndex index,
//			T entry ) {
//		return this.ingest(
//				writableAdapter,
//				index,
//				entry,
//				new UniformVisibilityWriter<T>(
//						new UnconstrainedVisibilityHandler<T, Object>()));
//	}
//
//	@Override
//	public <T> void ingest(
//			WritableDataAdapter<T> writableAdapter,
//			PrimaryIndex index,
//			Iterator<T> entryIterator ) {
//		ingest(
//				writableAdapter,
//				index,
//				entryIterator,
//				null,
//				new UniformVisibilityWriter<T>(
//						new UnconstrainedVisibilityHandler<T, Object>()));
//	}
//
//	public <T> void ingest(
//			final WritableDataAdapter<T> writableAdapter,
//			final PrimaryIndex index,
//			Iterator<T> entryIterator,
//			IngestCallback<T> ingestCallback,
//			VisibilityWriter<T> customFieldVisibilityWriter ) {
//		if (writableAdapter instanceof IndexDependentDataAdapter) {
//			ingestInternal(
//					writableAdapter,
//					index,
//					new HBaseIteratorWrapper<T, T>(
//							entryIterator,
//							new Converter<T, T>() {
//
//								@Override
//								public Iterator<T> convert(
//										final T entry ) {
//									return ((IndexDependentDataAdapter) writableAdapter).convertToIndex(
//											index,
//											entry);
//								}
//							},
//							null),
//					ingestCallback,
//					customFieldVisibilityWriter);
//		}
//		else {
//			ingestInternal(
//					writableAdapter,
//					index,
//					entryIterator,
//					ingestCallback,
//					customFieldVisibilityWriter);
//		}
//	}

//	private <T> void ingestInternal(
//			final WritableDataAdapter<T> dataWriter,
//			final PrimaryIndex index,
//			final Iterator<T> entryIterator,
//			final IngestCallback<T> ingestCallback,
//			final VisibilityWriter<T> customFieldVisibilityWriter ) {
//		try {
//			store(dataWriter);
//			store(index);
//
//			final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
//			final String altIdxTableName = tableName + HBaseUtils.ALT_INDEX_TABLE;
//			// final byte[] adapterId = dataWriter.getAdapterId().getBytes();
//
//			boolean useAltIndex = options.isUseAltIndex();
//
//			if (useAltIndex) {
//				if (operations.tableExists(tableName)) {
//					if (!operations.tableExists(altIdxTableName)) {
//						useAltIndex = false;
//						LOGGER.warn("Requested alternate index table [" + altIdxTableName + "] does not exist.");
//					}
//				}
//				else {
//					if (operations.tableExists(altIdxTableName)) {
//						operations.deleteTable(altIdxTableName);
//						LOGGER.warn("Deleting current alternate index table [" + altIdxTableName + "] as main table does not yet exist.");
//					}
//				}
//			}
//
//			final String indexName = StringUtils.stringFromBinary(index.getId().getBytes());
//			HBaseWriter writer = operations.createWriter(
//					indexName,
//					dataWriter.getAdapterId().getString(),
//					options.isCreateTable());
//
//			/*
//			 * HBase doesnt support locality groups. Hence we will not use this.
//			 * if (options.isUseLocalityGroups() &&
//			 * !operations.localityGroupExists( tableName, adapterId)) {
//			 * operations.addLocalityGroup( tableName, adapterId); }
//			 */
//
//			final List<IngestCallback<T>> callbacks = new ArrayList<IngestCallback<T>>();
//
//			HBaseWriter altIdxWriter = null;
//			if (useAltIndex) {
//				altIdxWriter = operations.createWriter(
//						altIdxTableName,
//						dataWriter.getAdapterId().getString(),
//						options.isCreateTable());
//
//				callbacks.add(new HBaseAltIndexIngestCallback<T>(
//						altIdxWriter,
//						dataWriter));
//			}
//
//			final StatsCompositionTool<T> statsCompositionTool = this.getStatsCompositionTool(dataWriter);
//			callbacks.add(statsCompositionTool);
//
//			if (ingestCallback != null) {
//				callbacks.add(ingestCallback);
//			}
//			final IngestCallback<T> finalIngestCallback;
//			if (callbacks.size() > 1) {
//				finalIngestCallback = new IngestCallbackList<T>(
//						callbacks);
//			}
//			else if (callbacks.size() == 1) {
//				finalIngestCallback = callbacks.get(0);
//			}
//			else {
//				finalIngestCallback = null;
//			}
//
//			writer.write(
//					new Iterable<RowMutations>() {
//						@Override
//						public Iterator<RowMutations> iterator() {
//							return new HBaseIteratorWrapper<T, RowMutations>(
//									entryIterator,
//									new Converter<T, RowMutations>() {
//
//										@Override
//										public Iterator<RowMutations> convert(
//												final T entry ) {
//											return HBaseUtils.entryToMutations(
//													dataWriter,
//													index,
//													entry,
//													customFieldVisibilityWriter).iterator();
//										}
//									},
//									finalIngestCallback == null ? null : new Callback<T, RowMutations>() {
//
//										@Override
//										public void notifyIterationComplete(
//												final T entry ) {
//											finalIngestCallback.entryIngested(
//													HBaseUtils.getIngestInfo(
//															dataWriter,
//															index,
//															entry,
//															customFieldVisibilityWriter),
//													entry);
//										}
//									});
//						}
//					},
//					dataWriter.getAdapterId().getString());
//			writer.close();
//			if (useAltIndex && (altIdxWriter != null)) {
//				altIdxWriter.close();
//			}
//
//			synchronizeStatsWithStore(
//					statsCompositionTool,
//					true);
//		}
//		catch (IOException e) {
//			LOGGER.warn(
//					"Unable to create writer",
//					e);
//		}
//
//	}

//	@Override
//	public <T> List<ByteArrayId> ingest(
//			WritableDataAdapter<T> writableAdapter,
//			PrimaryIndex index,
//			T entry,
//			VisibilityWriter<T> customFieldVisibilityWriter ) {
//		if (writableAdapter instanceof IndexDependentDataAdapter) {
//			final IndexDependentDataAdapter adapter = ((IndexDependentDataAdapter) writableAdapter);
//			final Iterator<T> indexedEntries = adapter.convertToIndex(
//					index,
//					entry);
//			final List<ByteArrayId> rowIds = new ArrayList<ByteArrayId>();
//			while (indexedEntries.hasNext()) {
//				rowIds.addAll(ingestInternal(
//						adapter,
//						index,
//						indexedEntries.next(),
//						customFieldVisibilityWriter));
//			}
//			return rowIds;
//		}
//		else {
//			return ingestInternal(
//					writableAdapter,
//					index,
//					entry,
//					customFieldVisibilityWriter);
//		}
//	}

//	public <T> List<ByteArrayId> ingestInternal(
//			final WritableDataAdapter<T> writableAdapter,
//			final PrimaryIndex index,
//			final T entry,
//			final VisibilityWriter<T> customFieldVisibilityWriter ) {
//		store(writableAdapter);
//		store(index);
//
//		HBaseWriter writer = null;
//		StatsCompositionTool<T> statisticsTool = null;
//		try {
//			final String indexName = StringUtils.stringFromBinary(index.getId().getBytes());
//			final String altIdxTableName = indexName + HBaseUtils.ALT_INDEX_TABLE;
//			// final byte[] adapterId =
//			// writableAdapter.getAdapterId().getBytes();
//
//			boolean useAltIndex = options.isUseAltIndex();
//
//			if (useAltIndex) {
//				if (operations.tableExists(indexName)) {
//					if (!operations.tableExists(altIdxTableName)) {
//						useAltIndex = false;
//						LOGGER.warn("Requested alternate index table [" + altIdxTableName + "] does not exist.");
//					}
//				}
//				else {
//					if (operations.tableExists(altIdxTableName)) {
//						operations.deleteTable(altIdxTableName);
//						LOGGER.warn("Deleting current alternate index table [" + altIdxTableName + "] as main table does not yet exist.");
//					}
//				}
//			}
//
//			statisticsTool = getStatsCompositionTool(writableAdapter);
//
//			writer = operations.createWriter(
//					indexName,
//					writableAdapter.getAdapterId().getString(),
//					options.isCreateTable());
//			final DataStoreEntryInfo entryInfo = HBaseUtils.write(
//					writableAdapter,
//					index,
//					entry,
//					writer,
//					customFieldVisibilityWriter);
//
//			writer.close();
//			if (useAltIndex) {
//				final HBaseWriter altIdxWriter = operations.createWriter(
//						altIdxTableName,
//						writableAdapter.getAdapterId().getString(),
//						options.isCreateTable());
//
//				HBaseUtils.writeAltIndex(
//						writableAdapter,
//						entryInfo,
//						entry,
//						altIdxWriter);
//
//				altIdxWriter.close();
//			}
//			statisticsTool.entryIngested(
//					entryInfo,
//					entry);
//
//			synchronizeStatsWithStore(
//					statisticsTool,
//					true);
//
//			return entryInfo.getRowIds();
//		}
//		catch (IOException e) {
//			LOGGER.error(
//					"Unable to ingest data entry",
//					e);
//		}
//		finally {
//			try {
//				statisticsTool.close();
//			}
//			catch (Exception e) {
//				LOGGER.error("Unable to close statistics tool");
//			}
//		}
//		return new ArrayList<ByteArrayId>();
//	}

//	@Override
//	public <T> void ingest(
//			WritableDataAdapter<T> writableAdapter,
//			PrimaryIndex index,
//			Iterator<T> entryIterator,
//			IngestCallback<T> ingestCallback ) {
//		//  #406 Need to fix
//		LOGGER.error("This method ingest4 is not yet coded. Need to fix it");
//	}

//	@Override
//	public <T> T getEntry(
//			PrimaryIndex index,
//			ByteArrayId rowId ) {
//		//  #406 Need to fix
//		LOGGER.error("This method getEntry2 is not yet coded. Need to fix it");
//		return null;
//	}

//	@Override
//	public <T> T getEntry(
//			PrimaryIndex index,
//			ByteArrayId dataId,
//			ByteArrayId adapterId,
//			String... additionalAuthorizations ) {
//		final String altIdxTableName = index.getId().getString() + HBaseUtils.ALT_INDEX_TABLE;
//
//		try {
//			if (options.isUseAltIndex() && operations.tableExists(altIdxTableName)) {
//				final List<ByteArrayId> rowIds = getAltIndexRowIds(
//						altIdxTableName,
//						dataId,
//						adapterId,
//						1);
//
//				if (rowIds.size() > 0) {
//					final HBaseRowIdQuery q = new HBaseRowIdQuery(
//							index,
//							rowIds.get(0));
//					return (T) q.query(
//							operations,
//							adapterStore);
//				}
//			}
//			else {
//				final String tableName = index.getId().getString();
//				final List<KeyValue> rows = getEntryRows(
//						tableName,
//						dataId,
//						adapterId,
//						1,
//						additionalAuthorizations);
//				if (rows != null && rows.size() > 0) {
//					Result r = new Result(
//							rows);
//					return (T) HBaseUtils.decodeRow(
//							r,
//							adapterStore,
//							null,
//							index,
//							null);
//				}
//			}
//		}
//		catch (IOException e) {
//			LOGGER.warn("Table does not exists " + e);
//		}
//		return null;
//	}

//	@Override
//	public boolean deleteEntry(
//			PrimaryIndex index,
//			ByteArrayId dataId,
//			ByteArrayId adapterId,
//			String... authorizations ) {
//		final String tableName = index.getId().getString();
//		final String altIdxTableName = tableName + HBaseUtils.ALT_INDEX_TABLE;
//		boolean useAltIndex = options.isUseAltIndex();
//		try {
//			useAltIndex = useAltIndex && operations.tableExists(altIdxTableName);
//		}
//		catch (IOException e) {
//			LOGGER.warn("Couldnt not check if " + altIdxTableName + " exists");
//		}
//		@SuppressWarnings("unchecked")
//		final DataAdapter<Object> adapter = (DataAdapter<Object>) adapterStore.getAdapter(adapterId);
//
//		final List<KeyValue> rows = (useAltIndex) ? getEntryRowWithRowIds(
//				tableName,
//				getAltIndexRowIds(
//						altIdxTableName,
//						dataId,
//						adapterId,
//						Integer.MAX_VALUE),
//				adapterId,
//				authorizations) : getEntryRows(
//				tableName,
//				dataId,
//				adapterId,
//				Integer.MAX_VALUE,
//				authorizations);
//
//		final StatsCompositionTool<Object> statsCompositionTool = getStatsCompositionTool(adapter);
//		final boolean success = (rows.size() > 0) && deleteRowsForSingleEntry(
//				tableName,
//				rows,
//				createDecodingDeleteObserver(
//						statsCompositionTool,
//						adapter,
//						index),
//				authorizations);
//
//		synchronizeStatsWithStore(
//				statsCompositionTool,
//				success);
//		if (success && useAltIndex) {
//			deleteAltIndexEntry(
//					altIdxTableName,
//					dataId,
//					adapterId);
//		}
//
//		try {
//			// issue; going to call .flush() internally even if success = false;
//			statsCompositionTool.close();
//		}
//		catch (Exception ex) {
//			LOGGER.error(
//					"Error closing statsCompositionTool",
//					ex);
//		}
//		return success;
//	}

//	@Override
//	public <T> CloseableIterator<T> getEntriesByPrefix(
//			PrimaryIndex index,
//			ByteArrayId rowPrefix,
//			String... authorizations ) {
//		//  #406 Need to fix
//		LOGGER.error("This method getEntriesByPrefix3 is not yet coded. Need to fix it");
//		return null;
//	}

//	@Override
//	public <T> CloseableIterator<T> query(
//			DataAdapter<T> adapter,
//			PrimaryIndex index,
//			Query query,
//			int limit,
//			String... authorizations ) {
//		//  #406 Need to fix
//		LOGGER.error("This method query5 is not yet coded. Need to fix it");
//		return null;
//	}
//
//	@Override
//	public <T> CloseableIterator<T> query(
//			DataAdapter<T> adapter,
//			PrimaryIndex index,
//			Query query,
//			Integer limit,
//			ScanCallback<?> scanCallback,
//			String... authorizations ) {
//		//  #406 Need to fix
//		LOGGER.error("This method query6 is not yet coded. Need to fix it");
//		return null;
//	}

//	@Override
//	public CloseableIterator<?> query(
//			Query query,
//			String... authorizations ) {
//		return query(
//				(List<ByteArrayId>) null,
//				query,
//				authorizations);
//	}
//
//	@Override
//	public <T> CloseableIterator<T> query(
//			DataAdapter<T> adapter,
//			Query query,
//			String... additionalAuthorizations ) {
//		//  #406 Need to fix
//		LOGGER.error("This method query3c is not yet coded. Need to fix it");
//		return null;
//	}
//
//	@Override
//	public <T> CloseableIterator<T> query(
//			PrimaryIndex index,
//			Query query,
//			String... additionalAuthorizations ) {
//		return query(
//				index,
//				query,
//				null,
//				additionalAuthorizations);
//	}

//	@Override
//	public <T> CloseableIterator<T> query(
//			DataAdapter<T> adapter,
//			PrimaryIndex index,
//			Query query,
//			String... additionalAuthorizations ) {
//		//  #406 Need to fix
//		LOGGER.error("This method query4c is not yet coded. Need to fix it");
//		return null;
//	}
//
//	@Override
//	public CloseableIterator<?> query(
//			List<ByteArrayId> adapterIds,
//			Query query,
//			String... additionalAuthorizations ) {
//		return query(
//				adapterIds,
//				query,
//				adapterStore,
//				null,
//				null,
//				additionalAuthorizations);
//	}
//
//	@Override
//	public CloseableIterator<?> query(
//			Query query,
//			int limit,
//			String... additionalAuthorizations ) {
//		//  #406 Need to fix
//		LOGGER.error("This method query3 is not yet coded. Need to fix it");
//		return null;
//	}
//
//	@Override
//	public <T> CloseableIterator<T> query(
//			DataAdapter<T> adapter,
//			Query query,
//			int limit,
//			String... additionalAuthorizations ) {
//		//  #406 Need to fix
//		LOGGER.error("This method query4b is not yet coded. Need to fix it");
//		return null;
//	}
//
//	@Override
//	public <T> CloseableIterator<T> query(
//			PrimaryIndex index,
//			Query query,
//			int limit,
//			String... additionalAuthorizations ) {
//		//  #406 Need to fix
//		LOGGER.error("This method query4a is not yet coded. Need to fix it");
//		return null;
//	}
//
//	@Override
//	public CloseableIterator<?> query(
//			List<ByteArrayId> adapterIds,
//			Query query,
//			int limit,
//			String... additionalAuthorizations ) {
//		//  #406 Need to fix
//		LOGGER.error("This method query4 is not yet coded. Need to fix it");
//		return null;
//	}

//	@SuppressWarnings("unchecked")
//	private <T> CloseableIterator<T> query(
//			final DataAdapter<T> adapter,
//			final Query query,
//			final Integer limit ) {
//		store(adapter);
//		return ((CloseableIterator<T>) query(
//				Arrays.asList(new ByteArrayId[] {
//					adapter.getAdapterId()
//				}),
//				query,
//				new MemoryAdapterStore(
//						new DataAdapter[] {
//							adapter
//						}),
//				limit,
//				null));
//	}
//
//	private CloseableIterator<?> query(
//			final List<ByteArrayId> adapterIds,
//			final Query query,
//			final AdapterStore adapterStore,
//			final Integer limit,
//			final ScanCallback<?> scanCallback,
//			final String... authorizations ) {
//		try (final CloseableIterator<Index<?, ?>> indices = indexStore.getIndices()) {
//			return query(
//					adapterIds,
//					query,
//					indices,
//					adapterStore,
//					limit,
//					scanCallback,
//					null,
//					authorizations);
//		}
//		catch (final IOException e) {
//			LOGGER.warn(
//					"unable to close index iterator for query",
//					e);
//		}
//		return new HBaseCloseableIteratorWrapper<Object>(
//				new Closeable() {
//					@Override
//					public void close()
//							throws IOException {}
//				},
//				new ArrayList<Object>().iterator());
//	}
//
//	@SuppressWarnings("unchecked")
//	private <T> CloseableIterator<T> query(
//			final PrimaryIndex index,
//			final Query query,
//			final Integer limit,
//			final QueryOptions queryOptions,
//			final String... additionalAuthorizations ) {
//		if ((query != null) && !query.isSupported(index)) {
//			throw new IllegalArgumentException(
//					"Index does not support the query");
//		}
//		return (CloseableIterator<T>) query(
//				null,
//				query,
//				new CloseableIterator.Wrapper(
//						Arrays.asList(
//								new PrimaryIndex[] {
//									index
//								}).iterator()),
//				adapterStore,
//				limit,
//				null,
//				queryOptions,
//				additionalAuthorizations);
//	}

//	private CloseableIterator<?> query(
//			final List<ByteArrayId> adapterIds,
//			final Query query,
//			final CloseableIterator<PrimaryIndex> indices,
//			final AdapterStore adapterStore,
//			final Integer limit,
//			final ScanCallback<?> scanCallback,
//			final QueryOptions queryOptions,
//			final String... authorizations ) {
//		// query the indices that are supported for this query object, and these
//		// data adapter Ids
//		final List<CloseableIterator<?>> results = new ArrayList<CloseableIterator<?>>();
//		int indexCount = 0;
//		// all queries will use the same instance of the dedupe filter for
//		// client side filtering because the filter needs to be applied across
//		// indices
//		final DedupeFilter clientDedupeFilter = new DedupeFilter();
//		while (indices.hasNext()) {
//			final PrimaryIndex index = indices.next();
//			final HBaseConstraintsQuery hbaseQuery;
//			if (query == null) {
//				hbaseQuery = new HBaseConstraintsQuery(
//						adapterIds,
//						index,
//						clientDedupeFilter,
//						scanCallback,
//						authorizations);
//			}
//			else if (query.isSupported(index)) {
//				// construct the query
//				hbaseQuery = new HBaseConstraintsQuery(
//						adapterIds,
//						index,
//						query.getIndexConstraints(index.getIndexStrategy()),
//						query.createFilters(index.getIndexModel()),
//						clientDedupeFilter,
//						scanCallback,
//						authorizations);
//			}
//			else {
//				continue;
//			}
//			if ((queryOptions != null) && (!queryOptions.getFieldIds().isEmpty())) {
//				// results should contain subset of fieldIds
//				hbaseQuery.setFieldIds(queryOptions.getFieldIds());
//			}
//			results.add(hbaseQuery.query(
//					operations,
//					adapterStore,
//					limit,
//					true));
//			indexCount++;
//		}
//		// if there aren't multiple indices, the client-side dedupe filter can
//		// just cache rows that are duplicated within the index and not
//		// everything
//		clientDedupeFilter.setMultiIndexSupportEnabled(indexCount > 1);
//		// concatenate iterators
//		return new HBaseCloseableIteratorWrapper<Object>(
//				new Closeable() {
//					@Override
//					public void close()
//							throws IOException {
//						for (final CloseableIterator<?> result : results) {
//							result.close();
//						}
//					}
//				},
//				Iterators.concat(results.iterator()));
//	}

}
