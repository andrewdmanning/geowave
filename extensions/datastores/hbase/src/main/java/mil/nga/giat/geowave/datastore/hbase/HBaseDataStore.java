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
import java.util.concurrent.atomic.AtomicBoolean;

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
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CastIterator;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStoreCallbackManager;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
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
import mil.nga.giat.geowave.core.store.query.RowIdQuery;
import mil.nga.giat.geowave.datastore.hbase.index.secondary.HBaseSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseAdapterStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseDataStatisticsStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseIndexStore;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.query.HBaseConstraintsQuery;
import mil.nga.giat.geowave.datastore.hbase.query.HBaseRowIdsQuery;
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

	public AdapterStore getAdapterStore() {
		return adapterStore;
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
					if (sanitizedQuery instanceof RowIdQuery) {
						final HBaseRowIdsQuery q = new HBaseRowIdsQuery(
								sanitizedQueryOptions.getAdapterIds(adapterStore),
								index,
								((RowIdQuery) sanitizedQuery).getRowIds(),
								sanitizedQueryOptions.getScanCallback(),
								filter,
								sanitizedQueryOptions.getAuthorizations());

						results.add(q.query(
								operations,
								tempAdapterStore,
								// TODO support this?
								//sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
								-1));
						continue;
					}
					else
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
						final HBaseConstraintsQuery hbaseQuery = new HBaseConstraintsQuery(
								Collections.singletonList(((AdapterIdQuery) sanitizedQuery).getAdapterId()),
								index,
								sanitizedQuery,
								filter,
								sanitizedQueryOptions.getScanCallback(),
								queryOptions.getAggregation(),
								sanitizedQueryOptions.getAuthorizations());
						results.add(hbaseQuery.query(
								operations,
								tempAdapterStore,
								// TODO support this?
								//sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
								sanitizedQueryOptions.getLimit()));
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
							hbaseQuery = new HBaseConstraintsQuery(
									adapterIds,
									index,
									sanitizedQuery,
									filter,
									sanitizedQueryOptions.getScanCallback(),
									queryOptions.getAggregation(),
									sanitizedQueryOptions.getAuthorizations());

							results.add(hbaseQuery.query(
									operations,
									tempAdapterStore,
									// TODO support this?
									//sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
									sanitizedQueryOptions.getLimit()));
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

	@Override
	public boolean delete(
			final QueryOptions queryOptions,
			final Query query ) {
		if (((query == null) || (query instanceof EverythingQuery)) && queryOptions.isAllAdaptersAndIndices()) {
			try {
				operations.deleteAll();
			}
			catch (final IOException e) {
				LOGGER.error(
						"Unable to delete all tables",
						e);
				return false;
			}
		}
		else {

			try (CloseableIterator<Index<?, ?>> indexIt = queryOptions.getIndices(indexStore)) {
				final AtomicBoolean aOk = new AtomicBoolean(
						true);
				while (indexIt.hasNext() && aOk.get()) {
					final PrimaryIndex index = (PrimaryIndex) indexIt.next();
					final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
					final String altIdxTableName = tableName + HBaseUtils.ALT_INDEX_TABLE;

					final boolean useAltIndex = options.isUseAltIndex() && operations.tableExists(altIdxTableName);

					final HBaseWriter idxDeleter = operations.createWriter(
							tableName,
							"",
							false);
					final HBaseWriter altIdxDelete = operations.createWriter(
							altIdxTableName,
							"",
							false);

					try (final DataStoreCallbackManager callbackCache = new DataStoreCallbackManager(
							statisticsStore,
							secondaryIndexDataStore)) {
						callbackCache.setPersistStats(options.persistDataStatistics);

						try (final CloseableIterator<DataAdapter<?>> adapterIt = queryOptions.getAdapters(adapterStore)) {
							while (adapterIt.hasNext()) {
								final DataAdapter<Object> adapter = (DataAdapter<Object>) adapterIt.next();

								final ScanCallback<Object> callback = new ScanCallback<Object>() {
									@Override
									public void entryScanned(
											final DataStoreEntryInfo entryInfo,
											final Object entry ) {
										callbackCache.getDeleteCallback(
												(WritableDataAdapter<Object>) adapter,
												index).entryDeleted(
												entryInfo,
												entry);
										try {
											addToBatch(
													idxDeleter,
													entryInfo.getRowIds());
											if (useAltIndex) {
												addToBatch(
														altIdxDelete,
														Collections.singletonList(adapter.getDataId(entry)));
											}
										}
										catch (final IOException e) {
											LOGGER.error(
													"Failed deletion",
													e);
											aOk.set(false);
										}

									}
								};

								CloseableIterator<?> dataIt = null;
								if (query instanceof RowIdQuery) {
									final HBaseRowIdsQuery<Object> q = new HBaseRowIdsQuery<Object>(
											queryOptions.getAdapterIds(adapterStore),
											index,
											((RowIdQuery) query).getRowIds(),
											callback,
											null,
											queryOptions.getAuthorizations());

									dataIt = q.query(
											operations,
											adapterStore,
											// TODO support this?
											//null,
											-1);
								}
								else if (query instanceof DataIdQuery) {
									final DataIdQuery idQuery = (DataIdQuery) query;
									dataIt = getEntries(
											index,
											idQuery.getDataIds(),
											(DataAdapter<Object>) adapterStore.getAdapter(idQuery.getAdapterId()),
											null,
											callback,
											queryOptions.getAuthorizations(),
											null,
											false);
								}
								else if (query instanceof AdapterIdQuery) {
									dataIt = new HBaseConstraintsQuery(
											Collections.singletonList(((AdapterIdQuery) query).getAdapterId()),
											index,
											query,
											null,
											callback,
											null,
											queryOptions.getAuthorizations()).query(
											operations,
											adapterStore,
											// TODO support this?
											//null,
											null);
								}
								else if (query instanceof PrefixIdQuery) {
									// TODO
//									dataIt = new HBaseRowPrefixQuery<Object>(
//											index,
//											((PrefixIdQuery) query).getRowPrefix(),
//											callback,
//											null,
//											queryOptions.getAuthorizations()).query(
//											operations,
//											null,
//											adapterStore);

								}
								else {
									dataIt = new HBaseConstraintsQuery(
											Collections.singletonList(adapter.getAdapterId()),
											index,
											query,
											null,
											callback,
											null,
											queryOptions.getAuthorizations()).query(
											operations,
											adapterStore,
											// TODO support this?
											//null,
											null);
								}

								while (dataIt.hasNext()) {
									dataIt.next();
								}
								try {
									dataIt.close();
								}
								catch (final Exception ex) {
									LOGGER.warn(
											"Cannot close iterator",
											ex);
								}
							}
						}
					}
					idxDeleter.close();
					if (altIdxDelete != null) {
						altIdxDelete.close();
					}
				}
				return aOk.get();
			}
			catch (final IOException e) {
				LOGGER.error(
						"Failed delete operation " + query.toString(),
						e);
				return false;
			}
		}

		return true;
	}

	private void addToBatch(
			final HBaseWriter deleter,
			final List<ByteArrayId> ids )
			throws IOException {
		final List<Delete> deletes = new ArrayList<Delete>();
		for (final ByteArrayId id : ids) {
			deletes.add(new Delete(
					id.getBytes()));
		}
		deleter.delete(deletes);
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

}
