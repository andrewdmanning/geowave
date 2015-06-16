package mil.nga.giat.geowave.datastore.hbase.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.Index;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

/**
 * This is used internally to translate Accumulo rows into native objects (using
 * the appropriate data adapter). It also performs any client-side filtering. It
 * will peek at the next entry in the accumulo iterator to always maintain a
 * reference to the next value.
 * 
 * @param <T>
 *            The type for the entry
 */
public class EntryIteratorWrapper<T> implements
		Iterator<T>
{
	private final static Logger LOGGER = Logger.getLogger(EntryIteratorWrapper.class);
	private final AdapterStore adapterStore;
	private final Index index;
	private final Iterator<Result> scannerIt;
	private final QueryFilter clientFilter;
	private final ScanCallback<T> scanCallback;

	private T nextValue;

	public EntryIteratorWrapper(
			final AdapterStore adapterStore,
			final Index index,
			final Iterator<Result> scannerIt,
			final QueryFilter clientFilter ) {
		this.adapterStore = adapterStore;
		this.index = index;
		this.scannerIt = scannerIt;
		this.clientFilter = clientFilter;
		this.scanCallback = null;
	}

	public EntryIteratorWrapper(
			final AdapterStore adapterStore,
			final Index index,
			final Iterator<Result> scannerIt,
			final QueryFilter clientFilter,
			final ScanCallback<T> scanCallback ) {
		this.adapterStore = adapterStore;
		this.index = index;
		this.scannerIt = scannerIt;
		this.clientFilter = clientFilter;
		this.scanCallback = scanCallback;
	}

	@Override
	public T next()
			throws NoSuchElementException {
		final T previousNext = nextValue;
		if (nextValue == null) {
			throw new NoSuchElementException();
		}
		nextValue = null;
		return previousNext;
	}

	@Override
	public boolean hasNext() {
		findNext();
		return nextValue != null;
	}
	
	private void findNext() {
		
		while ((nextValue == null) && scannerIt.hasNext()) {
			final Result row = scannerIt.next();
			final T decodedValue = decodeRow(
					row.listCells().get(0),
					clientFilter,
					index);
			if (decodedValue != null) {
				nextValue = decodedValue;
				return;
			}
		}
	}

	private T decodeRow(
			final Cell row,
			final QueryFilter clientFilter,
			final Index index ) {
		return HBaseUtils.decodeRow(
				CellUtil.cloneRow(row),
				CellUtil.cloneValue(row),
				adapterStore,
				clientFilter,
				index,
				scanCallback);
	}

	@Override
	public void remove() {
		// TODO #406 Need to fix
		LOGGER.error("This method is not yet coded. Need to fix it");
	}

}
