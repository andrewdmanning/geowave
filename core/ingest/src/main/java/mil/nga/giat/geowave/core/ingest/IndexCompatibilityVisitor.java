package mil.nga.giat.geowave.core.ingest;

import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/**
 * This is a visitor that can interpret the compatibility of an index
 * 
 */
public interface IndexCompatibilityVisitor
{
	/**
	 * Determine whether an index is compatible with the visitor
	 * 
	 * @param index
	 *            an index that an ingest type supports
	 * @return whether this index is compatible with the visitor
	 */
	public boolean isCompatible(
			Class<? extends CommonIndexValue>[] indexTypes );
}
