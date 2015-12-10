package mil.nga.giat.geowave.adapter.vector.index;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;

public interface IndexQueryStrategySPI
{
	public CloseableIterator<Index<?, ?>> getIndices(
			Constraints timeConstraints,
			Constraints geoConstraints,
			CloseableIterator<Index<?, ?>> indices );
}
