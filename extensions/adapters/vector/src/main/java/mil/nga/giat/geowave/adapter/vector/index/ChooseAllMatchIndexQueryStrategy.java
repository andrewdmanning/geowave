package mil.nga.giat.geowave.adapter.vector.index;

import java.io.IOException;
import java.util.NoSuchElementException;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;

public class ChooseAllMatchIndexQueryStrategy implements
		IndexQueryStrategySPI
{

	public static final String NAME = "All Match";

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

			@Override
			public boolean hasNext() {
				while (nextIdx == null && indices.hasNext()) {
					Index<?, ?> nextChoosenIdx = indices.next();
					if (nextChoosenIdx instanceof PrimaryIndex) {
						nextIdx = (PrimaryIndex) nextChoosenIdx;
						if (geoConstraints.isSupported(nextIdx) && timeConstraints.isSupported(nextIdx)) break;
						nextIdx = null;
					}
				}
				return nextIdx != null;
			}

			@Override
			public Index<?, ?> next()
					throws NoSuchElementException {
				if (nextIdx == null) throw new NoSuchElementException();
				Index<?, ?> returnVal = nextIdx;
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
}
