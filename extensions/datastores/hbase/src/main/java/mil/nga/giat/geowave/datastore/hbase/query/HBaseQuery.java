/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.query;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.index.Index;

import org.apache.log4j.Logger;

/**
 * @author viggy Functionality similar to <code> AccumuloQuery </code>
 */
abstract public class HBaseQuery
{

	private final static Logger LOGGER = Logger.getLogger(HBaseQuery.class);
	protected final List<ByteArrayId> adapterIds;
	protected final Index index;

	private final String[] authorizations;

	public HBaseQuery(
			final Index index,
			final String... authorizations ) {
		this(
				null,
				index,
				authorizations);
	}

	public HBaseQuery(
			final List<ByteArrayId> adapterIds,
			final Index index,
			final String... authorizations ) {
		this.adapterIds = adapterIds;
		this.index = index;
		this.authorizations = authorizations;
	}

	public String[] getAdditionalAuthorizations() {
		return authorizations;
	}

	abstract protected List<ByteArrayRange> getRanges();

}
