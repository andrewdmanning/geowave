package mil.nga.giat.geowave.adapter.vector.ingest;

import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.StringUtils;

import org.apache.log4j.Logger;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterVisitor;

import com.beust.jcommander.Parameter;

public class CQLFilterOptionProvider implements
		Filter,
		Persistable
{
	private final static Logger LOGGER = Logger.getLogger(CQLFilterOptionProvider.class);

	@Parameter(names = {
		"-cql"
	}, required = false, description = "A CQL filter, only data matching this filter will be ingested")
	private String cqlFilterString = null;
	private Filter filter;

	public String getCqlFilterString() {
		return cqlFilterString;
	}

	@Override
	public byte[] toBinary() {
		if (cqlFilterString == null) {
			return new byte[] {};
		}
		return StringUtils.stringToBinary(cqlFilterString);
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		if (bytes.length > 0) {
			cqlFilterString = StringUtils.stringFromBinary(bytes);
			resolveCQLStrToFilter();
		}
		else {
			cqlFilterString = null;
			filter = null;
		}
	}

	@Override
	public boolean evaluate(
			final Object object ) {
		if (filter == null) {
			return true;
		}
		return filter.evaluate(object);
	}

	@Override
	public Object accept(
			final FilterVisitor visitor,
			final Object extraData ) {
		if (filter == null) {
			if (visitor != null) {
				return visitor.visitNullFilter(extraData);
			}
			return extraData;
		}
		return filter.accept(
				visitor,
				extraData);
	}

	private void resolveCQLStrToFilter() {
		if (cqlFilterString != null) {
			try {
				filter = asFilter(cqlFilterString);
			}
			catch (final CQLException e) {
				LOGGER.error(
						"Cannot parse CQL expression '" + cqlFilterString + "'",
						e);
				cqlFilterString = null;
				filter = null;
			}
		}
		else {
			cqlFilterString = null;
		}
	}

	private static Filter asFilter(
			final String cqlPredicate )
			throws CQLException {
		return CQL.toFilter(cqlPredicate);
	}

}
