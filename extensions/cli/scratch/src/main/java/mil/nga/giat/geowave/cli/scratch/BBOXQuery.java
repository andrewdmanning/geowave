package mil.nga.giat.geowave.cli.scratch;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStore;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class BBOXQuery extends
		AbstractGeoWaveQuery
{

	@Override
	protected void applyOptions(
			final Options options ) {

		final Option east = new Option(
				"e",
				true,
				"East in degrees longitude");
		east.setRequired(true);
		final Option west = new Option(
				"w",
				true,
				"West in degrees longitude");
		west.setRequired(true);
		final Option north = new Option(
				"n",
				true,
				"North in degrees latitude");
		north.setRequired(true);
		final Option south = new Option(
				"s",
				true,
				"South in degrees latitude");
		south.setRequired(true);

		options.addOption(west);
		options.addOption(east);
		options.addOption(north);
		options.addOption(south);
	}

	@Override
	protected void parseOptions(
			final CommandLine commandLine ) {}

	@Override
	protected long runQuery(
			final FeatureDataAdapter adapter,
			final ByteArrayId adapterId,
			final DataStore dataStore ) {
		return 0;
	}

}
