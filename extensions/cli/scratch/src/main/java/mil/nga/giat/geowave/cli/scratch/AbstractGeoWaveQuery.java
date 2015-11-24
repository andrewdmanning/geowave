package mil.nga.giat.geowave.cli.scratch;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.cli.CLIOperationDriver;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.split.AccumuloCommandLineOptions;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.common.base.Stopwatch;

abstract public class AbstractGeoWaveQuery implements
		CLIOperationDriver
{

	@Override
	public boolean runOperation(
			final String[] args )
			throws ParseException {
		final Stopwatch stopWatch = new Stopwatch();
		final Options allOptions = new Options();
		AccumuloCommandLineOptions.applyOptions(allOptions);
		final Option adapterIdOpt = new Option(
				"adapterId",
				true,
				"Optional ability to provide an adapter ID");
		adapterIdOpt.setRequired(false);
		allOptions.addOption(adapterIdOpt);
		applyOptions(allOptions);
		final BasicParser parser = new BasicParser();
		final CommandLine commandLine = parser.parse(
				allOptions,
				args);
		final AccumuloCommandLineOptions cli = AccumuloCommandLineOptions.parseOptions(commandLine);
		parseOptions(commandLine);
		ByteArrayId adapterId = null;
		if (commandLine.hasOption("adapterId")) {
			adapterId = new ByteArrayId(
					commandLine.getOptionValue("adapterId"));
		}

		DataStore dataStore;
		AdapterStore adapterStore;
		try {
			dataStore = new AccumuloDataStore(
					cli.getAccumuloOperations());
			adapterStore = new AccumuloAdapterStore(
					cli.getAccumuloOperations());
			final FeatureDataAdapter adapter;
			if (adapterId != null) {
				adapter = (FeatureDataAdapter) adapterStore.getAdapter(adapterId);
			}
			else {
				adapter = (FeatureDataAdapter) adapterStore.getAdapters().next();
			}
			stopWatch.start();
			final long results = runQuery(
					adapter,
					adapterId,
					dataStore);
			stopWatch.stop();
			System.out.println("Got " + results + " results in " + stopWatch.toString());
			return true;
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	abstract protected void applyOptions(
			Options options );

	abstract protected void parseOptions(
			CommandLine commandLine );

	abstract protected long runQuery(
			final FeatureDataAdapter adapter,
			final ByteArrayId adapterId,
			DataStore dataStore );

}
