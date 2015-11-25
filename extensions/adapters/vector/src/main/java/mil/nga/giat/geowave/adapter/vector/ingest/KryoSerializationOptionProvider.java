package mil.nga.giat.geowave.adapter.vector.ingest;

import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.ingest.IngestFormatOptionProvider;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public class KryoSerializationOptionProvider implements
		IngestFormatOptionProvider,
		Persistable
{
	private boolean kryo = false;

	@Override
	public void applyOptions(
			final Options allOptions ) {
		allOptions.addOption(
				"kryo",
				false,
				"A flag to indicate whether kryo serialization should be used");
	}

	@Override
	public void parseOptions(
			final CommandLine commandLine ) {
		if (commandLine.hasOption("kryo")) {
			kryo = true;
		}
	}

	public boolean isKryo() {
		return kryo;
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {
			kryo ? (byte) 1 : (byte) 0
		};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		if ((bytes != null) && (bytes.length > 0) && (bytes[0] > 0)) {
			kryo = true;
		}
	}
}
