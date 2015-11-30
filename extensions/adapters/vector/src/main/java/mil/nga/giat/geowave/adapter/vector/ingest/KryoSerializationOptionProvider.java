package mil.nga.giat.geowave.adapter.vector.ingest;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.ingest.IngestFormatOptionProvider;

public class KryoSerializationOptionProvider implements
		IngestFormatOptionProvider,
		Persistable
{
	private boolean kryo = false;

	private boolean whole = false;

	@Override
	public void applyOptions(
			final Options allOptions ) {
		allOptions.addOption(
				"kryo",
				false,
				"A flag to indicate whether kryo serialization should be used");
		allOptions.addOption(
				"whole",
				false,
				"A flag to indicate whether whole feature serialization should be used");
	}

	@Override
	public void parseOptions(
			final CommandLine commandLine ) {
		if (commandLine.hasOption("kryo")) {
			kryo = true;
		}
		if (commandLine.hasOption("whole")) {
			whole = true;
		}
	}

	public boolean isKryo() {
		return kryo;
	}

	public boolean isWholeFeature() {
		return whole;
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {
			kryo ? (byte) 1 : whole ? (byte) 2 : (byte) 0
		};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		if ((bytes != null) && (bytes.length > 0)) {
			if (bytes[0] == 1) {
				kryo = true;
			}
			if (bytes[0] == 2) {
				whole = true;
			}
		}
	}
}
