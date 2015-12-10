package mil.nga.giat.geowave.adapter.vector.ingest;

import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.ingest.IngestFormatOptionProvider;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public class FeatureSerializationOptionProvider implements
		IngestFormatOptionProvider,
		Persistable
{
	private boolean kryo = false;

	private boolean avro = false;

	@Override
	public void applyOptions(
			final Options allOptions ) {
		allOptions.addOption(
				"kryo",
				false,
				"A flag to indicate whether kryo serialization should be used");
		allOptions.addOption(
				"avro",
				false,
				"A flag to indicate whether avro feature serialization should be used");
	}

	@Override
	public void parseOptions(
			final CommandLine commandLine ) {
		if (commandLine.hasOption("kryo")) {
			kryo = true;
		}
		if (commandLine.hasOption("avro")) {
			avro = true;
		}
	}

	public boolean isKryo() {
		return kryo;
	}

	public boolean isAvro() {
		return avro;
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {
			kryo ? (byte) 1 : avro ? (byte) 2 : (byte) 0
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
				avro = true;
			}
		}
	}
}
