package mil.nga.giat.geowave.core.ingest;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.CompoundIndexStrategy;
import mil.nga.giat.geowave.core.index.simple.RoundRobinKeyIndexStrategy;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestCommandLineOptions
{
	private final static Logger LOGGER = LoggerFactory.getLogger(
			IngestCommandLineOptions.class);

	private static Map<String, IndexCompatibilityVisitor> registeredDimensionalityTypes = null;
	private static String defaultDimensionalityType;
	private final String visibility;
	private final boolean clearNamespace;
	private final String dimensionalityType;
	private final int randomPartitions;

	public IngestCommandLineOptions(
			final String visibility,
			final boolean clearNamespace,
			final String dimensionalityType,
			final int randomPartitions ) {
		this.visibility = visibility;
		this.clearNamespace = clearNamespace;
		this.dimensionalityType = dimensionalityType;
		this.randomPartitions = randomPartitions;
	}

	public String getVisibility() {
		return visibility;
	}

	public String getDimensionalityType() {
		return dimensionalityType;
	}

	public boolean isClearNamespace() {
		return clearNamespace;
	}

	public int getRandomPartitions() {
		return randomPartitions;
	}

	public PrimaryIndex getIndex(
			final DataAdapterProvider<?> adapterProvider ) {
		final IndexCompatibilityVisitor compatibilityVisitor = getSelectedIndexCompatibility(getDimensionalityType());
		final Class <? extends CommonIndexValue>[] supportedIndexTypes = adapterProvider.getDataAdapters(visibility)[0].getSupportedIndexableTypes();

			if (compatibilityVisitor.isCompatible(supportedIndexTypes)) {
				if (randomPartitions > 1) {
					return new CustomIdIndex(
							new CompoundIndexStrategy(
									new RoundRobinKeyIndexStrategy(
											randomPartitions),
									i.getIndexStrategy()),
							i.getIndexModel(),
							new ByteArrayId(
									i.getId().getString() + "_RAND" + randomPartitions));
				}
				else {
					return i;
				}
			}
		}return null;

	}

	public boolean isSupported(
			final PrimaryIndex[] supportedIndices ) {
		return (getIndex(supportedIndices) != null);
	}

	private static synchronized String getDimensionalityTypeOptionDescription() {
		if (registeredDimensionalityTypes == null) {
			initDimensionalityTypeRegistry();
		}
		if (registeredDimensionalityTypes.isEmpty()) {
			return "There are no registered dimensionality types.  The supported index listed first for any given data type will be used.";
		}
		final StringBuilder builder = ConfigUtils.getOptions(registeredDimensionalityTypes.keySet());
		builder.append(
				"(optional; default is '").append(
				defaultDimensionalityType).append(
				"')");
		return builder.toString();
	}

	private static String getDefaultDimensionalityType() {
		if (registeredDimensionalityTypes == null) {
			initDimensionalityTypeRegistry();
		}
		if (defaultDimensionalityType == null) {
			return "";
		}
		return defaultDimensionalityType;
	}

	private static IndexCompatibilityVisitor getSelectedIndexCompatibility(
			final String dimensionalityType ) {
		if (registeredDimensionalityTypes == null) {
			initDimensionalityTypeRegistry();
		}
		final IndexCompatibilityVisitor compatibilityVisitor = registeredDimensionalityTypes.get(
				dimensionalityType);
		if (compatibilityVisitor == null) {
			return new IndexCompatibilityVisitor() {

				@Override
				public boolean isCompatible(
						final PrimaryIndex index ) {
					return true;
				}
			};
		}
		return compatibilityVisitor;
	}

	private static synchronized void initDimensionalityTypeRegistry() {
		registeredDimensionalityTypes = new HashMap<String, IndexCompatibilityVisitor>();
		final Iterator<IngestDimensionalityTypeProviderSpi> dimensionalityTypesProviders = ServiceLoader.load(
				IngestDimensionalityTypeProviderSpi.class).iterator();
		int currentDefaultPriority = Integer.MIN_VALUE;
		while (dimensionalityTypesProviders.hasNext()) {
			final IngestDimensionalityTypeProviderSpi dimensionalityTypeProvider = dimensionalityTypesProviders.next();
			if (registeredDimensionalityTypes.containsKey(
					dimensionalityTypeProvider.getDimensionalityTypeName())) {
				LOGGER.warn(
						"Dimensionality type '" + dimensionalityTypeProvider.getDimensionalityTypeName() + "' already registered.  Unable to register type provided by " + dimensionalityTypeProvider.getClass().getName());
			}
			else {
				registeredDimensionalityTypes.put(
						dimensionalityTypeProvider.getDimensionalityTypeName(),
						dimensionalityTypeProvider.getCompatibilityVisitor());
				if (dimensionalityTypeProvider.getPriority() > currentDefaultPriority) {
					currentDefaultPriority = dimensionalityTypeProvider.getPriority();
					defaultDimensionalityType = dimensionalityTypeProvider.getDimensionalityTypeName();
				}
			}
		}
	}

	public static IngestCommandLineOptions parseOptions(
			final CommandLine commandLine )
					throws ParseException {
		final boolean success = true;
		boolean clearNamespace = false;
		int randomPartitions = -1;
		if (commandLine.hasOption(
				"c")) {
			clearNamespace = true;
		}
		String visibility = null;
		if (commandLine.hasOption(
				"v")) {
			visibility = commandLine.getOptionValue(
					"v");
		}
		final String dimensionalityType = commandLine.getOptionValue(
				"dim",
				getDefaultDimensionalityType());
		if (!success) {
			throw new ParseException(
					"Required option is missing");
		}
		if (commandLine.hasOption(
				"randompartitions")) {
			randomPartitions = Integer.parseInt(
					commandLine.getOptionValue(
							"randompartitions"));
		}
		return new IngestCommandLineOptions(
				visibility,
				clearNamespace,
				dimensionalityType,
				randomPartitions);
	}

	public static void applyOptions(
			final Options allOptions ) {
		final Option visibility = new Option(
				"v",
				"visibility",
				true,
				"The visibility of the data ingested (optional; default is 'public')");
		allOptions.addOption(
				visibility);

		final Option dimensionalityType = new Option(
				"dim",
				"dimensionality",
				true,
				"The preferred dimensionality type to index the data for this ingest operation. " + getDimensionalityTypeOptionDescription());
		allOptions.addOption(
				dimensionalityType);
		allOptions.addOption(
				new Option(
						"c",
						"clear",
						false,
						"Clear ALL data stored with the same prefix as this namespace (optional; default is to append data to the namespace if it exists)"));
		allOptions.addOption(
				new Option(
						"randompartitions",
						"random partitions",
						true,
						"Prefix data with this many unique identifiers to enforce random pre-splits for the index"));
	}
}
