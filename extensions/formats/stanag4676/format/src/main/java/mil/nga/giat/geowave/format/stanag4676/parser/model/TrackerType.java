package mil.nga.giat.geowave.format.stanag4676.parser.model;

//STANAG 4676
/**
 * The type of tracker that generated a track (i.e. manual, automatic,
 * semi-automatic).
 */
public enum TrackerType {
	/**
	 * A track is approximated/estimated by a human operator/analyst.
	 */
	MANUAL_TRACKER(
			"MANUAL_TRACKER"),

	/**
	 * A track generated by an automatic tracker, based on measured data.
	 */
	AUTOMATIC_TRACKER(
			"AUTOMATIC_TRACKER"),

	/**
	 * A track generated by an automatic tracker in combination with information
	 * approximated/estimated by an operator/analyst.
	 */
	SEMIAUTOMATIC_TRACKER(
			"SEMIAUTOMATIC_TRACKER");

	private String value;

	TrackerType() {
		this.value = TrackStatus.values()[0].toString();
	}

	TrackerType(
			final String value ) {
		this.value = value;
	}

	public static TrackerType fromString(
			String value ) {
		for (final TrackerType item : TrackerType.values()) {
			if (item.toString().equals(
					value)) {
				return item;
			}
		}
		return null;
	}

	@Override
	public String toString() {
		return value;
	}
}
