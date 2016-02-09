package mil.nga.giat.geowave.format.stanag4676.parser.model;

public class MotionImagery extends
		TrackItem
{
	private Long id;

	/**
	 * Provides the electromagnetic band for a given video collection
	 */
	public SymbolicSpectralRange band;

	/**
	 * Provides a URI to a JPEG or PNG image chip of an object being tracked
	 */
	public String imageReference;

	/**
	 * Provides an embedded JPEG or PNG image chip of an object being tracked
	 */
	public String imageChip;

	public Integer frameNumber;

	public Integer pixelRow;

	public Integer pixelColumn;

	public Long getId() {
		return id;
	}

	public void setId(
			final Long id ) {
		this.id = id;
	}

	public SymbolicSpectralRange getBand() {
		return band;
	}

	public void setBand(
			final SymbolicSpectralRange band ) {
		this.band = band;
	}

	public String getImageReference() {
		return imageReference;
	}

	public void setImageReference(
			final String imageReference ) {
		this.imageReference = imageReference;
	}

	public String getImageChip() {
		return imageChip;
	}

	public void setImageChip(
			final String imageChip ) {
		this.imageChip = imageChip;
	}

	public int getFrameNumber() {
		return frameNumber != null ? frameNumber : -1;
	}

	public int getPixelRow() {
		return pixelRow != null ? pixelRow : -1;
	}

	public int getPixelColumn() {
		return pixelColumn != null ? pixelColumn : -1;
	}
}
