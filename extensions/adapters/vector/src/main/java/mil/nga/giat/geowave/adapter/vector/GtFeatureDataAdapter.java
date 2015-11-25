package mil.nga.giat.geowave.adapter.vector;

import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public interface GtFeatureDataAdapter extends
		WritableDataAdapter<SimpleFeature>
{
	public SimpleFeatureType getType();

	public TimeDescriptors getTimeDescriptors();
}
