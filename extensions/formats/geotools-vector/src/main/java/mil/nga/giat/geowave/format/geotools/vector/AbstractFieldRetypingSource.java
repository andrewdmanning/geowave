package mil.nga.giat.geowave.format.geotools.vector;

import mil.nga.giat.geowave.format.geotools.vector.RetypingVectorDataPlugin.RetypingVectorDataSource;

import org.geoserver.feature.RetypingFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.Name;
import org.opengis.filter.identity.FeatureId;

abstract public class AbstractFieldRetypingSource implements
		RetypingVectorDataSource
{

	abstract public String getFeatureId(
			SimpleFeature original );

	abstract public Object retypeAttributeValue(
			Object value,
			Name attributeName );

	@Override
	public SimpleFeature getRetypedSimpleFeature(
			SimpleFeatureBuilder builder,
			SimpleFeature original ) {

		final SimpleFeatureType target = builder.getFeatureType();
		for (int i = 0; i < target.getAttributeCount(); i++) {
			final AttributeDescriptor attributeType = target.getDescriptor(i);
			Object value = null;

			if (original.getFeatureType().getDescriptor(
					attributeType.getName()) != null) {
				final Name name = attributeType.getName();
				value = retypeAttributeValue(
						original.getAttribute(name),
						name);
			}

			builder.add(value);
		}
		String featureId = getFeatureId(original);
		if (featureId == null) {
			// a null ID will default to use the original
			final FeatureId id = RetypingFeatureCollection.reTypeId(
					original.getIdentifier(),
					original.getFeatureType(),
					target);
			featureId = id.getID();
		}
		final SimpleFeature retyped = builder.buildFeature(featureId);
		retyped.getUserData().putAll(
				original.getUserData());
		return retyped;
	}
}
