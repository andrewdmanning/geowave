package mil.nga.giat.geowave.adapter.vector.serialization.kryo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

public class KryoFeatureSerializer extends
		Serializer<SimpleFeature>
{
	private final static Logger LOGGER = Logger.getLogger(KryoFeatureSerializer.class);
	private final Map<String, SimpleFeatureType> typeCache = new HashMap<>();
	private final Map<String, SimpleFeatureBuilder> builderCache = new HashMap<>();
	private WKBReader geoByteReader;
	private WKBWriter geoByteWriter;

	@Override
	public void write(
			final Kryo kryo,
			final Output output,
			final SimpleFeature sf ) {
		output.writeString(sf.getID());
		output.writeString(sf.getFeatureType().getTypeName());
		output.writeString(DataUtilities.encodeType(sf.getFeatureType()));
		// write attributes
		for (final AttributeDescriptor ad : sf.getFeatureType().getAttributeDescriptors()) {
			writeAttribute(
					kryo,
					output,
					ad.getType().getBinding(),
					sf.getAttribute(ad.getLocalName()));
		}
	}

	private void writeAttribute(
			final Kryo kryo,
			final Output output,
			final Class<?> attributeType,
			final Object attributeValue ) {
		if (attributeValue == null) {
			// null marker
			output.writeBoolean(true);
		}
		else {
			output.writeBoolean(false);
			if ((attributeType == Boolean.class) || (attributeType == boolean.class)) {
				output.writeBoolean((Boolean) attributeValue);
			}
			else if ((attributeType == Byte.class) || (attributeType == byte.class)) {
				output.writeByte((Byte) attributeValue);
			}
			else if ((attributeType == Short.class) || (attributeType == short.class)) {
				output.writeShort((Short) attributeValue);
			}
			else if ((attributeType == Integer.class) || (attributeType == int.class)) {
				output.writeInt((Integer) attributeValue);
			}
			else if ((attributeType == Long.class) || (attributeType == long.class)) {
				output.writeLong((Long) attributeValue);
			}
			else if ((attributeType == Float.class) || (attributeType == float.class)) {
				output.writeFloat((Float) attributeValue);
			}
			else if ((attributeType == Double.class) || (attributeType == double.class)) {
				output.writeDouble((Double) attributeValue);
			}
			else if (attributeType == String.class) {
				output.writeString((String) attributeValue);
			}
			else if ((attributeType == java.sql.Date.class) || (attributeType == java.sql.Time.class) || (attributeType == java.sql.Timestamp.class) || (attributeType == java.util.Date.class)) {
				output.writeLong(((Date) attributeValue).getTime());
			}
			else if (Geometry.class.isAssignableFrom(attributeType)) {
				final byte[] buffer = getGeoByteWriter().write(
						(Geometry) attributeValue);
				final int length = buffer.length;
				output.writeInt(length);
				output.write(buffer);
			}
			else if (attributeType == UUID.class) {
				output.writeString(((UUID) attributeValue).toString());
			}
			else {
				// catch-all for other types
				try {
					// check to see if Kryo knows how to serialize this class
					kryo.newInstance(attributeType);
					// answer is yes, so use Kryo
					kryo.writeObject(
							output,
							attributeValue);
				}
				catch (final KryoException ke) {

					// Kryo cannot deserialize this class, so log a warning
					// and fall-back to default Java serialization

					if (Serializable.class.isAssignableFrom(attributeType)) {
						// IMPORTANT: Any time we see this warning, we should
						// write a custom Kryo serializer for the type to
						// achieve full optimization
						LOGGER.warn("Kryo was unable to serialize class of type " + attributeType.getName() + ", using ObjectOutputStream to write full metadata");

						final ByteArrayOutputStream bos = new ByteArrayOutputStream();
						ObjectOutputStream oos;
						try {
							oos = new ObjectOutputStream(
									bos);
							oos.writeObject(attributeValue);
							oos.flush();
							final byte[] bytes = bos.toByteArray();
							output.writeInt(bytes.length);
							output.write(bytes);
						}
						catch (final IOException e) {
							LOGGER.error(
									"Unable to serialize object of type " + attributeType.getName(),
									e);
						}
					}
					else {
						// If we get here, it means (1) Kyro is unable to
						// deserialize this class and (2) we cannot fall back on
						// default Java serialization since the class does not
						// implement java.io.Serializable. We MUST write a
						// custom serializer for whatever class is causing this
						// issue. Unfortunately, since you can jam an object of
						// almost any type you want in a SimpleFeature, it would
						// be impossible to write custom serializers for any
						// case in advance)
						LOGGER.error("Unable to serialize object of type " + attributeType.getName() + ": class must at least implement java.io.Serializable");
					}
				}
			}
		}
	}

	@Override
	public SimpleFeature read(
			final Kryo kryo,
			final Input input,
			final Class<SimpleFeature> type ) {
		final String featureId = input.readString();
		final String typeName = input.readString();
		final String typeSpec = input.readString();
		final SimpleFeatureType sft = getType(
				typeName,
				typeSpec);
		if (sft != null) {
			final SimpleFeatureBuilder sfBuilder = getBuilder(sft);
			// read attributes
			for (final AttributeDescriptor ad : sft.getAttributeDescriptors()) {
				final Object value = readAttribute(
						kryo,
						input,
						ad.getType().getBinding());
				sfBuilder.set(
						ad.getLocalName(),
						value);
			}
			return sfBuilder.buildFeature(featureId);
		}
		return null; // unable to deserialize SimpleFeature
	}

	private Object readAttribute(
			final Kryo kryo,
			final Input input,
			final Class<?> attributeType ) {
		final boolean isNull = input.readBoolean();
		if (isNull) {
			return null;
		}
		else {
			if ((attributeType == Boolean.class) || (attributeType == boolean.class)) {
				return input.readBoolean();
			}
			else if ((attributeType == Byte.class) || (attributeType == byte.class)) {
				return input.readByte();
			}
			else if ((attributeType == Short.class) || (attributeType == short.class)) {
				return input.readShort();
			}
			else if ((attributeType == Integer.class) || (attributeType == int.class)) {
				return input.readInt();
			}
			else if ((attributeType == Long.class) || (attributeType == long.class)) {
				return input.readLong();
			}
			else if ((attributeType == Float.class) || (attributeType == float.class)) {
				return input.readFloat();
			}
			else if ((attributeType == Double.class) || (attributeType == double.class)) {
				return input.readDouble();
			}
			else if (attributeType == String.class) {
				return input.readString();
			}
			else if (attributeType == java.sql.Date.class) {
				return new java.sql.Date(
						input.readLong());
			}
			else if (attributeType == java.sql.Time.class) {
				return new java.sql.Time(
						input.readLong());
			}
			else if (attributeType == java.sql.Timestamp.class) {
				return new java.sql.Timestamp(
						input.readLong());
			}
			else if (attributeType == java.util.Date.class) {
				return new java.util.Date(
						input.readLong());
			}
			else if (Geometry.class.isAssignableFrom(attributeType)) {
				final int length = input.readInt();
				final byte[] buffer = new byte[length];
				input.readBytes(buffer);
				try {
					return getGeoByteReader().read(
							buffer);
				}
				catch (final ParseException e) {
					LOGGER.warn(
							"Unable to deserialize Geometry",
							e);
					return null;
				}

			}
			else if (attributeType == UUID.class) {
				return UUID.fromString(input.readString());
			}
			else {
				// catch-all for other types
				try {
					// check to see if Kryo knows how to serialize this class
					kryo.newInstance(attributeType);
					// answer is yes, so use Kryo
					return kryo.readObject(
							input,
							attributeType);
				}
				catch (final KryoException ke) {

					// Kryo cannot deserialize this class, so log a warning
					// and fall-back to default Java serialization

					if (Serializable.class.isAssignableFrom(attributeType)) {
						// IMPORTANT: Any time we see this warning, we should
						// write a custom Kryo serializer for the type to
						// achieve full optimization
						LOGGER.warn("Kryo was unable to deserialize class of type " + attributeType.getName() + ", using ObjectInputStream to read full metadata");

						final int length = input.readInt();
						final byte[] buffer = new byte[length];
						input.readBytes(buffer);
						final ByteArrayInputStream bis = new ByteArrayInputStream(
								buffer);
						ObjectInputStream ois;
						try {
							ois = new ObjectInputStream(
									bis);
							return ois.readObject();
						}
						catch (IOException | ClassNotFoundException e) {
							LOGGER.error(
									"Unable to deserialize object of type " + attributeType.getName(),
									e);
							return null;
						}
					}
					else {
						// If we get here, it means (1) Kyro is unable to
						// deserialize this class and (2) we cannot fall back on
						// default Java serialization since the class does not
						// implement java.io.Serializable. We MUST write a
						// custom serializer for whatever class is causing this
						// issue. Unfortunately, since you can jam an object of
						// almost any type you want in a SimpleFeature, it would
						// be impossible to write custom serializers for any
						// case in advance)
						LOGGER.error("Unable to serialize object of type " + attributeType.getName() + ": class must at least implement java.io.Serializable");
						return null;
					}
				}
			}
		}
	}

	private SimpleFeatureType getType(
			final String typeName,
			final String typeSpec ) {
		if (typeCache.containsKey(typeName)) {
			return typeCache.get(typeName);
		}
		else {
			SimpleFeatureType type = null;
			try {
				type = DataUtilities.createType(
						typeName,
						typeSpec);
				typeCache.put(
						typeName,
						type);
			}
			catch (final SchemaException e) {
				LOGGER.warn(
						"Unable to deserialize SimpleFeature",
						e);
			}
			return type;
		}
	}

	private SimpleFeatureBuilder getBuilder(
			final SimpleFeatureType sft ) {
		if (builderCache.containsKey(sft.getTypeName())) {
			return builderCache.get(sft.getTypeName());
		}
		else {
			final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
					sft);
			builderCache.put(
					sft.getTypeName(),
					builder);
			return builder;
		}
	}

	private WKBReader getGeoByteReader() {
		if (geoByteReader == null) {
			geoByteReader = new WKBReader();
		}
		return geoByteReader;
	}

	private WKBWriter getGeoByteWriter() {
		if (geoByteWriter == null) {
			geoByteWriter = new WKBWriter();
		}
		return geoByteWriter;
	}

}