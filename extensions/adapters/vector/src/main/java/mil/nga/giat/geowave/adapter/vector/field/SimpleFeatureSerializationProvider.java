package mil.nga.giat.geowave.adapter.vector.field;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import mil.nga.giat.geowave.adapter.vector.serialization.kryo.KryoFeatureSerializer;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

import org.geotools.feature.simple.SimpleFeatureImpl;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class SimpleFeatureSerializationProvider implements
		FieldSerializationProviderSpi<SimpleFeature>
{

	@Override
	public FieldReader<SimpleFeature> getFieldReader() {
		return new SimpleFeatureReader();
	}

	@Override
	public FieldWriter<Object, SimpleFeature> getFieldWriter() {
		return new SimpleFeatureWriter();
	}

	public static class SimpleFeatureReader implements
			FieldReader<SimpleFeature>
	{
		private final Kryo kryo;

		public SimpleFeatureReader() {
			super();
			kryo = new Kryo();
			kryo.register(
					SimpleFeatureImpl.class,
					new KryoFeatureSerializer());
		}

		@Override
		public SimpleFeature readField(
				final byte[] fieldData ) {
			if (fieldData == null) {
				return null;
			}
			final Input input = new Input(
					fieldData);
			return kryo.readObject(
					input,
					SimpleFeatureImpl.class);
		}

	}

	public static class SimpleFeatureWriter implements
			FieldWriter<Object, SimpleFeature>
	{
		private final Kryo kryo;

		public SimpleFeatureWriter() {
			super();
			kryo = new Kryo();
			kryo.register(
					SimpleFeatureImpl.class,
					new KryoFeatureSerializer());
		}

		@Override
		public byte[] writeField(
				final SimpleFeature fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}
			final Output output = new Output(
					new ByteArrayOutputStream());
			kryo.writeObject(
					output,
					fieldValue);
			output.close();
			return ((ByteArrayOutputStream) output.getOutputStream()).toByteArray();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final SimpleFeature fieldValue ) {
			return new byte[] {};
		}

	}

	public static class WholeFeatureReader implements
			FieldReader<byte[][]>
	{
		SimpleFeatureType type;

		public WholeFeatureReader(
				final SimpleFeatureType type ) {
			super();
			this.type = type;

		}

		@Override
		public byte[][] readField(
				final byte[] fieldData ) {
			if (fieldData == null) {
				return null;
			}
			final DataInputStream input = new DataInputStream(
					new ByteArrayInputStream(
							fieldData));
			final int attrCnt = type.getAttributeCount();
			final byte[][] retVal = new byte[attrCnt][];
			try {

				for (int i = 0; i < attrCnt; i++) {
					int byteLength;
					byteLength = input.readInt();
					if (byteLength < 0) {
						retVal[i] = null;
						continue;
					}
					final byte[] fieldValue = new byte[byteLength];
					if (input.read(fieldValue) != byteLength) {
						throw new IOException(
								"Inconsistent bytes read from field");
					}
					retVal[i] = fieldValue;
				}
			}
			catch (final IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return retVal;
		}

	}

	public static class WholeFeatureWriter implements
			FieldWriter<Object, Object[]>
	{
		public WholeFeatureWriter() {
			super();

		}

		@Override
		public byte[] writeField(
				final Object[] fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			final DataOutputStream output = new DataOutputStream(
					baos);

			try {
				for (final Object attr : fieldValue) {
					if (attr == null) {
						output.writeInt(-1);

						continue;
					}
					final FieldWriter writer = FieldUtils.getDefaultWriterForClass(attr.getClass());
					final byte[] binary = writer.writeField(attr);
					output.writeInt(binary.length);
					output.write(binary);
				}
				output.close();
			}
			catch (final IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return baos.toByteArray();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Object[] fieldValue ) {
			return new byte[] {};
		}

	}

}
