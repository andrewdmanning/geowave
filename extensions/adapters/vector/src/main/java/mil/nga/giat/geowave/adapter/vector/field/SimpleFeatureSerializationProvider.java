package mil.nga.giat.geowave.adapter.vector.field;

import java.io.ByteArrayOutputStream;

import org.geotools.feature.simple.SimpleFeatureImpl;
import org.opengis.feature.simple.SimpleFeature;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import mil.nga.giat.geowave.adapter.vector.serialization.kryo.KryoFeatureSerializer;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

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

	protected static class SimpleFeatureReader implements
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

	protected static class SimpleFeatureWriter implements
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

}
