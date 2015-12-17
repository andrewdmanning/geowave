package mil.nga.giat.geowave.adapter.vector.field;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.geotools.feature.FeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureImpl;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

public class SimpleFeatureSerializationProvider
{

	public static class WholeFeatureReader implements
			FieldReader<byte[][]>
	{
		SimpleFeatureType type;

		public WholeFeatureReader(
				SimpleFeatureType type ) {
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
			int attrCnt = type.getAttributeCount();
			byte[][] retVal = new byte[attrCnt][];
			try {

				for (int i = 0; i < attrCnt; i++) {
					int byteLength;
					byteLength = input.readInt();
					if (byteLength < 0) {
						retVal[i] = null;
						continue;
					}
					byte[] fieldValue = new byte[byteLength];
					input.read(fieldValue);
					retVal[i] = fieldValue;
				}
			}
			catch (IOException e) {
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
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			final DataOutputStream output = new DataOutputStream(
					baos);

			try {
				for (Object attr : fieldValue) {
					if (attr == null) {
						output.writeInt(-1);

						continue;
					}
					FieldWriter writer = FieldUtils.getDefaultWriterForClass(attr.getClass());
					byte[] binary = writer.writeField(attr);
					output.writeInt(binary.length);
					output.write(binary);
				}
				output.close();
			}
			catch (IOException e) {
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
