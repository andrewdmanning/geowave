package mil.nga.giat.geowave.adapter.vector.serialization.kryo;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import mil.nga.giat.geowave.adapter.vector.serialization.kryo.KryoFeatureSerializer;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;

import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureImpl;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.vividsolutions.jts.geom.Coordinate;

public class KryoFeatureSerializerTest
{
	private static Kryo kryo = new Kryo();
	private static SimpleFeatureType schema;
	private static SimpleFeatureBuilder builder;
	private static String LOCATION_ATTRIB = "location";
	private static String CITY_ATTRIB = "city";
	private static String STATE_ATTRIB = "state";
	private static String MUNINCIPAL_POP_ATTRIB = "munincipalPop";
	private static String RANDOM_UUID_ATTRIB = "randomUuid";
	private static String CREATED_DATE_ATTRIB = "created";
	private static String CREATED_SQL_DATE_ATTRIB = "createdSqlDate";
	private static String TIME_ATTRIB = "time";
	private static String TIMESTAMP_ATTRIB = "timestamp";
	private static String POLYGON_ATTRIB = "polygon";
	private static List<String> ALL_ATTRIBS = Arrays.asList(new String[] {
		LOCATION_ATTRIB,
		CITY_ATTRIB,
		STATE_ATTRIB,
		MUNINCIPAL_POP_ATTRIB,
		RANDOM_UUID_ATTRIB,
		CREATED_DATE_ATTRIB,
		CREATED_SQL_DATE_ATTRIB,
		TIME_ATTRIB,
		TIMESTAMP_ATTRIB,
		POLYGON_ATTRIB
	});

	@BeforeClass
	public static void setup()
			throws SchemaException {
		kryo.register(
				SimpleFeatureImpl.class,
				new KryoFeatureSerializer());
		schema = DataUtilities.createType(
				"stateCapitalData",
				"*" + LOCATION_ATTRIB + ":Geometry," + CITY_ATTRIB + ":String," + STATE_ATTRIB + ":String," + MUNINCIPAL_POP_ATTRIB + ":Integer," + RANDOM_UUID_ATTRIB + ":UUID," + CREATED_DATE_ATTRIB + ":Date," + CREATED_SQL_DATE_ATTRIB + ":java.sql.Date," + TIME_ATTRIB + ":java.sql.Time," + TIMESTAMP_ATTRIB + ":java.sql.Timestamp," + POLYGON_ATTRIB + ":Polygon");
		builder = new SimpleFeatureBuilder(
				schema);
	}

	@Test
	public void testSimpleFeatureSerialization() {
		final Output output = new Output(
				new ByteArrayOutputStream());
		final SimpleFeature original = buildSimpleFeature(
				"Richmond",
				"Virginia",
				37.5,
				-77.6,
				214114);
		kryo.writeObject(
				output,
				original);
		output.close();
		final Input input = new Input(
				((ByteArrayOutputStream) output.getOutputStream()).toByteArray());
		final SimpleFeature deserialized = kryo.readObject(
				input,
				SimpleFeatureImpl.class);
		Assert.assertTrue(deserialized != null);
		for (final String attribute : ALL_ATTRIBS) {
			Assert.assertTrue(original.getAttribute(
					attribute).equals(
					deserialized.getAttribute(attribute)));
		}
	}

	private SimpleFeature buildSimpleFeature(
			final String state,
			final String city,
			final double lng,
			final double lat,
			final int munincipalPop ) {
		final Coordinate pointCoord = new Coordinate(
				lng,
				lat);
		builder.set(
				LOCATION_ATTRIB,
				GeometryUtils.GEOMETRY_FACTORY.createPoint(pointCoord));
		builder.set(
				STATE_ATTRIB,
				state);
		builder.set(
				CITY_ATTRIB,
				city);
		builder.set(
				MUNINCIPAL_POP_ATTRIB,
				munincipalPop);
		builder.set(
				RANDOM_UUID_ATTRIB,
				UUID.randomUUID());
		builder.set(
				CREATED_DATE_ATTRIB,
				new java.util.Date());
		builder.set(
				CREATED_SQL_DATE_ATTRIB,
				new java.sql.Date(
						System.currentTimeMillis()));
		builder.set(
				TIME_ATTRIB,
				new java.sql.Time(
						System.currentTimeMillis()));
		builder.set(
				TIMESTAMP_ATTRIB,
				new java.sql.Timestamp(
						System.currentTimeMillis()));
		builder.set(
				POLYGON_ATTRIB,
				GeometryUtils.GEOMETRY_FACTORY.createPolygon(new Coordinate[] {
					pointCoord,
					new Coordinate(
							0,
							0),
					new Coordinate(
							10,
							10),
					pointCoord
				}));
		return builder.buildFeature(UUID.randomUUID().toString());
	}
}