package mil.nga.giat.geowave.examples.ingest;

import org.junit.Test;

public class SimpleIngestIndexWriterTest extends
		SimpleHBaseIngestTest
{
	@Test
	public void TestIngest() {
		final SimpleIngestIndexWriter si = new SimpleIngestIndexWriter();
		si.generateGrid(mockDataStore);
		validate(mockDataStore);
	}
}