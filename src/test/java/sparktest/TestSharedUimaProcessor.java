package sparktest;

import static org.junit.Assert.fail;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionReaderDescription;
import org.junit.Test;

public class TestSharedUimaProcessor {

	@Test
	public void TestProcessor() {
		CollectionReaderDescription reader = SampleCollectionReaderFactory.getTestFileReaderDescription();
		AnalysisEngineDescription pipeline = SamplePipelineFactory.getNewPipelineDescription();

		SparkConf configuration = new SparkConf().setMaster("local[8]")
				.setAppName(this.getClass().getName() + " (Test)");

		SharedUimaProcessor processor = new SharedUimaProcessor(configuration);
		Iterator<CAS> results = processor.process(reader, pipeline);

		if (!results.hasNext()) {
			fail("No results found.");
		}
		int i = 0;
		while (results.hasNext()) {
			CAS currentResult = results.next();
			System.out.println("Result [" + (++i) + "]: " + currentResult);
		}
	}
}
