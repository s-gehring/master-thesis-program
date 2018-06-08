package sparktest.benchmark;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionReaderDescription;

import sparktest.SharedUimaProcessor;

public class Benchmarks {
	public static BenchmarkResult benchmark(final CollectionReaderDescription reader,
			final AnalysisEngineDescription pipeline, final SparkConf configuration) {

		BenchmarkResult benchmark = new BenchmarkResult();

		benchmark.startMeasurement("initialization");
		SharedUimaProcessor processor = new SharedUimaProcessor(configuration);
		benchmark.endMeasurement("initialization");
		benchmark.startMeasurement("analysis");
		Iterator<CAS> results = processor.process(reader, pipeline);
		benchmark.endMeasurement("analysis");

		if (!results.hasNext()) {
			System.exit(1);
		}
		int i = 0;

		int casSum = 0, docSum = 0;
		while (results.hasNext()) {
			CAS currentResult = results.next();
			++i;
			casSum = casSum + currentResult.size();
			docSum = docSum + currentResult.getDocumentText().length();
		}
		BenchmarkMetadata meta = new BenchmarkMetadata(i, casSum, docSum);

		benchmark.setMetadata(meta);
		return benchmark;
	}
}
