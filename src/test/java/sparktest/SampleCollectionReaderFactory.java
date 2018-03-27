package sparktest;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.resource.ResourceInitializationException;
import org.dkpro.core.io.tika.TikaReader;

public class SampleCollectionReaderFactory {
	private SampleCollectionReaderFactory() {
	}

	public static CollectionReaderDescription getTestFileReaderDescription() {
		CollectionReaderDescription result;
		try {
			File testFile = new File(SampleCollectionReaderFactory.class.getClassLoader()
					.getResource("Digitale Teilhabe.pdf").getPath());
			URL testFolder;
			try {
				testFolder = new File(testFile.getParent()).toURI().toURL();
			} catch (MalformedURLException e) {
				throw new RuntimeException("Can't form URL off '" + testFile.getParent() + "'.", e);
			}
			result = CollectionReaderFactory.createReaderDescription(TikaReader.class, TikaReader.PARAM_SOURCE_LOCATION,
					testFolder.toString(), TikaReader.PARAM_PATTERNS, "*.pdf");
		} catch (ResourceInitializationException e) {
			throw new RuntimeException("Failed to create the collection reader description for test files.", e);
		}
		return result;
	}
}
