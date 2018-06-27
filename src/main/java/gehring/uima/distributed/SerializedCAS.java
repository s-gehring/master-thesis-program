package gehring.uima.distributed;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.CasCreationUtils;
import org.xml.sax.SAXException;

public class SerializedCAS implements Serializable {

	private static final Logger LOGGER = Logger.getLogger(SerializedCAS.class);
	private transient String preview = null;
	private byte[] content;
	private static final int MAX_PREVIEW_LENGTH = 250;

	private void generatePreview(final CAS cas) {
		if (this.preview != null) {
			return;
		}
		if (this.content == null) {
			this.preview = null;
		}
		String docText = cas.getDocumentText();
		int docLength = docText.length();
		if (docLength < MAX_PREVIEW_LENGTH) {
			this.preview = docText;
		} else {
			this.preview = docText.substring(0, 247) + "...";
		}
	}

	public SerializedCAS(final CAS cas) {
		LOGGER.info("Serializing CAS...");
		if (cas == null) {
			this.content = null;
			return;
		}
		this.generatePreview(cas);
		try (ByteArrayOutputStream casBytes = new ByteArrayOutputStream()) {
			XmiCasSerializer.serialize(cas, casBytes);
			this.content = casBytes.toByteArray();
		} catch (IOException e) {
			LOGGER.warn("Error closing temporary output stream.", e);
		} catch (SAXException e) {
			System.out.println(cas.getDocumentText());
			this.preview = cas.getDocumentText().substring(0,
					cas.getDocumentText().length() > 250 ? 250 : cas.getDocumentText().length());
			throw new RuntimeException("Error serializing cas into bytes.", e);
		}
		LOGGER.info("Done serializing CAS.");
	}

	public void populateCAS(final CAS cas) {
		if (this.content == null) {
			throw new NullPointerException("Can't populate CAS, since the serialized CAS was null.");
		}
		try (InputStream casBytes = new ByteArrayInputStream(this.content)) {
			LOGGER.info("Trying to deserialize CAS...");
			XmiCasDeserializer.deserialize(casBytes, cas);
			LOGGER.info("Done deserializing CAS.");
		} catch (IOException e) {
			LOGGER.warn("Error closing temporary input stream.", e);
		} catch (SAXException e) {
			throw new RuntimeException("Error deserializing bytes into CAS.", e);
		}

	}

	public CAS getCAS(final AnalysisEngine pipeline) {
		CAS cas;
		try {
			cas = pipeline.newCAS();
		} catch (ResourceInitializationException e) {
			throw new RuntimeException("Failed to generate CAS on deserialization.", e);
		}
		this.populateCAS(cas);
		return cas;
	}

	public CAS getCAS(final AnalysisEngineDescription pipelineDescription) {
		CAS targetCas;
		try {
			targetCas = CasCreationUtils.createCas(pipelineDescription);
		} catch (ResourceInitializationException e1) {
			throw new RuntimeException("Failed to instantiate a new CAS.", e1);
		}

		try (InputStream casBytes = new ByteArrayInputStream(this.content)) {

			XmiCasDeserializer.deserialize(casBytes, targetCas);

		} catch (IOException e) {
			LOGGER.warn("Error closing temporary input stream.", e);
		} catch (SAXException e) {
			throw new RuntimeException("Error deserializing bytes into CAS.", e);
		}
		return targetCas;
	}

	protected byte[] getSerializedContent() {
		return this.content;
	}

	@Override
	public String toString() {

		return "Serialized CAS (excerpt: " + (this.preview == null ? "<null>" : this.preview) + ")";
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj instanceof SerializedCAS) {
			SerializedCAS cas = (SerializedCAS) obj;
			return Arrays.equals(cas.content, this.content);
		}

		return false;
	}

	@Override
	public int hashCode() {
		return this.content.hashCode();
	}
}