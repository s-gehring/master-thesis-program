package sparktest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.admin.CASFactory;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.xml.sax.SAXException;

public class SerializedCAS implements Serializable {

	private transient static final Logger LOGGER = Logger.getLogger(SerializedCAS.class);
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
	}

	public void populateCAS(final CAS cas) {
		if (this.content == null) {
			throw new NullPointerException("Can't populate CAS, since the serialized CAS was null.");
		}

		try (InputStream casBytes = new ByteArrayInputStream(this.content)) {

			XmiCasDeserializer.deserialize(casBytes, cas);
		} catch (IOException e) {
			LOGGER.warn("Error closing temporary input stream.", e);
		} catch (SAXException e) {
			throw new RuntimeException("Error deserializing bytes into CAS.", e);
		}

	}

	public CAS getCAS() {
		if (this.content == null) {
			return null;
		}
		CAS target = null;
		try (InputStream casBytes = new ByteArrayInputStream(this.content)) {
			target = CASFactory.createCAS().getCAS();
			XmiCasDeserializer.deserialize(casBytes, target);

		} catch (IOException e) {
			LOGGER.warn("Error closing temporary input stream.", e);
		} catch (SAXException e) {
			throw new RuntimeException("Error deserializing bytes into CAS.", e);
		}
		return target;
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
			return cas.getCAS().equals(this.getCAS());
		}
		return false;
	}

	@Override
	public int hashCode() {
		return this.content.hashCode();
	}
}
