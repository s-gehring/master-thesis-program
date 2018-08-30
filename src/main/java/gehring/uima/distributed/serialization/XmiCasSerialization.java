package gehring.uima.distributed.serialization;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.xml.sax.SAXException;

public class XmiCasSerialization implements CasSerialization {

    private static final long          serialVersionUID = 3464307708976926424L;
    private static XmiCasSerialization instance;

    public synchronized static XmiCasSerialization getInstance() {
        return instance == null ? instance = new XmiCasSerialization() : instance;
    }

    private XmiCasSerialization() {
        // Singleton
    }

    @Override
    public byte[] serialize(final CAS cas) {

        try (ByteArrayOutputStream casBytes = new ByteArrayOutputStream()) {
            XmiCasSerializer.serialize(cas, casBytes);
            return casBytes.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException("Error closing temporary output stream.", e);
        } catch (SAXException e) {
            throw new RuntimeException("Error serializing CAS into bytes.", e);
        }

    }

    @Override
    public CAS deserialize(final byte[] data, final CAS cas) {
        try (InputStream casBytes = new ByteArrayInputStream(data)) {
            XmiCasDeserializer.deserialize(casBytes, cas);
            return cas;
        } catch (SAXException | IOException e) {
            throw new RuntimeException("Error deserializing bytes into CAS.", e);
        }

    }

}
