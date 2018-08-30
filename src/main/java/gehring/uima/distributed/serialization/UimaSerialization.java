package gehring.uima.distributed.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.Serialization;

public class UimaSerialization extends ObjectSerialization implements CasSerialization {

    private static final long        serialVersionUID = -4067203196992423699L;
    private static UimaSerialization instance;

    public synchronized static UimaSerialization getInstance() {
        return instance == null ? instance = new UimaSerialization() : instance;
    }

    private UimaSerialization() {
        // Singleton
    }

    @Override
    public byte[] serialize(final CAS cas) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Serialization.serializeCAS(cas, out);
        return out.toByteArray();
    }

    @Override
    public CAS deserialize(final byte[] data, final CAS cas) {
        ByteArrayInputStream iStream = new ByteArrayInputStream(data);
        Serialization.deserializeCAS(cas, iStream);
        return cas;
    }

}
