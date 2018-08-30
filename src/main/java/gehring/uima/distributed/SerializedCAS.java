package gehring.uima.distributed;

import gehring.uima.distributed.compression.CompressionAlgorithm;
import gehring.uima.distributed.compression.NoCompression;
import gehring.uima.distributed.exceptions.UimaException;
import gehring.uima.distributed.serialization.CasSerialization;
import gehring.uima.distributed.serialization.XmiCasSerialization;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.CAS;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.CasCreationUtils;

public class SerializedCAS implements Serializable {

    private static final long    serialVersionUID = 3057205660621559722L;
    private static final Logger  LOGGER           = Logger.getLogger(SerializedCAS.class);
    private byte[]               content;
    private CompressionAlgorithm compression;
    private CasSerialization     serialization;

    public SerializedCAS(final CAS cas) {
        this(cas, NoCompression.getInstance(), XmiCasSerialization.getInstance());
    }

    public int size() {
        if (this.content == null) {
            return 0;
        }
        return this.content.length;
    }

    public SerializedCAS(final CAS cas, final CompressionAlgorithm compressionAlgorithm,
            final CasSerialization serializationAlgorithm) {

        if (cas == null) {
            this.content = null;
            return;
        }
        if (compressionAlgorithm == null) {
            LOGGER.warn("Calling CAS serialization with null compression. Use "
                    + NoCompression.class.getName()
                    + " instead.");
            this.compression = NoCompression.getInstance();
        } else {
            this.compression = compressionAlgorithm;
        }
        if (serializationAlgorithm == null) {
            LOGGER.warn("Calling CAS serialization with null serialization. Use "
                    + XmiCasSerialization.class.getName()
                    + " instead.");
            this.serialization = XmiCasSerialization.getInstance();
        } else {
            this.serialization = serializationAlgorithm;
        }

        byte[] serialized = this.serialization.serialize(cas);
        this.content = this.compression.compress(serialized);

        LOGGER.trace("Done serializing CAS.");
    }

    public void populateCAS(final CAS cas) {
        if (this.content == null) {
            throw new NullPointerException(
                    "Can't populate CAS, since the serialized CAS was null.");
        }

        byte[] uncompressedContent = this.compression.decompress(this.content);
        this.serialization.deserialize(uncompressedContent, cas);

    }

    public CAS getCAS(final AnalysisEngine pipeline) {
        CAS cas;
        try {
            cas = pipeline.newCAS();
        } catch (ResourceInitializationException e) {
            throw new UimaException("Failed to generate CAS on deserialization.", e);
        }
        this.populateCAS(cas);
        return cas;
    }

    public CAS getCAS(final AnalysisEngineDescription pipelineDescription) {
        CAS targetCas;
        try {
            targetCas = CasCreationUtils.createCas(pipelineDescription);
        } catch (ResourceInitializationException e1) {
            throw new UimaException("Failed to instantiate a new CAS.", e1);
        }
        this.populateCAS(targetCas);
        return targetCas;
    }

    protected byte[] getSerializedContent() {
        return this.content;
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
        return Arrays.hashCode(this.content);
    }
}
