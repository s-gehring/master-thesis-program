package gehring.uima.distributed;

import gehring.uima.distributed.exceptions.SharedUimaProcessorException;
import gehring.uima.distributed.exceptions.UimaException;

import java.io.IOException;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.xml.sax.SAXException;

public class XmiSerializedCAS {

    private SerializedCAS             sCas;
    private AnalysisEngineDescription pipelineDescription;

    public XmiSerializedCAS(final SerializedCAS sCas,
            final AnalysisEngineDescription pipelineDescription) {
        this.sCas = sCas;
        this.pipelineDescription = pipelineDescription;
    }

    @Override
    public String toString() {
        CAS resultCas = this.sCas.getCAS(this.pipelineDescription);
        String result;
        try (ByteArrayOutputStream oStream = new ByteArrayOutputStream()) {
            XmiCasSerializer.serialize(resultCas, oStream);
            result = oStream.toString("UTF-8");
        } catch (SAXException e) {
            throw new UimaException("Error serializing CAS into an XMI file.", e);
        } catch (IOException e) {
            throw new SharedUimaProcessorException("Error closing XMI output stream.", e);
        }
        return result;
    }
}
