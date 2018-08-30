package gehring.uima.distributed.compression;

import gehring.uima.distributed.exceptions.SharedUimaProcessorException;

import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.apache.commons.io.output.ByteArrayOutputStream;

public class ZLib implements CompressionAlgorithm {

    private static final long serialVersionUID = -5166264145257234592L;
    private static ZLib       instance         = null;

    public static synchronized ZLib getInstance() {
        if (instance == null) {
            instance = new ZLib();
        }
        return instance;

    }

    private ZLib() {
    }

    @Override
    public byte[] compress(final byte[] input) {
        Deflater deflater = new Deflater();
        deflater.setInput(input);
        byte[] result;
        try (ByteArrayOutputStream compressedOutput = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            deflater.finish();
            while (!deflater.finished()) {
                int processedBytes = deflater.deflate(buffer);

                compressedOutput.write(buffer, 0, processedBytes);
            }
            result = compressedOutput.toByteArray();
        } catch (IOException e) {
            throw new SharedUimaProcessorException(
                    "Error closing byte array output stream while compressing.",
                    e);
        }
        return result;
    }

    private int inflate(final Inflater inflater, final byte[] buffer) {
        try {
            return inflater.inflate(buffer);
        } catch (DataFormatException e) {
            throw new SharedUimaProcessorException("Failed to decompress given data.", e);
        }
    }

    @Override
    public byte[] decompress(final byte[] input) {
        Inflater inflater = new Inflater();
        inflater.setInput(input);
        byte[] result;
        try (ByteArrayOutputStream decompressedOutput = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];

            while (!inflater.finished()) {
                int processedBytes = inflate(inflater, buffer);
                decompressedOutput.write(buffer, 0, processedBytes);
            }
            result = decompressedOutput.toByteArray();
        } catch (IOException e1) {
            throw new SharedUimaProcessorException(
                    "Error closing byte array output stream while decompressing.", e1);
        }
        return result;
    }

}
