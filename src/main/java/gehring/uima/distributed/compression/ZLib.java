package gehring.uima.distributed.compression;

import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;

public class ZLib implements CompressionAlgorithm {
	private static final Logger LOGGER = Logger.getLogger(ZLib.class);

	private static ZLib instance = null;
	public synchronized static ZLib getInstance() {
		return instance == null ? instance = new ZLib() : instance;
	}

	private ZLib() {
	}

	@Override
	public ByteArrayOutputStream compress(final ByteArrayOutputStream input) {
		Deflater deflater = new Deflater();
		deflater.setInput(input.toByteArray());
		ByteArrayOutputStream compressedOutput = new ByteArrayOutputStream();
		byte[] buffer = new byte[1024];
		deflater.finish();
		while (!deflater.finished()) {
			int processedBytes = deflater.deflate(buffer);

			compressedOutput.write(buffer, 0, processedBytes);
		}
		try {
			input.close();
		} catch (IOException e) {
			LOGGER.warn("Failed to close input stream of uncompressed data.", e);
		}
		return compressedOutput;
	}

	@Override
	public ByteArrayOutputStream decompress(final byte[] input) {
		Inflater inflater = new Inflater();
		inflater.setInput(input);

		@SuppressWarnings("resource")
		ByteArrayOutputStream decompressedOutput = new ByteArrayOutputStream();
		byte[] buffer = new byte[1024];

		while (!inflater.finished()) {
			int processedBytes;
			try {
				processedBytes = inflater.inflate(buffer);
			} catch (DataFormatException e) {
				throw new RuntimeException("Failed to decompress given data.", e);
			}
			decompressedOutput.write(buffer, 0, processedBytes);
		}
		return decompressedOutput;
	}

}
