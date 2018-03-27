package sparktest.compression;

import org.apache.commons.io.output.ByteArrayOutputStream;

public class NoCompression implements CompressionAlgorithm {
	private static NoCompression instance = null;

	private NoCompression() {
		// Singleton
	}
	public synchronized static NoCompression getInstance() {
		return instance == null ? instance = new NoCompression() : instance;
	}

	@Override
	public ByteArrayOutputStream compress(final ByteArrayOutputStream input) {
		return input;
	}

	@Override
	public ByteArrayOutputStream decompress(final ByteArrayOutputStream input) {
		return input;
	}

}
