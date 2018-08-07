package gehring.uima.distributed.compression;

public class NoCompression implements CompressionAlgorithm {
	private static NoCompression instance = null;

	private NoCompression() {
		// Singleton
	}
	public synchronized static NoCompression getInstance() {
		return instance == null ? instance = new NoCompression() : instance;
	}

	@Override
	public byte[] compress(final byte[] input) {
		return input;
	}

	@Override
	public byte[] decompress(final byte[] input) {
		return input;
	}

}
