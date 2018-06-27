package gehring.uima.distributed.compression;

import org.apache.commons.io.output.ByteArrayOutputStream;

public interface CompressionAlgorithm {
	public ByteArrayOutputStream compress(final ByteArrayOutputStream input);
	public ByteArrayOutputStream decompress(final ByteArrayOutputStream input);
}
