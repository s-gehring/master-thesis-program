package gehring.uima.distributed.compression;

import java.io.Serializable;

import org.apache.commons.io.output.ByteArrayOutputStream;

public interface CompressionAlgorithm extends Serializable {
	public ByteArrayOutputStream compress(final ByteArrayOutputStream input);
	public ByteArrayOutputStream decompress(final byte[] input);
}
