package gehring.uima.distributed.compression;

import java.io.Serializable;

public interface CompressionAlgorithm extends Serializable {
	public byte[] compress(final byte[] input);
	public byte[] decompress(final byte[] input);
}
