package gehring.uima.distributed.serialization;

import java.io.Serializable;

import org.apache.uima.cas.CAS;

public interface CasSerialization extends Serializable {
	public byte[] serialize(CAS cas);

	public CAS deserialize(byte[] data, CAS cas);
}
