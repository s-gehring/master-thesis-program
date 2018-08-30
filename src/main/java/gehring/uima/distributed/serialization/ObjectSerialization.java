package gehring.uima.distributed.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public abstract class ObjectSerialization implements CasSerialization {

    private static final long serialVersionUID = -7154617795820988751L;

    protected static byte[] serialize(final Object object) {
        if (!(object instanceof Serializable)) {
            throw new RuntimeException("Object (" + object.toString() + ") not serializable.");
        }
        byte[] serialized;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            ObjectOutput out = null;

            try {
                out = new ObjectOutputStream(bos);
                out.writeObject(object);
                out.flush();
            } catch (IOException e) {
                throw new RuntimeException("Error serialization object.", e);
            }

            serialized = bos.toByteArray();
        } catch (IOException e1) {
            throw new RuntimeException(
                    "Error closing byte array output stream while serialization.", e1);
        }
        return serialized;
    }

    protected static Serializable deserialize(final byte[] data) {
        Object result;
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data)) {

            try {
                ObjectInput objectReader = new ObjectInputStream(inputStream);
                result = objectReader.readObject();
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException("Error deserializing object from byte data.", e);
            }

        } catch (IOException e1) {
            throw new RuntimeException(
                    "Error closing byte array output stream while deserialization.", e1);
        }
        if (!(result instanceof Serializable)) {
            // Woot? Add confusion to runtime exception.
            throw new RuntimeException("The... just deserialized object is not... serializable.");
        }
        return (Serializable) result;
    }
}
