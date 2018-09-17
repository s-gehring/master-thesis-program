package gehring.uima.distributed.serialization;

import gehring.uima.distributed.exceptions.SerializationException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

public abstract class ObjectSerialization implements CasSerialization {

    private static final long serialVersionUID = -7154617795820988751L;

    private static void flushToOutputStream(final OutputStream bos, final Object object) {
        try (ObjectOutput out = new ObjectOutputStream(bos)) {

            out.writeObject(object);
            out.flush();
        } catch (IOException e) {
            throw new SerializationException("Error serialization object.", e);
        }

    }

    protected static byte[] serialize(final Object object) {
        if (!(object instanceof Serializable)) {
            throw new SerializationException(
                    "Object (" + object.toString() + ") not serializable.");
        }
        byte[] serialized;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {

            flushToOutputStream(bos, object);

            serialized = bos.toByteArray();
        } catch (IOException e1) {
            throw new SerializationException(
                    "Error closing byte array output stream while serialization.", e1);
        }
        return serialized;
    }

    private static Object readIntoObject(final InputStream inputStream) {

        try (ObjectInput objectReader = new ObjectInputStream(inputStream)) {

            return objectReader.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new SerializationException("Error deserializing object from byte data.", e);
        }

    }

    protected static Serializable deserialize(final byte[] data) {
        Object result;
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data)) {

            result = readIntoObject(inputStream);

        } catch (IOException e1) {
            throw new SerializationException(
                    "Error closing byte array output stream while deserialization.", e1);
        }
        if (!(result instanceof Serializable)) {
            // Woot? Add confusion to runtime exception.
            throw new SerializationException(
                    "The... just deserialized object is not... serializable.");
        }
        return (Serializable) result;
    }
}
