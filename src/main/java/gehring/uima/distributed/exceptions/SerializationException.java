package gehring.uima.distributed.exceptions;

public class SerializationException extends SharedUimaProcessorException {

    private static final long serialVersionUID = -6237599671803332433L;

    public SerializationException(final String message) {
        super(message);
    }

    public SerializationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
