package gehring.uima.distributed.exceptions;

public class SharedUimaProcessorException extends RuntimeException {

    private static final long serialVersionUID = -6237699671803332433L;

    public SharedUimaProcessorException(final String message) {
        super(message);
    }

    public SharedUimaProcessorException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
