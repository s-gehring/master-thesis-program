package gehring.uima.distributed.exceptions;

public class SparkException extends SharedUimaProcessorException {

    private static final long serialVersionUID = 8159029755343863984L;

    public SparkException(final String message) {
        super(message);
    }

    public SparkException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
