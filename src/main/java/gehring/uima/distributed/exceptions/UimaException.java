package gehring.uima.distributed.exceptions;

public class UimaException extends RuntimeException {

    private static final long serialVersionUID = 5606060402283790267L;

    public UimaException(final String message) {
        super(message);
    }

    public UimaException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
