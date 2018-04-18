package sparktest.equals;

public class NotEqualException extends NotCorrectException {

	public NotEqualException(final String msg, final NotCorrectException cause) {
		super(msg, cause);
	}

	public NotEqualException(final String msg) {
		super(msg);
	}

}
