package sparktest.equals;

public class NotCorrectException extends Exception {

	public NotCorrectException(final String msg) {
		super(msg);
	}
	public NotCorrectException(final String msg, final Throwable cause) {
		super(msg, cause);
	}
}
