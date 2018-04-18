package sparktest.equals;

import java.util.Collection;
import java.util.StringJoiner;

public class NotContainedException extends NotCorrectException {

	private Collection<NotEqualException> causes;

	public NotContainedException(final String msg, final Collection<NotEqualException> causes) {
		super(msg);
		this.causes = causes;
	}

	@Override
	public String toString() {
		StringJoiner joiner = new StringJoiner("\n\t", "\n[\n\t", "\n]");
		String superb = super.toString();
		for (NotEqualException cause : this.causes) {
			joiner.add(cause.getLocalizedMessage());
		}

		return superb + " " + joiner.toString();
	}

}
