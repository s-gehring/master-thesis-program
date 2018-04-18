package sparktest.equals;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.text.AnnotationFS;
import org.apache.uima.cas.text.AnnotationIndex;

public class CasEquals {

	// private static final Logger LOGGER = Logger.getLogger(CasEquals.class);
	private CasEquals() {

	}

	public static void equalsFeature(final Feature left, final Feature right) throws NotEqualException {
		if (left == right) {
			return;
		}
		if (left == null || right == null) {
			throw new NotEqualException("One Feature is null, the other one is not.");
		}
		if (left.getName() != right.getName()) {
			if (left.getName() == null || right.getName() == null) {
				throw new NotEqualException("One Feature name is null, the other one is not.");
			}
			if (!left.getName().equals(right.getName())) {
				throw new NotEqualException("The Feature names are not equal.");
			}
		}

		if (left.getShortName() != right.getShortName()) {
			if (left.getShortName() == null || right.getShortName() == null) {
				throw new NotEqualException("One Feature short name is null, the other one is not.");
			}
			if (!left.getShortName().equals(right.getShortName())) {
				throw new NotEqualException("The Feature short names are not equal.");
			}
		}
		if (left.isMultipleReferencesAllowed() != right.isMultipleReferencesAllowed()) {
			throw new NotEqualException(
					"Features seem to be the same, but one allows multiple references, the other does not.");
		}

	}

	private static void containsFeature(final Collection<Feature> features, final Feature feat)
			throws NotContainedException {
		List<NotEqualException> wrongs = new LinkedList<NotEqualException>();
		for (Feature containedFeat : features) {
			try {
				equalsFeature(feat, containedFeat);
			} catch (NotEqualException e) {
				wrongs.add(e);
				continue;
			}
			return;
		}
		throw new NotContainedException("Feature '" + feat + "' is not contained.", wrongs);
	}

	public static void equalsType(final Type left, final Type right) throws NotEqualException {
		if (left == right) {
			return;
		}
		if (left == null || right == null) {
			throw new NotEqualException("One Type is null, the other isn't.");

		}
		if (left.isArray() != right.isArray()) {
			throw new NotEqualException("One Type is an array, the other isn't.");

		}
		if (left.isFeatureFinal() != right.isFeatureFinal()) {
			throw new NotEqualException("One Type is feature final, the other isn't.");

		}
		if (left.isInheritanceFinal() != right.isInheritanceFinal()) {
			throw new NotEqualException("One Type is inheritance final, the other isn't.");

		}
		if (left.isPrimitive() != right.isPrimitive()) {
			throw new NotEqualException("One Type is primitive, the other isn't.");

		}
		if (right.isArray()) {
			try {
				equalsType(left.getComponentType(), right.getComponentType());
			} catch (NotEqualException e) {
				throw new NotEqualException("Array component types are not equal.", e);
			}
		}
		if (left.getName() != right.getName()) {
			if (left.getName() == null) {
				throw new NotEqualException("One Type name is null, the other (" + right.getName() + ") isn't.");
			}
			if (!left.getName().equals(right.getName())) {
				throw new NotEqualException(
						"The Type names ('" + left.getName() + "' and '" + right.getName() + "') are not equal.");
			}
		}
		if (left.getShortName() != right.getShortName()) {
			if (left.getShortName() == null) {
				throw new NotEqualException(
						"One Type shortname is null, the other (" + right.getShortName() + ") isn't.");
			}
			if (!left.getShortName().equals(right.getShortName())) {
				throw new NotEqualException("The Type shortnames ('" + left.getShortName() + "' and '"
						+ right.getShortName() + "') are not equal.");
			}
		}
		if (left.getNumberOfFeatures() != right.getNumberOfFeatures()) {
			throw new NotEqualException("One Type has " + left.getNumberOfFeatures() + " features, the other has "
					+ right.getNumberOfFeatures() + ".");
		}
		List<Feature> lhsFeatures = left.getFeatures();
		List<Feature> rhsFeatures = right.getFeatures();
		if (lhsFeatures != rhsFeatures) {
			if (lhsFeatures == null || rhsFeatures == null) {
				throw new NotEqualException("One Type has a null feature list, the other hasn't.");
			}

			for (Feature feat : lhsFeatures) {
				try {
					containsFeature(rhsFeatures, feat);
				} catch (NotContainedException e) {

					throw new NotEqualException("RHS doesn't contain '" + feat + "'.", e);
				}
			}

		}

	}

	public static void equalsAnnotationFS(final AnnotationFS left, final AnnotationFS right) throws NotEqualException {
		if (left == right) {
			return;
		}
		if (left == null || right == null) {
			throw new NotEqualException("One AnnotationFS is null, the other one is not.");
		}
		if (left.getBegin() != right.getBegin()) {
			throw new NotEqualException("AnnotationFS don't begin at the same index. (" + left.getBegin() + " vs. "
					+ right.getBegin() + ")");
		}
		if (left.getEnd() != right.getEnd()) {
			throw new NotEqualException(
					"AnnotationFS don't end at the same index. (" + left.getEnd() + " vs. " + right.getEnd() + ")");
		}
		String covTextLeft = left.getCoveredText();
		String covTextRight = right.getCoveredText();
		if (covTextLeft != covTextRight) {
			if (covTextLeft == null || covTextRight == null) {
				throw new NotEqualException("One AnnotationFS has covered null text, while the other has not.");
			}
			if (!covTextLeft.equals(covTextRight)) {
				throw new NotEqualException("The AnnotationFS are not covering the same text.");
			}
		}
		try {
			equalsType(left.getType(), right.getType());
		} catch (NotEqualException e) {
			throw new NotEqualException("The Type of the AnnotationFS are not equal.", e);
		}

		/*
		 * One might think that the classes should be equal with equal AnnotationFS. However,
		 * since everything is AnnotationImpl anyway, we can't check for this equality.
		 * The class check is instead incorporated in the type check, which is the "class"
		 * of UIMA feature structures.
		 *
		 * @formatter:off
		 * Class<? extends AnnotationFS> leftClass = left.getClass();
		 * Class<? extends AnnotationFS> rightClass = right.getClass();
		 *
		 * if (!leftClass.equals(rightClass)) {
		 * 	   throw new NotEqualException("Two AnnotationFS are not of the same class. ("
		 * 	   + leftClass + " vs. " + rightClass + ")");
		 * }
		 *
		 * @formatter:on
		 */

	}

	private static void containsAnnotationFS(final AnnotationIndex<? extends AnnotationFS> index,
			final AnnotationFS featStructure) throws NotContainedException {
		List<NotEqualException> causes = new LinkedList<NotEqualException>();
		for (AnnotationFS containedFS : index) {
			try {
				equalsAnnotationFS(containedFS, featStructure);
				return;
			} catch (NotEqualException e) {
				causes.add(e);
			}
		}
		throw new NotContainedException("AnnotationFS " + featStructure.getClass() + " is not contained in index.",
				causes);
	}

	public static void equalsAnnotationIndex(final AnnotationIndex<? extends AnnotationFS> left,
			final AnnotationIndex<? extends AnnotationFS> right) throws NotEqualException {

		if (left == right) {
			return;
		}
		if (left == null || right == null) {
			throw new NotEqualException("One annotation index is null, the other is not.");
		}
		for (AnnotationFS lhs : left) {
			try {
				containsAnnotationFS(right, lhs);
			} catch (NotContainedException e) {
				throw new NotEqualException(
						"The RHS doesn't contain " + lhs.getClass() + ", which is contained in the LHS.", e);
			}

		}
		if (left.getIndexingStrategy() != right.getIndexingStrategy()) {
			throw new NotEqualException("Indexing strategies are different. (" + left.getIndexingStrategy() + " vs. "
					+ right.getIndexingStrategy() + ")");
		}
		if (left.getType() != right.getType()) {
			if (left.getType() == null) {
				throw new NotEqualException("One type is null, the other one isn't.");

			}
			try {
				equalsType(left.getType(), right.getType());
			} catch (NotEqualException e) {

				throw new NotEqualException("Types are different.", e);

			}
		}
	}

	public static void equalsCas(final CAS left, final CAS right) throws NotEqualException {
		if (left == right) {
			throw new NotEqualException("CAS are identical.");
		}
		if (left == null || right == null) {
			throw new NotEqualException("One CAS is null, the other isn't.");
		}
		try {
			equalsAnnotationIndex(left.getAnnotationIndex(), right.getAnnotationIndex());
		} catch (NotEqualException e) {
			throw new NotEqualException("The annotation indexes are different.", e);
		}
		try {
			equalsAnnotationFS(left.getDocumentAnnotation(), right.getDocumentAnnotation());
		} catch (NotEqualException e) {
			throw new NotEqualException("The documentAnnotations are different.", e);
		}

		String lhsText = left.getDocumentText(), rhsText = right.getDocumentText();
		if (lhsText != rhsText) {
			if (lhsText != null && !lhsText.equals(rhsText)) {
				throw new NotEqualException("The SofA text is different.");
			}
		}

		lhsText = left.getDocumentLanguage();
		rhsText = right.getDocumentLanguage();
		if (lhsText != rhsText) {
			if (lhsText == null) {
				throw new NotEqualException("One language is null, the other isn't.");
			}
			if (!lhsText.equals(rhsText)) {
				throw new NotEqualException("The SofA language is different.");
			}

		}

	}
}
