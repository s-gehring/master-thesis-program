package sparktest;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.text.AnnotationFS;
import org.apache.uima.cas.text.AnnotationIndex;

public class CasEquals {

	private static final Logger LOGGER = Logger.getLogger(CasEquals.class);
	private CasEquals() {

	}

	private static boolean arrayTypeEquals(final Type left, final Type right) {
		return true;
	}
	public static boolean equals(final Type left, final Type right) {
		if (left == right) {
			return true;
		}
		if (left == null || right == null) {
			LOGGER.info("One Type is null, the other isn't.");
			return false;
		}
		if (left.isArray() != right.isArray()) {
			LOGGER.info("One Type is an array, the other isn't.");
			return false;
		}
		if (left.isFeatureFinal() != right.isFeatureFinal()) {
			LOGGER.info("One Type is feature final, the other isn't.");
			return false;
		}
		if (left.isInheritanceFinal() != right.isInheritanceFinal()) {
			LOGGER.info("One Type is inheritance final, the other isn't.");
			return false;
		}
		if (left.isPrimitive() != right.isPrimitive()) {
			LOGGER.info("One Type is primitive, the other isn't.");
			return false;
		}
		if (right.isArray()) {
			return arrayTypeEquals(left, right);
		}
		if (left.getName() != right.getName()) {
			if (left.getName() == null) {
				LOGGER.info("One Type name is null, the other (" + right.getName() + ") isn't.");
				return false;
			}
			if (!left.getName().equals(right.getName())) {
				LOGGER.info("The Type names ('" + left.getName() + "' and '" + right.getName() + "') are not equal.");
				return false;
			}
		}
		if (left.getShortName() != right.getShortName()) {
			if (left.getShortName() == null) {
				LOGGER.info("One Type shortname is null, the other (" + right.getShortName() + ") isn't.");
				return false;
			}
			if (!left.getShortName().equals(right.getShortName())) {
				LOGGER.info("The Type shortnames ('" + left.getShortName() + "' and '" + right.getShortName()
						+ "') are not equal.");
				return false;
			}
		}
		if (left.getNumberOfFeatures() != right.getNumberOfFeatures()) {
			LOGGER.info("One Type has " + left.getNumberOfFeatures() + " features, the other has "
					+ right.getNumberOfFeatures() + ".");
			return false;
		}
		List<Feature> lhsFeatures = left.getFeatures();
		List<Feature> rhsFeatures = right.getFeatures();
		if (lhsFeatures != rhsFeatures) {
			if (lhsFeatures == null || rhsFeatures == null) {
				LOGGER.info("One Type has a null feature list, the other hasn't.");
				return false;
			}

			for (Feature feat : lhsFeatures) {
				if (!rhsFeatures.contains(feat)) {
					LOGGER.info("RHS doesn't contain '" + feat + "'.");

				}
			}
			LOGGER.info(rhsFeatures);
			return false;// IDENTICAL!!
		}
		return true;
	}

	public static boolean equalsAnnotationFS(final AnnotationFS left, final AnnotationFS right) {
		return left == right || (left != null && left.equals(right));
	}

	public static boolean equals(final AnnotationIndex<? extends AnnotationFS> left,
			final AnnotationIndex<? extends AnnotationFS> right) {
		if (left == right) {
			return true;
		}
		if (left == null || right == null) {
			return false;
		}
		for (AnnotationFS lhs : left) {
			if (!right.contains(lhs)) {
				LOGGER.info("The RHS doesn't contain " + lhs + ", which is contained in the LHS.");
				return false;
			}
		}
		if (left.getIndexingStrategy() != right.getIndexingStrategy()) {
			LOGGER.info("Indexing strategies are different. (" + left.getIndexingStrategy() + " vs. "
					+ right.getIndexingStrategy() + ")");
			return false;
		}
		if (left.getType() != right.getType()) {
			if (left.getType() == null) {
				LOGGER.info("One type is null, the other one isn't.");
				return false;
			}
			if (!equals(left.getType(), right.getType())) {
				LOGGER.info("Types are different.");
				return false;
			}
		}
		return true;
	}

	public static boolean equals(final CAS left, final CAS right) {
		if (left == right) {
			LOGGER.info("CAS are identical.");
			return false;
		}
		if (left == null || right == null) {
			LOGGER.info("One CAS is null, the other isn't.");
			return false;
		}
		if (!equals(left.getAnnotationIndex(), right.getAnnotationIndex())) {
			LOGGER.info("The annotation indexes are different.");
			return false;
		}
		if (!equalsAnnotationFS(left.getDocumentAnnotation(), right.getDocumentAnnotation())) {
			LOGGER.info("The documentAnnotations are different.");
			return false;
		}

		String lhsText = left.getDocumentText(), rhsText = right.getDocumentText();
		if (lhsText != rhsText) {
			if (lhsText != null && !lhsText.equals(rhsText)) {
				LOGGER.info("The SofA text is different.");
				return false;
			}
		}

		lhsText = left.getDocumentLanguage();
		rhsText = right.getDocumentLanguage();
		if (lhsText != rhsText) {
			if (lhsText != null && !lhsText.equals(rhsText)) {
				LOGGER.info("The SofA language is different.");
				return false;
			}
		}

		return true;
	}
}
