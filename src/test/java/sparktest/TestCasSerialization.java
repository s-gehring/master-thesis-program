package sparktest;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.cas.CAS;
import org.junit.Before;
import org.junit.Test;

import sparktest.equals.CasEquals;
import sparktest.equals.NotEqualException;

public class TestCasSerialization {

	private static CAS finishedCas = null;
	private AnalysisEngine pipeline;

	@Before
	public void setupFinishedCas() throws UIMAException {
		AnalysisEngine pipeline = SamplePipelineFactory.getNewPipeline();
		CAS cas = pipeline.newCAS();
		cas.setDocumentLanguage("en");
		cas.setDocumentText(
				"The Sun is the star at the center of the Solar System. It is a nearly perfect sphere of hot plasma,[14][15] with internal convective motion that generates a magnetic field via a dynamo process.[16] It is by far the most important source of energy for life on Earth. Its diameter is about 1.39 million kilometers, i.e. 109 times that of Earth, and its mass is about 330,000 times that of Earth, accounting for about 99.86% of the total mass of the Solar System.[17] About three quarters of the Sun's mass consists of hydrogen (~73%); the rest is mostly helium (~25%), with much smaller quantities of heavier elements, including oxygen, carbon, neon, and iron.[18]\n"
						+ "The Sun is a G-type main-sequence star (G2V) based on its spectral class. As such, it is informally referred to as a yellow dwarf. It formed approximately 4.6 billion[a][10][19] years ago from the gravitational collapse of matter within a region of a large molecular cloud. Most of this matter gathered in the center, whereas the rest flattened into an orbiting disk that became the Solar System. The central mass became so hot and dense that it eventually initiated nuclear fusion in its core. It is thought that almost all stars form by this process.\n"
						+ "The Sun is roughly middle-aged; it has not changed dramatically for more than four billion[a] years, and will remain fairly stable for more than another five billion years. After hydrogen fusion in its core has diminished to the point at which it is no longer in hydrostatic equilibrium, the core of the Sun will experience a marked increase in density and temperature while its outer layers expand to eventually become a red giant. It is calculated that the Sun will become sufficiently large to engulf the current orbits of Mercury and Venus, and render Earth uninhabitable.\n"
						+ "The enormous effect of the Sun on Earth has been recognized since prehistoric times, and the Sun has been regarded by some cultures as a deity. The synodic rotation of Earth and its orbit around the Sun are the basis of solar calendars, one of which is the predominant calendar in use today.");
		pipeline.process(cas);
		this.pipeline = pipeline;
		finishedCas = cas;
	}

	private File getTempFile() throws IOException {
		String simpleName = this.getClass().getSimpleName();
		if (simpleName.isEmpty()) {
			simpleName = "(anonymous)";
		}
		File f = File.createTempFile(simpleName, ".tmp");
		f.deleteOnExit();
		return f;
	}

	private static String getCASDifference(final SerializedCAS leftCas, final SerializedCAS rightCas) {
		if (leftCas == rightCas || (leftCas != null && leftCas.equals(rightCas))) {
			return null;
		}
		if (leftCas == null) {
			return "First is null, seconds is not.";
		}
		if (rightCas == null) {
			return "Second is null, first is not.";
		}

		byte[] leftCasBytes = leftCas.getSerializedContent();
		byte[] rightCasBytes = rightCas.getSerializedContent();
		int correctBytes = 0;
		int byteNumber = Math.min(leftCasBytes.length, rightCasBytes.length);
		for (int i = 0; i < byteNumber; ++i) {
			if (leftCasBytes[i] == rightCasBytes[i]) {
				++correctBytes;
			}
		}
		if (leftCasBytes.length != rightCasBytes.length) {
			return "Wrong sizes. First size is " + rightCasBytes.length + "B, the second " + leftCasBytes.length + "B.";
		}
		if ((byteNumber - correctBytes) == 0) {
			return null;
		}
		return "There were differences in " + (byteNumber - correctBytes) + "B.";

	}
	/*
	 * Is s⁻¹(s(x)) == s⁻¹(s(s⁻¹(s(x))))?
	 */
	@Test
	public void testSerializationWeak() throws IOException, ClassNotFoundException, NotEqualException {
		SerializedCAS firstOutCas = new SerializedCAS(finishedCas);
		CAS firstRun = firstOutCas.getCAS(this.pipeline);
		SerializedCAS secondOutCas = new SerializedCAS(firstRun);
		CAS secondRun = secondOutCas.getCAS(this.pipeline);

		CasEquals.equalsCas(firstRun, secondRun);

	}

	/*
	 * Is s⁻¹(s(x)) == x?
	 */
	@Test
	public void testSerializationIntoFile() throws IOException, ClassNotFoundException, NotEqualException {
		SerializedCAS outCas = new SerializedCAS(finishedCas);
		SerializedCAS inCas;
		File f = this.getTempFile();
		try (ObjectOutputStream outStream = new ObjectOutputStream(new FileOutputStream(f))) {
			outStream.writeObject(outCas);
		}

		try (ObjectInputStream inStream = new ObjectInputStream(new FileInputStream(f))) {
			inCas = (SerializedCAS) inStream.readObject();
		}
		if (!inCas.equals(outCas)) {
			String msg = getCASDifference(inCas, outCas);
			assertTrue("Serialized and deserialized sCAS are not equal (" + msg + ").", msg == null);

		}

		CasEquals.equalsCas(outCas.getCAS(this.pipeline), finishedCas);

	}

	public static void main(final String[] argv) throws Exception {
		TestCasSerialization x = new TestCasSerialization();
		x.testSerializationWeak();
		x.testSerializationIntoFile();
	}
}
