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

@SuppressWarnings("static-method")
public class TestCasSerialization {

	private static CAS finishedCas = null;

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

	@Test
	public void testSerialization() throws IOException, ClassNotFoundException {
		SerializedCAS outCas = new SerializedCAS(finishedCas);
		SerializedCAS inCas;
		File f = this.getTempFile();
		try (ObjectOutputStream outStream = new ObjectOutputStream(new FileOutputStream(f))) {
			outStream.writeObject(outCas);
		}

		try (ObjectInputStream inStream = new ObjectInputStream(new FileInputStream(f))) {
			inCas = (SerializedCAS) inStream.readObject();
		}
		assertTrue("Serialized and deserialized sCAS are not equal.", inCas.equals(outCas));
		assertTrue("Serialized and deserialized CAS are not equal.", finishedCas.equals(outCas.getCAS()));
	}
}
