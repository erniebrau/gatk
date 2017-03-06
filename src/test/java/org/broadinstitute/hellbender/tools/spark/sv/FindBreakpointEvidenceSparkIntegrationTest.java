package org.broadinstitute.hellbender.tools.spark.sv;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.utils.test.ArgumentsBuilder;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.broadinstitute.hellbender.utils.test.IntegrationTestSpec;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Integration test on the SV pipeline as it exists right now [2017-03-06]
 */
public class FindBreakpointEvidenceSparkIntegrationTest extends CommandLineProgramTest {

    private static final String THIS_TEST_FOLDER = getTestDataDir() + "/" + "spark/sv/IntegrationTests";
    private static final String TEST_BAM_LEFT = THIS_TEST_FOLDER + "/" + "assembly13016.bam";
    private static final String TEST_BAM_RIGHT = THIS_TEST_FOLDER + "/" + "assembly13017.bam";
    private static final String KMER_KILL_LIST = THIS_TEST_FOLDER + "/" + "dummy.kill.kmers";


    private static final class SvPipelineSparkTest {
        final String referenceLoc = b37_reference_20_21;
        final String bamLoc;
        final String kmerIgnoreListLoc;
        final String outputDir;

        SvPipelineSparkTest(final String bamLoc,
                            final String kmerIgnoreListLoc,
                            final String outputDir) {
            this.bamLoc = bamLoc;
            this.kmerIgnoreListLoc = kmerIgnoreListLoc;
            this.outputDir = outputDir;
        }

        String getCommandLineNoApiKey() {
            return  " -R " + referenceLoc +
                    " -I " + bamLoc +
                    " -O " + outputDir +
                    " --kmersToIgnore " + kmerIgnoreListLoc +
                    " --kmerIntervals " + outputDir + "/" + "kmerIntervals" +
                    " --breakpointEvidenceDir " + outputDir + "/" + "evidence" +
                    " --breakpointIntervals " + outputDir + "/" + "intervals" +
                    " --qnameIntervalsMapped " + outputDir + "/" + "qnameIntervalsMapped" +
                    " --qnameIntervalsForAssembly " + outputDir + "/" + "qnameIntervalsForAssembly" ;
        }

        String getCommandLine() {
            return  getCommandLineNoApiKey() +
                    " --apiKey " + getGCPTestApiKey();
        }
    }

    @DataProvider(name = "SvPipelineTest")
    public Object[][] createTestData() {

        List<Object[]> tests = new ArrayList<>();
        final File tempDirLeft = BaseTest.createTempDir("forLeft");
        tempDirLeft.deleteOnExit();
        tests.add(new Object[]{new SvPipelineSparkTest(TEST_BAM_LEFT, KMER_KILL_LIST, tempDirLeft.getAbsolutePath())});
        final File tempDirRight = BaseTest.createTempDir("forLeft");
        tempDirRight.deleteOnExit();
        tests.add(new Object[]{new SvPipelineSparkTest(TEST_BAM_RIGHT, KMER_KILL_LIST, tempDirRight.getAbsolutePath())});

        return tests.toArray(new Object[][]{});
    }

    @Test(dataProvider = "SvPipelineTest", groups = "spark")
    public void testSvPipelineSpark(FindBreakpointEvidenceSparkIntegrationTest.SvPipelineSparkTest params) throws IOException {

        @SuppressWarnings("unchecked")
        final List<String> expectedFileNames = new ArrayList<>(Collections.EMPTY_LIST);

        new IntegrationTestSpec(
                new ArgumentsBuilder().add(params.getCommandLineNoApiKey()).getString(),
                expectedFileNames)
                .executeTest("testFindBreakpointEvidenceSpark-", this);
    }
}
