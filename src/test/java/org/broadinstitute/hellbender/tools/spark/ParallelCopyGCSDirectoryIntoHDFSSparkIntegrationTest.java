package org.broadinstitute.hellbender.tools.spark;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import org.broadinstitute.hellbender.utils.test.ArgumentsBuilder;
import org.broadinstitute.hellbender.utils.test.IntegrationTestSpec;
import org.broadinstitute.hellbender.utils.test.MiniClusterUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;


public class ParallelCopyGCSDirectoryIntoHDFSSparkIntegrationTest extends CommandLineProgramTest {

    private MiniDFSCluster cluster;

    @BeforeClass(alwaysRun = true)
    private void setupMiniCluster() throws IOException {
        cluster = MiniClusterUtils.getMiniCluster();
    }

    @AfterClass(alwaysRun = true)
    private void shutdownMiniCluster() {
        MiniClusterUtils.stopCluster(cluster);
    }

    @Test(groups = {"spark", "bucket"})
    public void testCopyDir() throws Exception {
        final Path tempPath = MiniClusterUtils.getTempPath(cluster, "test", "dir");
        String args =
                    "--inputGCSDirectory " + getGCPTestInputPath() + "huge" +
                            " --apiKey " + getGCPTestApiKey() +
                            " --outputHDFSDirectory " + tempPath;
            ArgumentsBuilder ab = new ArgumentsBuilder().add(args);
            IntegrationTestSpec spec = new IntegrationTestSpec(
                    ab.getString(),
                    Collections.emptyList());
            spec.executeTest("testCopyDir-" + args, this);

            Assert.assertEquals(BucketUtils.fileSize(tempPath + "/" + "dbsnp_138.hg19.vcf", getAuthenticatedPipelineOptions()),
                    BucketUtils.fileSize(getGCPTestInputPath() + "huge/" + "dbsnp_138.hg19.vcf", getAuthenticatedPipelineOptions()));
        }
}
