package org.broadinstitute.hellbender.tools.spark;

import htsjdk.samtools.util.IOUtil;
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

import java.io.File;
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

    @Override
    public String getTestedToolName() {
        return ParallelCopyGCSDirectoryIntoHDFSSpark.class.getSimpleName();
    }

    @Test(groups = {"spark", "bucket"})
    public void testCopyFile() throws Exception {
        // copy a multi-block file
        final Path tempPath = MiniClusterUtils.getTempPath(cluster, "test", "dir");
        final String gcpInputPath = getGCPTestInputPath() + "large/dbsnp_138.b37.1.1-65M.vcf";
        String args =
                "--inputGCSPath " + gcpInputPath +
                        " --apiKey " + getGCPTestApiKey() +
                        " --outputHDFSDirectory " + tempPath;
        ArgumentsBuilder ab = new ArgumentsBuilder().add(args);
        IntegrationTestSpec spec = new IntegrationTestSpec(
                ab.getString(),
                Collections.emptyList());
        spec.executeTest("testCopyFile-" + args, this);

        final String hdfsPath = tempPath + "/" + "dbsnp_138.b37.1.1-65M.vcf";
        Assert.assertEquals(BucketUtils.fileSize(hdfsPath, getAuthenticatedPipelineOptions()),
                BucketUtils.fileSize(gcpInputPath, getAuthenticatedPipelineOptions()));

        BucketUtils.copyFile(hdfsPath, getAuthenticatedPipelineOptions(), publicTestDir + "fileFromHDFS.vcf");
        BucketUtils.copyFile(gcpInputPath, getAuthenticatedPipelineOptions(), publicTestDir + "fileFromGCS.vcf");
        IOUtil.assertFilesEqual(new File(publicTestDir + "fileFromHDFS.vcf"), new File(publicTestDir + "fileFromGCS.vcf"));
    }
}
