package org.broadinstitute.hellbender.utils.test;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.utils.help.GATKHelpDoclet;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Smoke test to run doc gen on a subset of classes to make sure it doesn't regress.
 */
public class HelpSmokeTest extends CommandLineProgramTest {
    /**
     * Entry point for manually running the gatkDoc process on a subset of packages from within GATK.
     */
    private static String[] docTestPackages = {
            "org.broadinstitute.hellbender.cmdline.argumentcollections",
            "org.broadinstitute.hellbender.cmdline.GATKPlugin",
            "org.broadinstitute.hellbender.engine.filters",
            "org.broadinstitute.hellbender.tools",
            "org.broadinstitute.hellbender.tools.picard.analysis",
            "org.broadinstitute.hellbender.tools.picard.analysis.directed",
            "org.broadinstitute.hellbender.tools.picard.analysis.artifacts",
            "org.broadinstitute.hellbender.tools.picard.sam",
            "org.broadinstitute.hellbender.tools.picard.vcf",
            "org.broadinstitute.hellbender.tools.spark",
            "org.broadinstitute.hellbender.tools.spark.pipelines",
            "org.broadinstitute.hellbender.tools.spark.pipelines.metrics",
            "org.broadinstitute.hellbender.tools.spark.transforms.bqsr",
            "org.broadinstitute.hellbender.tools.spark.transforms.markduplicates",
            "org.broadinstitute.hellbender.tools.walkers.bqsr",
            "org.broadinstitute.hellbender.tools.walkers.vqsr",
            "org.broadinstitute.hellbender.tools.walkers.variantutils",
    };

    @Test
    public static void documentationSmokeTest() throws IOException {
        File docTestTarget = createTempDir("docgentest");
        String[] argArray = new String[]{
                "-doclet", GATKHelpDoclet.class.getName(),
                "-docletpath", "build/libs/",
                "-sourcepath", "src/main/java",
                "-settings-dir", "src/main/resources/org/broadinstitute/hellbender/utils/helpTemplates",
                "-d", docTestTarget.getAbsolutePath(), // directory must exist
                "-output-file-extension", "html",
                "-build-timestamp", "2016/11/11 11:11:11",
                "-absolute-version", "1.1-111",
                "-cp", System.getProperty("java.class.path"),
                "-verbose"
        };

        final List<String> docArgList = new ArrayList<>();
        docArgList.addAll(Arrays.asList(argArray));
        docArgList.addAll(Arrays.asList(docTestPackages));

        // This is  smoke test; we just want to make sure it doesn't blow up
        int success = com.sun.tools.javadoc.Main.execute(docArgList.toArray(new String[]{}));
        Assert.assertEquals(success, 0, "Failure processing gatkDoc via javadoc");
    }

}
