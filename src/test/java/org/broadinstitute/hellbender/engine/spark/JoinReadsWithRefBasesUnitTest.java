package org.broadinstitute.hellbender.engine.spark;

import com.google.api.services.genomics.model.Read;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.values.KV;
import htsjdk.samtools.SAMRecord;
import junit.framework.TestCase;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.engine.dataflow.ReadsPreprocessingPipelineTestData;
import org.broadinstitute.hellbender.engine.dataflow.datasources.RefWindowFunctions;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceDataflowSource;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.reference.ReferenceBases;
import org.broadinstitute.hellbender.utils.test.FakeReferenceSource;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class JoinReadsWithRefBasesUnitTest extends TestCase {
    @DataProvider(name = "bases")
    public Object[][] bases(){
        Object[][] data = new Object[2][];
        List<Class<?>> classes = Arrays.asList(Read.class, SAMRecord.class);
        for (int i = 0; i < classes.size(); ++i) {
            Class<?> c = classes.get(i);
            ReadsPreprocessingPipelineTestData testData = new ReadsPreprocessingPipelineTestData(c);

            List<GATKRead> reads = testData.getReads();
            List<SimpleInterval> intervals = testData.getAllIntervals();
            List<KV<GATKRead, ReferenceBases>> kvReadRefBases = testData.getKvReadsRefBases();
            data[i] = new Object[]{reads, kvReadRefBases, intervals};
        }
        return data;
    }

    @Test(dataProvider = "bases")
    public void refBasesTest(List<GATKRead> reads, List<KV<GATKRead, ReferenceBases>> kvReadRefBases,
                             List<SimpleInterval> intervals) throws IOException {
        JavaSparkContext ctx = SparkTestUtils.getTestContext();

        JavaRDD<GATKRead> rddReads = ctx.parallelize(reads);

        ReferenceDataflowSource mockSource = mock(ReferenceDataflowSource.class, withSettings().serializable());
        for (SimpleInterval i : intervals) {
            when(mockSource.getReferenceBases(any(PipelineOptions.class), eq(i))).thenReturn(FakeReferenceSource.bases(i));
        }
        when(mockSource.getReferenceWindowFunction()).thenReturn(RefWindowFunctions.IDENTITY_FUNCTION);

        JavaPairRDD<GATKRead, ReferenceBases> rddResult = JoinReadsWithRefBases.addBases(mockSource, rddReads);
        Map<GATKRead, ReferenceBases> result = rddResult.collectAsMap();

        for (KV<GATKRead, ReferenceBases> kv : kvReadRefBases) {
            ReferenceBases referenceBases = result.get(kv.getKey());
            Assert.assertNotNull(referenceBases);
            Assert.assertEquals(kv.getValue(),referenceBases);
        }
        ctx.close();
    }

}