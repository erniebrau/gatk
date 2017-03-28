package org.broadinstitute.hellbender.tools.spark;

import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.SparkProgramGroup;
import org.broadinstitute.hellbender.engine.AuthHolder;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import scala.Tuple2;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@CommandLineProgramProperties(summary="Parallel copy a big file from GCS into HDFS",
        oneLineSummary="Parallel copy a big file from GCS into HDFS.",
        programGroup = SparkProgramGroup.class)

public class ReadBigFileFromGCSIntoHDFSSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;

    @Argument(doc = "input GCS file path", fullName = "inputGCSPath")
    private String inputGCSPath = null;


    @Argument(doc = "output file on HDFS", shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME)
    private String outputFile;


    @Override
    protected void runTool(final JavaSparkContext ctx) {

        final AuthHolder auth = getAuthHolder();
        final String inputGCSPathFinal = inputGCSPath;


        try {

            Path file = new Path(outputFile);
            final String basename = file.getName();
            FileSystem fs = file.getFileSystem(new Configuration());
            fs.createNewFile(file);

            final long chunkSize = Long.parseLong(fs.getConf().get("dfs.blocksize"));

            final long size = BucketUtils.fileSize(inputGCSPath, auth.asPipelineOptionsDeprecated());
            final long chunks = size / chunkSize + 1;

            List<Integer> chunkList = new ArrayList<>((int) chunks);
            for (int i = 0; i < chunks; i++) {
                chunkList.add(i);
            }

            final JavaRDD<Integer> chunkRDD = ctx.parallelize(chunkList, chunkList.size());

            final JavaRDD<Tuple2<Integer, String>> chunkMappingRDD =
                    chunkRDD.map(chunkNum -> readChunkToHdfs(auth, inputGCSPathFinal, chunkSize, chunkNum, basename));

            final List<Tuple2<Integer, String>> chunkListLocal = chunkMappingRDD.collect();

            Map<Integer, String> chunkMap = new HashMap<>();
            for (Tuple2<Integer, String> entry : chunkListLocal) {
                chunkMap.put(entry._1(), entry._2());
            }

            Path[] chunkPaths = new Path[chunkListLocal.size()];

            for (int i = 0; i < chunks; i++) {
                final String chunkPath = chunkMap.get(i);
                chunkPaths[i] = new Path(chunkPath);
            }


            fs.concat(file, chunkPaths);

        } catch (IOException e) {
            throw new GATKException(e.getMessage(), e);
        }

    }

    private static final Tuple2<Integer, String> readChunkToHdfs(final AuthHolder authHolder, final String inputGCSPathFinal, final long chunkSize, final Integer chunkNum, final String basename) {
        final String path = "hdfs://tmp/basename.chunk." + chunkNum;
        try {
            final SeekableByteChannel channel = new GcsUtil.GcsUtilFactory().create(authHolder.asPipelineOptionsDeprecated()).open(GcsPath.fromUri(inputGCSPathFinal));
            final long start = chunkSize * (long) chunkNum;
            channel.position(start);
            final OutputStream outputStream = new BufferedOutputStream(BucketUtils.createFile(path, authHolder));
            final int bufferSize = 512;
            ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
            long bytesRead = 0;
            while(channel.read(byteBuffer) > 0) {
                byteBuffer.flip();
                while (byteBuffer.hasRemaining() && bytesRead < chunkSize) {
                    byte b = byteBuffer.get();
                    outputStream.write(b);
                    bytesRead++;
                }
                if (bytesRead >= chunkSize) {
                    break;
                }
                byteBuffer.clear();
            }
            outputStream.close();
            channel.close();
        } catch (IOException e) {
            throw new GATKException(e.getMessage(), e);
        }
        return new Tuple2<>(chunkNum, path);
    }

}
