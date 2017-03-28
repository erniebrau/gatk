package org.broadinstitute.hellbender.tools.spark;

import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.programgroups.SparkProgramGroup;
import org.broadinstitute.hellbender.engine.AuthHolder;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import scala.Tuple2;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.*;

@CommandLineProgramProperties(summary="Parallel copy a big file from GCS into HDFS",
        oneLineSummary="Parallel copy a big file from GCS into HDFS.",
        programGroup = SparkProgramGroup.class)

public class ParallelCopyGCSDirectoryIntoHDFSSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;

    @Argument(doc = "input GCS file path", fullName = "inputGCSDirectory")
    private String inputGCSDirectory = null;


    @Argument(doc = "output directory on HDFS", shortName = "outputHDFSDirectory",
            fullName = "outputHDFSDirectory")
    private String outputHDFSDirectory;


    @Override
    protected void runTool(final JavaSparkContext ctx) {

        final AuthHolder auth = getAuthHolder();
        final String inputGCSPathFinal = inputGCSDirectory;
        final String outputDirectoryFinal = outputHDFSDirectory;


        try {

            Path file = new Path(outputHDFSDirectory);
            FileSystem fs = file.getFileSystem(new Configuration());
            fs.mkdirs(file);

            final long chunkSize = Long.parseLong(fs.getConf().get("dfs.blocksize"));

            final GcsUtil gcsUtil = new GcsUtil.GcsUtilFactory().create(auth.asPipelineOptionsDeprecated());

            final List<GcsPath> gcsPaths = gcsUtil.expand(GcsPath.fromUri(inputGCSPathFinal + "*"));
            List<Tuple2<String, Integer>> chunkList = new ArrayList<>();
            for (GcsPath path : gcsPaths) {

                final String filePath = path.toString();

                final long size = BucketUtils.fileSize(filePath, auth.asPipelineOptionsDeprecated());
                final long chunks = size / chunkSize + 1;

                for (int i = 0; i < chunks; i++) {
                    chunkList.add(new Tuple2<>(filePath, i));
                }
            }

            final JavaPairRDD<String, Integer> chunkRDD = ctx.parallelizePairs(chunkList, chunkList.size());

            final JavaPairRDD<String, Tuple2<Integer, String>> chunkMappingRDD =
                    chunkRDD.mapValues(chunkNum -> readChunkToHdfs(auth, inputGCSPathFinal, chunkSize, chunkNum, outputDirectoryFinal));

            final Map<String, Iterable<Tuple2<Integer, String>>> chunksByFilePath = chunkMappingRDD.groupByKey().collectAsMap();

            for (GcsPath path : gcsPaths) {

                final String filePath = path.toString();

                final Iterable<Tuple2<Integer, String>> chunkListForFile = chunksByFilePath.get(filePath);
                final String basename = path.toFile().getName();
                fs.createNewFile(new Path(outputDirectoryFinal + "/" + basename));

                SortedMap<Integer, String> chunkMap = new TreeMap<>();
                for (Tuple2<Integer, String> entry : chunkListForFile) {
                    chunkMap.put(entry._1(), entry._2());
                }

                Path[] chunkPaths = new Path[chunkMap.size()];

                final Iterator<Integer> iterator = chunkMap.keySet().iterator();
                while (iterator.hasNext()) {
                    final Integer next =  iterator.next();
                    final String chunkPath = chunkMap.get(next);
                    chunkPaths[next] = new Path(chunkPath);
                }

                fs.concat(file, chunkPaths);
            }



        } catch (IOException e) {
            throw new GATKException(e.getMessage(), e);
        }

    }

    private static final Tuple2<Integer, String> readChunkToHdfs(final AuthHolder authHolder, final String inputGCSPathFinal, final long chunkSize, final Integer chunkNum, final String outputDirectory) {
        final GcsPath gcsPath = GcsPath.fromUri(inputGCSPathFinal);
        final String basename = gcsPath.toFile().getName();
        Path outputPath = new Path(outputDirectory);
        final String path = "hdfs://" + outputPath.getName() + "/" + basename + ".chunk." + chunkNum;
        try {

            final SeekableByteChannel channel = new GcsUtil.GcsUtilFactory().create(authHolder.asPipelineOptionsDeprecated()).open(gcsPath);
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
