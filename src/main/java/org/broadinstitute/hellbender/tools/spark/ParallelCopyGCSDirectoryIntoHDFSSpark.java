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

@CommandLineProgramProperties(summary="Parallel copy a file or directory (non-recursive) from GCS into HDFS",
        oneLineSummary="Parallel copy a file or directory from GCS into HDFS.",
        programGroup = SparkProgramGroup.class)

public class ParallelCopyGCSDirectoryIntoHDFSSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;

    @Argument(doc = "input GCS file path (add trailing slash when specifying a directory)", fullName = "inputGCSPath")
    private String inputGCSPath = null;


    @Argument(doc = "output directory on HDFS", shortName = "outputHDFSDirectory",
            fullName = "outputHDFSDirectory")
    private String outputHDFSDirectory;


    @Override
    protected void runTool(final JavaSparkContext ctx) {

        final AuthHolder auth = getAuthHolder();
        final String inputGCSPathFinal = inputGCSPath;
        final String outputDirectoryFinal = outputHDFSDirectory;

        try {

            Path file = new Path(outputHDFSDirectory);
            FileSystem fs = file.getFileSystem(new Configuration());
            fs.mkdirs(file);

            final long chunkSize = Long.parseLong(fs.getConf().get("dfs.blocksize"));

            final GcsUtil gcsUtil = new GcsUtil.GcsUtilFactory().create(auth.asPipelineOptionsDeprecated());

            final List<GcsPath> gcsPaths;
            if (inputGCSPath.endsWith("/")) {
                gcsPaths = gcsUtil.expand(GcsPath.fromUri(inputGCSPathFinal + "*"));
            } else {
                gcsPaths = Collections.singletonList(GcsPath.fromUri(inputGCSPathFinal));
            }

            List<Tuple2<String, Integer>> chunkList = new ArrayList<>();
            for (GcsPath path : gcsPaths) {

                final String filePath = path.toString();
                if (path.endsWith("/")) {
                    logger.info("skipping directory " + path);
                    continue;
                }
                final long size = BucketUtils.fileSize(filePath, auth.asPipelineOptionsDeprecated());
                final long chunks = size / chunkSize + 1;
                logger.info("processing path " + path + ", size = " + size + ", chunks = " + chunks);

                for (int i = 0; i < chunks; i++) {
                    chunkList.add(new Tuple2<>(filePath, i));
                }
            }

            if (chunkList.size() == 0) {
                logger.info("no files found to copy");
                return;
            }

            final JavaPairRDD<String, Integer> chunkRDD = ctx.parallelizePairs(chunkList, chunkList.size());

            final String fsURI = fs.getUri().toString();
            logger.warn("FS URI: "+ fsURI);

            final JavaPairRDD<String, Tuple2<Integer, String>> chunkMappingRDD =
                    chunkRDD.mapToPair(p -> new Tuple2<>(p._1(), readChunkToHdfs(auth, p._1(), chunkSize, p._2(), outputDirectoryFinal)));

            final Map<String, Iterable<Tuple2<Integer, String>>> chunksByFilePath = chunkMappingRDD.groupByKey().collectAsMap();

            for (GcsPath path : gcsPaths) {

                final String filePath = path.toString();

                final Iterable<Tuple2<Integer, String>> chunkListForFile = chunksByFilePath.get(filePath);
                final String basename = path.getName(path.getNameCount() - 1).toString();
                final Path outFilePath = new Path(outputDirectoryFinal + "/" + basename);
                fs.createNewFile(outFilePath);

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

                fs.concat(outFilePath, chunkPaths);
            }



        } catch (IOException e) {
            throw new GATKException(e.getMessage(), e);
        }

    }

    private static final Tuple2<Integer, String> readChunkToHdfs(final AuthHolder authHolder, final String inputGCSPathFinal, final long chunkSize, final Integer chunkNum, final String outputDirectory) {
        final GcsPath gcsPath = GcsPath.fromUri(inputGCSPathFinal);
        final String basename = gcsPath.getName(gcsPath.getNameCount() - 1).toString();
        Path outputPath = new Path(outputDirectory);
        final String path = outputPath + "/" + basename + ".chunk." + chunkNum;
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
