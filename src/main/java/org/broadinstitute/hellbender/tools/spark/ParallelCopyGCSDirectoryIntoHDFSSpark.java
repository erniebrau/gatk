package org.broadinstitute.hellbender.tools.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.programgroups.SparkProgramGroup;
import org.broadinstitute.hellbender.engine.AuthHolder;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import org.broadinstitute.hellbender.utils.io.IOUtils;
import scala.Tuple2;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This tool uses Spark to do a parallel copy of either a file or a directory from GCS into HDFS.
 * Files are divided into chunks of size equal to the HDFS block size (with the exception of the final
 * chunk) and each Spark task is responsible for copying one chunk. To copy all of the files in a GCS directory,
 * provide the GCS directory path, including the trailing slash. Directory copies are non-recursive so
 * subdirectories will be skipped. Within directories each file is divided into chunks independently (so this will be
 * inefficient if you have lots of files smaller than the block size). After all chunks are copied, the HDFS
 * concat method is used to stitch together chunks into single files without re-copying them.
 */
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

        org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(outputHDFSDirectory);

        try(FileSystem fs = file.getFileSystem(new Configuration())){

            fs.mkdirs(file);
            final long chunkSize = Long.parseLong(fs.getConf().get("dfs.blocksize"));

            final List<Path> gcsPaths;
            final Path inputGCSNIOPath = IOUtils.getPath(inputGCSPathFinal);
            if (Files.isDirectory(inputGCSNIOPath)) {
                logger.warn("directory: " + inputGCSPathFinal);
                gcsPaths = Files.list(inputGCSNIOPath).collect(Collectors.toList());
            } else {
                logger.warn("single file");
                gcsPaths = Collections.singletonList(inputGCSNIOPath);
            }

            List<Tuple2<String, Integer>> chunkList = new ArrayList<>();
            for (Path path : gcsPaths) {

                if (Files.isDirectory(path)) {
                    logger.info("skipping directory " + path);
                    continue;
                }

                final long size = Files.size(path);
                final long chunks = size / chunkSize + 1;
                logger.info("processing path " + path + ", size = " + size + ", chunks = " + chunks);

                for (int i = 0; i < chunks; i++) {
                    chunkList.add(new Tuple2<>(path.toUri().toString(), i));
                }
            }

            if (chunkList.size() == 0) {
                logger.info("no files found to copy");
                return;
            }

            final JavaPairRDD<String, Integer> chunkRDD = ctx.parallelizePairs(chunkList, chunkList.size());

            final JavaPairRDD<String, Tuple2<Integer, String>> chunkMappingRDD =
                    chunkRDD.mapToPair(p -> new Tuple2<>(p._1(), readChunkToHdfs(auth, p._1(), chunkSize, p._2(), outputDirectoryFinal)));

            final Map<String, Iterable<Tuple2<Integer, String>>> chunksByFilePath = chunkMappingRDD.groupByKey().collectAsMap();

            for (Path path : gcsPaths) {

                if (Files.isDirectory(path)) {
                    continue;
                }

                final String filePath = path.toUri().toString();
                final Iterable<Tuple2<Integer, String>> chunkListForFile = chunksByFilePath.get(filePath);
                final String basename = path.getName(path.getNameCount() - 1).toString();
                final org.apache.hadoop.fs.Path outFilePath = new org.apache.hadoop.fs.Path(outputDirectoryFinal + "/" + basename);
                fs.createNewFile(outFilePath);

                SortedMap<Integer, String> chunkMap = new TreeMap<>();
                for (Tuple2<Integer, String> entry : chunkListForFile) {
                    chunkMap.put(entry._1(), entry._2());
                }

                org.apache.hadoop.fs.Path[] chunkPaths = new org.apache.hadoop.fs.Path[chunkMap.size()];

                final Iterator<Integer> iterator = chunkMap.keySet().iterator();
                while (iterator.hasNext()) {
                    final Integer next =  iterator.next();
                    final String chunkPath = chunkMap.get(next);
                    chunkPaths[next] = new org.apache.hadoop.fs.Path(chunkPath);
                }

                fs.concat(outFilePath, chunkPaths);
            }



        } catch (IOException e) {
            throw new GATKException(e.getMessage(), e);
        }

    }

    private static final Tuple2<Integer, String> readChunkToHdfs(final AuthHolder authHolder, final String inputGCSPathFinal, final long chunkSize, final Integer chunkNum, final String outputDirectory) {
        final Path gcsPath = IOUtils.getPath(inputGCSPathFinal);
        final String basename = gcsPath.getName(gcsPath.getNameCount() - 1).toString();
        org.apache.hadoop.fs.Path outputPath = new org.apache.hadoop.fs.Path(outputDirectory);
        final String chunkPath = outputPath + "/" + basename + ".chunk." + chunkNum;

        try (SeekableByteChannel channel = Files.newByteChannel(gcsPath);
             final OutputStream outputStream = new BufferedOutputStream(BucketUtils.createFile(chunkPath, authHolder))){

            final long start = chunkSize * (long) chunkNum;
            channel.position(start);
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
        } catch (IOException e) {
            throw new GATKException(e.getMessage() + "; inputGCSPathFinal = " + inputGCSPathFinal, e);
        }
        return new Tuple2<>(chunkNum, chunkPath);
    }

}
