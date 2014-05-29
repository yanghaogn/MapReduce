import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
 import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;

public class SortByTemperatureUsingTotalOrderPartitioner extends Configured
        implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (conf == null) {
            return -1;
        }
        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(conf, true);
        
        conf.setPartitionerClass(TotalOrderPartitioner.class);
        
        InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<IntWritable, Text>(
                0.1, 10000, 10);
        Path input = FileInputFormat.getInputPaths(conf)[0];
        input = input.makeQualified(input.getFileSystem(conf));
        Path partitionFile = new Path(input, "_partitions");
        TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
        InputSampler.writePartitionFile(conf, sampler);
        
        
        // Add to DistributedCache
        URI partitionUri = new URI(partitionFile.toString() + "#_partitions");
        DistributedCache.addCacheFile(partitionUri, conf);
        DistributedCache.createSymlink(conf);
        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(
                new SortByTemperatureUsingTotalOrderPartitioner(), args);
        System.exit(exitCode);
    }
}