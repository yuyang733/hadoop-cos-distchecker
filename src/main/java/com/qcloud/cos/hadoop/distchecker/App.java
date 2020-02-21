package com.qcloud.cos.hadoop.distchecker;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class App extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    private static final String name = "hadoop-cos-distChecker";

    private static final int argsNumber = 4;

    public String usage() {
        return "[source directory] [source file list] [target directory] [result output path]";
    }

    public static void main(String[] args) {
        try {
            int result = ToolRunner.run(new App(), args);
            if (result != 0) {
                String errMessage = String.format("Job: %s execute failed. result code: %d.", App.name, result);
                System.err.println(errMessage);
            }
        } catch (Exception e) {
            LOG.error("{} job failed. exception: {}.", App.name, e);
        }
    }

    // 检查源目录和目的目录是否存在
    private void checkWorkDirExists(String workDir) throws IOException {
        Path workDirPath = new Path(workDir);
        FileSystem fileSystem = workDirPath.getFileSystem(this.getConf());
        if (!fileSystem.exists(workDirPath) && !fileSystem.isFile(workDirPath)) {
            throw new IOException("the work dir path is not exist.");
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < App.argsNumber) {
            System.out.println(usage());
            return -1;
        }

        String sourceDirectory = args[0];
        String sourceFileList = args[1];
        String targetDirectory = args[2];
        String resultOutputPath = args[3];

        // 检查源工作目录是否存在
        this.checkWorkDirExists(sourceDirectory);
        this.getConf().set("hadoop.cos.dist.checker.source.work.dir", sourceDirectory);

        // 检查目标工作目录是否存在
        this.checkWorkDirExists(targetDirectory);
        this.getConf().set("hadoop.cos.dist.checker.target.work.dir", targetDirectory);

        Job job = Job.getInstance(this.getConf(), App.name);
        job.setJarByClass(App.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(CheckMapper.class);
        job.setNumReduceTasks(0);               // 这个作业不需要reduce阶段

        FileInputFormat.addInputPath(job, new Path(sourceFileList));
        FileOutputFormat.setOutputPath(job, new Path(resultOutputPath));

        boolean status = job.waitForCompletion(true);

        return status ? 0 : 1;
    }
}
