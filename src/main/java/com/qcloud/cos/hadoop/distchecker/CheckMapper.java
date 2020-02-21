package com.qcloud.cos.hadoop.distchecker;

import com.qcloud.cos.hadoop.distchecker.checksum.CRC64;
import com.qcloud.cos.hadoop.distchecker.checksum.utils.IOUtils;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.CheckedInputStream;

public class CheckMapper extends Mapper<Object, Text, Text, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(CheckMapper.class);

    private Configuration configuration;
    private FileSystem sourceFs = null;
    private Path sourceWorkingPath = null;
    private FileSystem targetFs = null;         // 目的文件系统
    private Path targetWorkingPath = null;      // 目的文件系统上的工作路径

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        this.configuration = context.getConfiguration();

        String sourceWorkingDir = this.configuration.get("hadoop.cos.dist.checker.source.work.dir");
        if (null == sourceWorkingDir || sourceWorkingDir.isEmpty()) {
            throw new IOException("The source work dir is not specified.");
        }
        this.sourceWorkingPath = new Path(sourceWorkingDir);
        this.sourceFs = this.sourceWorkingPath.getFileSystem(configuration);
        if (null == this.sourceFs) {
            throw new IOException(String.format("Can not get the source file system from the path: %s.",
                    this.sourceWorkingPath));
        }
        this.sourceFs.setWorkingDirectory(this.sourceWorkingPath);

        String targetWorkingDir = this.configuration.get("hadoop.cos.dist.checker.target.work.dir");
        if (null == targetWorkingDir || targetWorkingDir.isEmpty()) {
            throw new IOException("The target work dir is not specified.");
        }
        this.targetWorkingPath = new Path(targetWorkingDir);
        this.targetFs = this.targetWorkingPath.getFileSystem(configuration);
        if (null == this.targetFs) {
            throw new IOException(String.format("Can not get the target file system for the path: %s.",
                    this.targetWorkingPath));
        }
        this.targetFs.setWorkingDirectory(this.targetWorkingPath);
    }

    private void checkInputPathStr(String inputPathStr) throws IOException {
        if (null == inputPathStr) {
            throw new IOException("The input path is null.");
        }
        if (inputPathStr.isEmpty()) {
            throw new IOException("The input path is empty.");
        }

        // 检查input path和源工作路径是否属于同一个文件系统
        Path inputPath = new Path(inputPathStr);
        FileSystem fs = inputPath.getFileSystem(this.configuration);
        if (fs.getScheme().compareToIgnoreCase(this.sourceFs.getScheme()) != 0) {
            String exceptionMessage = String.format("The source path [%s] does not belong to the file system [%s].",
                    inputPathStr, this.sourceFs.getScheme());
            throw new IOException(exceptionMessage);
        }

        if (fs.getUri().getHost().compareToIgnoreCase(this.sourceFs.getUri().getHost()) != 0) {
            String exceptionMessage = String.format("The source path [%s] does not belong to the file system [%s].",
                    inputPathStr, this.sourceFs.getUri());
            throw new IOException(exceptionMessage);
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String sourceFilePathStr = value.toString().trim();
        try {
            // 这里不阻断下面的检查流程
            this.checkInputPathStr(sourceFilePathStr);
        } catch (IOException e) {
            LOG.warn("Check input path [{}] failed.", sourceFilePathStr, e);
            CheckReport checkReport = new CheckReport(
                    sourceFilePathStr,
                    null, null, null, null, CheckResult.UNCHECKED);
            context.write(new Text(sourceFilePathStr), new Text(checkReport.toString()));
            return;
        }

        Path sourceFilePath = new Path(sourceFilePathStr);

        if (!this.sourceFs.exists(sourceFilePath)) {
            CheckReport checkReport = new CheckReport(sourceFilePath.toString(), null, null, null, null,
                    CheckResult.SOURCE_FILE_MISSING);
            context.write(new Text(sourceFilePathStr), new Text(checkReport.toString()));
            return;
        }
        // 将源路径转换成目标文件系统上的路径
        Path targetFilePath = PathUtils.convertSourcePathToTargetPath(sourceFilePath,
                this.sourceWorkingPath, this.targetWorkingPath);
        LOG.info("The target file path: {}.", targetFilePath);

        // 首先判断目的路径是否存在
        if (!this.targetFs.exists(targetFilePath)) {
            // 目的路径不存在
            CheckReport checkReport = new CheckReport(sourceFilePath.toString(), targetFilePath.toString(), null,
                    null,
                    null,
                    CheckResult.TARGET_FILE_MISSING);
            context.write(new Text(sourceFilePathStr), new Text(checkReport.toString()));
            return;
        }

        // 源文件存在且目的文件也存在
        // 先判断长度是否相等
        FileStatus sourceFileStatus = sourceFs.getFileStatus(sourceFilePath);
        FileStatus targetFileStatus = targetFs.getFileStatus(targetFilePath);
        if (sourceFileStatus.getLen() != targetFileStatus.getLen()) {
            CheckReport checkReport = new CheckReport(sourceFilePath.toString(), targetFilePath.toString(), null,
                    null,
                    null,
                    CheckResult.MISMATCH);
            context.write(new Text(sourceFilePathStr), new Text(checkReport.toString()));
            return;
        }

        // 检查是否都是目录
        if (sourceFileStatus.isDirectory() && targetFileStatus.isDirectory()) {
            CheckReport checkReport = new CheckReport(sourceFilePath.toString(), targetFilePath.toString(), null,
                    null,
                    null,
                    CheckResult.SUCCESS);
            context.write(new Text(sourceFilePathStr), new Text(checkReport.toString()));
            return;
        } else if ((sourceFileStatus.isDirectory() && !targetFileStatus.isDirectory())
                || (!sourceFileStatus.isDirectory() && targetFileStatus.isDirectory())) {
            CheckReport checkReport = new CheckReport(sourceFilePath.toString(), targetFilePath.toString(), null,
                    null,
                    null,
                    CheckResult.MISMATCH);
            context.write(new Text(sourceFilePathStr), new Text(checkReport.toString()));
            return;
        }

        // 继续判断CRC64是否相等
        // TODO 这里要在加一些判断
        if (targetFileStatus instanceof CosNFileStatus) {
            if (null != ((CosNFileStatus) targetFileStatus).getCrc64ecma()) {
                LOG.info("Comparing the crc64 between the source file [{}] and the target file [{}].",
                        sourceFilePath, targetFilePath);
                BigInteger targetFileChecksum = new BigInteger(((CosNFileStatus) targetFileStatus).getCrc64ecma());
                // 使用CRC64来校验
                try (CheckedInputStream sourceFileInputStream =
                             new CheckedInputStream(sourceFs.open(sourceFilePath),
                                     new CRC64())) {
                    byte[] buffer = new byte[10 * 1024 * 1024];
                    while (sourceFileInputStream.read(buffer) != -1) ;
                    LOG.info("Finish compute the crc64.");
                    long sourceFileChecksum = IOUtils.getCRCValue(sourceFileInputStream);
                    if (sourceFileChecksum != targetFileChecksum.longValue()) {
                        CheckReport checkReport = new CheckReport(sourceFilePath.toString(),
                                targetFilePath.toString(), "CRC64",
                                String.valueOf(sourceFileChecksum),
                                String.valueOf(targetFileChecksum.longValue()),
                                CheckResult.MISMATCH);
                        context.write(new Text(sourceFilePathStr), new Text(checkReport.toString()));
                    } else {
                        CheckReport checkReport = new CheckReport(sourceFilePath.toString(),
                                targetFilePath.toString(), "CRC64",
                                String.valueOf(sourceFileChecksum),
                                String.valueOf(targetFileChecksum.longValue()),
                                CheckResult.SUCCESS);
                        context.write(new Text(sourceFilePathStr), new Text(checkReport.toString()));
                    }
                }
            } else if (null != ((CosNFileStatus) targetFileStatus).getETag()) {
                LOG.info("Comparing the MD5 Hash between the source file [{}] and dest file [{}]", sourceFilePath,
                        targetFilePath);
                // 使用MD5值进行计算
                String targetFileChecksum = ((CosNFileStatus) targetFileStatus).getETag();
                // 计算源文件的MD5值
                try (FSDataInputStream sourceFileInputStream = sourceFs.open(sourceFilePath)) {
                    MessageDigest messageDigest = null;

                    try {
                        messageDigest = MessageDigest.getInstance("MD5");
                    } catch (NoSuchAlgorithmException e) {
                        LOG.error("Can not get the MD5 checksum algorithm instance.");
                        CheckReport checkReport = new CheckReport(sourceFilePath.toString(),
                                targetFilePath.toString(), "MD5",
                                null,
                                targetFileChecksum,
                                CheckResult.UNCHECKED);
                        context.write(new Text(sourceFilePathStr), new Text(checkReport.toString()));
                        return;
                    }

                    DigestInputStream digestInputStream = new DigestInputStream(sourceFileInputStream,
                            messageDigest);
                    byte[] buffer = new byte[10 * 1024 * 1024];
                    while (digestInputStream.read(buffer) != -1) ;
                    LOG.info("finish compute the md5 hash.");
                    byte[] md5hash = messageDigest.digest();
                    if (Hex.encodeHexString(md5hash).compareToIgnoreCase(targetFileChecksum) != 0) {
                        CheckReport checkReport = new CheckReport(sourceFilePath.toString(),
                                targetFilePath.toString(), "MD5",
                                Hex.encodeHexString(md5hash),
                                targetFileChecksum,
                                CheckResult.UNCONFIRM);
                        context.write(new Text(sourceFilePathStr), new Text(checkReport.toString()));
                        return;
                    }
                    CheckReport checkReport = new CheckReport(sourceFilePath.toString(),
                            targetFilePath.toString(), "MD5",
                            Hex.encodeHexString(md5hash),
                            targetFileChecksum,
                            CheckResult.SUCCESS);
                    context.write(new Text(sourceFilePathStr), new Text(checkReport.toString()));
                }

            } else {
                CheckReport checkReport = new CheckReport(sourceFilePath.toString(),
                        targetFilePath.toString(), null,
                        null,
                        null,
                        CheckResult.UNCHECKED);
                context.write(new Text(sourceFilePathStr), new Text(checkReport.toString()));
            }
        } else {
            CheckReport checkReport = new CheckReport(sourceFilePath.toString(),
                    targetFilePath.toString(), null,
                    null,
                    null,
                    CheckResult.TARGET_FILESYSTEM_ERROR);
            context.write(new Text(sourceFilePathStr), new Text(checkReport.toString()));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        LOG.info("Begin to clean up the check mapper.");
        super.cleanup(context);
    }
}
