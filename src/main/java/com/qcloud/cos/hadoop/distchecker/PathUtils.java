package com.qcloud.cos.hadoop.distchecker;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public final class PathUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PathUtils.class);

    public static Path convertSourcePathToTargetPath(Path sourcePath, Path sourceWorkingDirPath, Path targetWorkingDirPath) {
        // 先全部转成相对路径，即是不带前面的host和port
        Path sourceFileRelativePath = new Path(sourcePath.toUri().getPath());
        Path sourceWorkingDirRelativePath = new Path(sourceWorkingDirPath.toUri().getPath());
        Path targetWorkingDirRelativePath = new Path(targetWorkingDirPath.toUri().getPath());

        // 根据源工作目录查找源路径的相对路径
        Path sourceFileParentRelativePath = sourceFileRelativePath.getParent();
        while (!sourceFileParentRelativePath.isRoot() && !sourceFileParentRelativePath.equals(sourceWorkingDirRelativePath)) {
            sourceFileParentRelativePath = sourceFileParentRelativePath.getParent();
        }

        Path targetFilePath;        // 最终的目的路径
        if (sourceFileParentRelativePath.isRoot()) {
            Path tmpRelativePath = sourceFileRelativePath;
            if (sourceFileRelativePath.toString().startsWith("/")) {
                tmpRelativePath = new Path(sourceFileRelativePath.toString().substring(1));
            }
            targetFilePath = new Path(targetWorkingDirRelativePath, tmpRelativePath);
        } else {
            targetFilePath = new Path(sourceFileRelativePath.toString().replaceFirst(sourceFileParentRelativePath.toString(),
                    targetWorkingDirPath.toString()));
        }

        targetFilePath = targetFilePath.makeQualified(targetWorkingDirPath.toUri(), targetWorkingDirPath);

        return targetFilePath;
    }
}
