package com.qcloud.cos.hadoop.distchecker;

import org.apache.hadoop.fs.Path;

import java.net.URI;

public final class PathUtils {

    public static Path convertSourcePathToTargetPath(Path sourcePath, Path sourceWorkingDirPath, Path targetWorkingDirPath) {
        URI uri = targetWorkingDirPath.toUri();

        // 先全部转成相对路径，即是不带前面的host和port
        Path sourceFileRelativePath = new Path(sourcePath.toUri().getPath());
        Path sourceWorkingDirRelativePath = new Path(sourceWorkingDirPath.toUri().getPath());
        Path targetWorkingDirRelativePath = new Path(targetWorkingDirPath.toUri().getPath());

        // 去掉目录的路径后缀
        if (sourceWorkingDirRelativePath.toString().endsWith("/")) {
            sourceWorkingDirRelativePath = new Path(
                    sourceWorkingDirRelativePath.toString().substring(
                            0, sourceWorkingDirRelativePath.toString().length() - 1));
        }
        if (targetWorkingDirRelativePath.toString().endsWith("/")) {
            targetWorkingDirRelativePath = new Path(
                    targetWorkingDirRelativePath.toString().substring(
                            0, targetWorkingDirRelativePath.toString().length() - 1));
        }

        // 根据源工作目录查找源路径的相对路径
        Path sourceFileParentRelativePath = sourceFileRelativePath.getParent();
        while (!sourceFileParentRelativePath.isRoot() && !sourceFileParentRelativePath.equals(sourceWorkingDirRelativePath)) {
            sourceFileParentRelativePath = sourceFileParentRelativePath.getParent();
        }

        Path targetFilePath;        // 最终的目的路径
        if (sourceFileParentRelativePath.isRoot()) {
            targetFilePath = new Path(targetWorkingDirRelativePath.toString() + sourceFileRelativePath.toString());
        } else {
            targetFilePath = new Path(sourceFileRelativePath.toString().replaceFirst(sourceFileParentRelativePath.toString(),
                    targetWorkingDirPath.toString()));
        }

        targetFilePath = targetFilePath.makeQualified(targetWorkingDirPath.toUri(), targetWorkingDirPath);

        return targetFilePath;
    }
}
