package com.qcloud.cos.hadoop.distchecker;

import java.io.Serializable;

public class CheckReport implements Serializable {

    private final String sourceFilePath;
    private final String destFilePath;
    private final String checkSumAlgorithm;
    private final String sourceChecksum;
    private final String targetChecksum;
    private final CheckResult checkResult;

    public CheckReport(String sourceFilePath, String destFilePath, String checkSumAlgorithm, String checksum,
                       String targetChecksum, CheckResult checkResult) {
        this.sourceFilePath = sourceFilePath == null ? "None" : sourceFilePath;
        this.destFilePath = destFilePath == null ? "None" : destFilePath;
        this.checkSumAlgorithm = checkSumAlgorithm == null ? "None" : checkSumAlgorithm;
        this.sourceChecksum = checksum == null ? "None" : checksum;
        this.targetChecksum = targetChecksum == null ? "None" : targetChecksum;
        this.checkResult = checkResult == null ? CheckResult.UNCHECKED : checkResult;
    }

    public String getSourceFilePath() {
        return sourceFilePath;
    }

    public String getDestFilePath() {
        return destFilePath;
    }

    public String getCheckSumAlgorithm() {
        return checkSumAlgorithm;
    }

    public String getSourceChecksum() {
        return sourceChecksum;
    }

    public String getTargetChecksum() {
        return targetChecksum;
    }

    public CheckResult getCheckResult() {
        return checkResult;
    }

    @Override
    public String toString() {
        return sourceFilePath + "," + destFilePath + "," + checkSumAlgorithm + "," + sourceChecksum + "," + targetChecksum + "," + checkResult.name() + "," + "'" + checkResult.getDescription() + "'";
    }
}
