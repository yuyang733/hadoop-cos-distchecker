package com.qcloud.cos.hadoop.distchecker;

public enum CheckResult {
    SUCCESS("The source file and the target file are the same."),
    MISMATCH("The source file and target file are different."),
    UNCONFIRM("Can not confirm if the source file and the target file are the same."),
    UNCHECKED("The source file is inaccessible and its MD5 checksum."),
    SOURCE_FILE_MISSING("The source file is missing."),
    TARGET_FILE_MISSING("The target file is missing."),
    TARGET_FILESYSTEM_ERROR("The target file system is error.");

    private String description;

    CheckResult(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return "CheckResult{" +
                "description='" + description + '\'' +
                '}';
    }
}
