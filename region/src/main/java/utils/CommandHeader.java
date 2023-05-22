package utils;

public enum CommandHeader {
    MASTER_TO_REGION_1("<master-region>[1]"),

    REGION_TO_MASTER_1("<region-master>[1]"),

    MASTER_TO_REGION_2("<master-region>[2]"),

    REGION_TO_MASTER_2("<region-master>[2]"),

    MASTER_TO_REGION_3("<master-region>[3]"),

    REGION_TO_MASTER_3("<region-master>[3]"),

    REGION_TO_REGION_1("<region-region>[1]"),

    REGION_TO_REGION_2("<region-region>[2]");


    public String value;

    CommandHeader(String value) {
        this.value = value;
    }
}