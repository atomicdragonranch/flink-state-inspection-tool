package io.flinkstate.testjob.generators;

import org.apache.flink.connector.datagen.source.GeneratorFunction;

public class ConfigGeneratorFunction implements GeneratorFunction<Long, String> {

    private static final long serialVersionUID = 1L;

    private final double thresholdInitial;
    private final double thresholdIncrement;

    public ConfigGeneratorFunction(double thresholdInitial, double thresholdIncrement) {
        this.thresholdInitial = thresholdInitial;
        this.thresholdIncrement = thresholdIncrement;
    }

    @Override
    public String map(Long sequence) throws Exception {
        int configIndex = (int) (sequence % 3);
        switch (configIndex) {
            case 0:
                double threshold = thresholdInitial + (sequence / 3) * thresholdIncrement;
                return "threshold=" + String.format("%.1f", threshold);
            case 1:
                String mode = (sequence / 3) % 2 == 0 ? "normal" : "aggressive";
                return "mode=" + mode;
            case 2:
                return "version=" + (sequence / 3 + 1);
            default:
                return "unknown=true";
        }
    }
}
