package org.crystal.pipelines;

import org.apache.beam.sdk.transforms.DoFn;

public class FilteredThreshold extends DoFn<Double,Double> {
    public double conditionNr =0;
    public FilteredThreshold(double i) {
        conditionNr =i;
    }
    @ProcessElement
    public void pel(ProcessContext processContext){
        if (processContext.element()> conditionNr){
            processContext.output(processContext.element());
        }
    }
}
