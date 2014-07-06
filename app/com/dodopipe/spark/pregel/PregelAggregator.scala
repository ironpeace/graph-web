package com.dodopipe.spark.pregel

import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.SparkContext._

/**
 * An aggregator used by pregel to communicator with all vertices
 */
@serializable class PregelAggregator(accumulator: Accumulator[Double] ) {

    def value_=(v: Double) = accumulator.value = v

    def value = accumulator.value

    def add(value: Double) = accumulator += value

    def averagedBy(value: Double) = accumulator.value / value

}

object PregelAggregator {

    def apply(sc: SparkContext) = new PregelAggregator(sc.accumulator(0.0))

}
