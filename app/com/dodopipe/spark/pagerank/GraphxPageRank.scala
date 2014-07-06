package com.dodopipe.spark.pagerank

import com.dodopipe.spark.pregel.{PregelAggregator, SparkPregel}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.{Logging, SparkContext}

import scala.reflect.ClassTag

object GraphxPageRank extends Logging {

    def withIterationSteps[VD:ClassTag, ED:ClassTag](sc: SparkContext,
                                                       graph: Graph[VD, ED],
                                                       stepNum: Int,
                                                       dumplingFactor: Double = 0.85)
    : Graph[Double, Double] = {

        val verticesNum = graph.vertices.count()
        val teleportation = (1.0 - dumplingFactor) / verticesNum
        val initialVertexValue = 0.0
        val initialMessage = 1.0 / verticesNum

        val pageRankGraph: Graph[(Double, Int), Double] =
            preparePagerankGraph(graph, initialVertexValue)


        def vertexProgram(id: VertexId,
                          value: (Double, Int),
                          msgSum: Double): (Double, Int) = {
            val _teleport = teleportation
            val _factor = dumplingFactor
            (_teleport + _factor * msgSum, value._2)

        }

        def vertexAggregate(id: VertexId,
                            value: (Double, Int),
                            agg: PregelAggregator):(Double, Int) = {

            if (value._2 == 0)
                agg.add(value._1)
            value
        }


        def sendMessage(edge: EdgeTriplet[(Double, Int), Double]) = {

            Iterator((edge.dstId, edge.srcAttr._1 * edge.attr))
        }

        def messageCombiner(a: Double,
                            b: Double): Double = a + b

        SparkPregel(sc,
                    pageRankGraph,
                    initialMessage,
                    stepNum,
                    activeDirection = EdgeDirection.Out)(vertexProgram,
                                                         vertexAggregate,
                                                         sendMessage,
                                                         messageCombiner)
        .mapVertices((id,
                      value) => value._1)

    }

    def withThreshold[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                                  delta: Double,
                                                  dumplingFactor: Double = 0.85)
    : Graph[Double, Double] = {

            val verticesNum = graph.vertices.count()
            val initialVertexValue = (0.0, 0.0)
            val initialMessage = (1.0 - dumplingFactor) / dumplingFactor

            val pagerankGraph: Graph[(Double,Double), Double] =
                preparePagerankGraph(graph, initialVertexValue).mapVertices((vid,v) => v._1)

            def vertexProgram(id: VertexId,
                              value: (Double, Double),
                              msgSum: Double): (Double, Double) = {
                val oldPR = value._1
                val newPR = oldPR + dumplingFactor * msgSum
                (newPR, newPR - oldPR)
            }

            def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {

                if (edge.srcAttr._2 > delta) {
                    Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
                } else {
                    Iterator.empty
                }
            }

            def messageCombiner(a: Double,
                                b: Double): Double = a + b

            Pregel(pagerankGraph,
                   initialMessage,
                   activeDirection = EdgeDirection.Out)(vertexProgram,
                                                        sendMessage,
                                                        messageCombiner)
                .mapVertices((vid,
                              attr) => attr._1)
    }

    protected def preparePagerankGraph[VD: ClassTag, ED: ClassTag, IVD: ClassTag]
    (graph: Graph[VD, ED],
     initValue: IVD): Graph[(IVD, Int), Double] = {

        graph
            // compute the number of outer links for every vertex.
            .outerJoinVertices(graph.outDegrees) { (vid,
                                                    value,
                                                    deg) => deg.getOrElse(0)
                                                 }
            // set 1.0 / num(outerlinks) as edge's weight
            // EdgeTriplet
            //      srcAttr : value of source vertex
            //      dstAttr : value of dest vertext
            //      attr :    value of edge
            // in the above step, we have set the outdegree nums as vertex value
            // edge.attr = 1.0 / numOfOutDegree
            .mapTriplets(e => 1.0 / e.srcAttr)
            // initialize the vertex's value
            .mapVertices((id,
                          attr) => (initValue, attr))
            .cache()
    }

}