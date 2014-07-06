package com.dodopipe.spark.pregel

import org.apache.spark.SparkContext
import org.apache.spark.graphx._

import scala.reflect.ClassTag


/**
 * Source was borrowed from graphx pregel-like implementation
 *
 *
 */
object SparkPregel {

    /**
     * Execute a Pregel-like iterative vertex-parallel abstraction.  The
     * user-defined vertex-program `vprog` is executed in parallel on
     * each vertex receiving any inbound messages and computing a new
     * value for the vertex.  The `sendMsg` function is then invoked on
     * all out-edges and is used to compute an optional message to the
     * destination vertex. The `mergeMsg` function is a commutative
     * associative function used to combine messages destined to the
     * same vertex.
     *
     * On the first iteration all vertices receive the `initialMsg` and
     * on subsequent iterations if a vertex does not receive a message
     * then the vertex-program is not invoked.
     *
     * This function iterates until there are no remaining messages, or
     * for `maxIterations` iterations.
     *
     * @tparam VD the vertex data type
     * @tparam ED the edge data type
     *
     * @param graph the input graph.
     *
     * @param initialMsg the message each vertex will receive at the on
     *                   the first iteration
     *
     * @param maxIterations the maximum number of iterations to run for
     *
     * @param activeDirection the direction of edges incident to a vertex that received a message in
     *                        the previous round on which to run `sendMsg`. For example, if this is `EdgeDirection.Out`, only
     *                        out-edges of vertices that received a message in the previous round will run. The default is
     *                        `EdgeDirection.Either`, which will run `sendMsg` on edges where either side received a message
     *                        in the previous round. If this is `EdgeDirection.Both`, `sendMsg` will only run on edges where
     *                        *both* vertices received a message.
     *
     * @param vertexProg the user-defined vertex program which runs on each
     *                            vertex and receives the inbound message and computes a new vertex
     *                            value.  On the first iteration the vertex program is invoked on
     *                            all vertices and is passed the default message.  On subsequent
     *                            iterations the vertex program is only invoked on those vertices
     *                            that receive messages.
     *
     * @param sendMsg a user supplied function that is applied to out
     *                edges of vertices that received messages in the current
     *                iteration
     *
     * @param mergeMsg a user supplied function that takes two incoming
     *                 messages of type A and merges them into a single message of type
     *                 A.  ''This function must be commutative and associative and
     *                 ideally the size of A should not increase.''
     *
     * @return the resulting graph at the end of the computation
     *
     */
    def apply[VD: ClassTag, ED: ClassTag](sc: SparkContext,
                                          graph: Graph[VD, ED],
                                          initialMsg: Double,
                                          maxIterations: Int = Int.MaxValue,
                                          activeDirection: EdgeDirection = EdgeDirection.Either)
                                         (vertexProg: (VertexId, VD, Double) => VD,
                                          vertexAggregator: (VertexId, VD, PregelAggregator) => VD,
                                          sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, Double)],
                                          mergeMsg: (Double, Double) => Double)
    : Graph[VD, ED] = {

        val aggregator = PregelAggregator(sc)
        val numVertices = graph.vertices.count()

        def vprog(vid: VertexId,
                  vd: VD,
                  a: Double): VD = {
            val rst = vertexProg(vid, vd, a)
            vertexAggregator(vid, rst, aggregator)
        }

        var g = graph.mapVertices((vid,
                                   vdata) => vprog(vid,
                                                   vdata,
                                                   initialMsg)).cache()

        // compute the messages
        var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
        var activeMessages = messages.count()
        // Loop
        var prevG: Graph[VD, ED] = null
        var i = 0
        while (activeMessages > 0 && i < maxIterations) {

            val danglingFactor = aggregator.averagedBy(numVertices)

            val newVerts = g.vertices.leftJoin(messages) {

                (vid, v, msgOpt) => {

                    val msg = msgOpt.getOrElse(0.0) + danglingFactor
                    vprog(vid, v, msg)
                }
            }.cache()

            prevG = g

            //update graph
            g = g.joinVertices(newVerts)((vid,
                                          v,
                                          newV) => newV)

            g.cache()
            aggregator.value = 0.0

            val oldMessages = messages
            messages = g
                       .mapReduceTriplets(sendMsg,
                                          mergeMsg)
                       .cache()
            activeMessages = messages.count()

            // Unpersist the RDDs hidden by newly-materialized RDDs
            oldMessages.unpersist(blocking = false)
            newVerts.unpersist(blocking = false)
            prevG.unpersistVertices(blocking = false)
            // count the iteration
            i += 1
        }

        g
    }

    // end of apply

}

// end of class Pregel

