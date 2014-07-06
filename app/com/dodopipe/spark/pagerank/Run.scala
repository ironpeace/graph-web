package com.dodopipe.spark.pagerank

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx.PartitionStrategy._
import org.apache.spark.graphx._

case class RunArguments(hostName: String,
                        inputFile: String,
                        exitCond: String,
                        threshold: Float,
                        stepNum: Int,
                        output: String,
                        partNum: Int,
                        partStrategy: Option[PartitionStrategy])

object Run extends Logging {

    def main(arguments: Array[String]) = {

        val args = parseArgs(arguments)

        val conf = new SparkConf()
            .set("spark.serializer",
                 "org.apache.spark.serializer.KryoSerializer")
            .set("spark.kryo.registrator",
                 "org.apache.spark.graphx.GraphKryoRegistrator")

        val sc = new SparkContext(args.hostName,
                                  "com.dodopipe.PageRank(" + args
                                      .inputFile + ")",
                                  conf)

        val unPartitionedGraph =
            GraphLoader.edgeListFile(sc,
                                     args.inputFile,
                                     minEdgePartitions = args.partNum).cache()

        val partitionStrategy = args.partStrategy
        val graph =
            partitionStrategy.foldLeft(unPartitionedGraph)(_.partitionBy(_))


        println("GRAPHX: Number of vertices " + graph.vertices.count)
        println("GRAPHX: Number of edges " + graph.edges.count)

        val pr = args.exitCond match {
            case "withThreshold" =>
                GraphxPageRank.withThreshold(graph, args.threshold)
                    //graph.pageRank(args.threshold)
                                            .vertices
                                            .cache()
            case "withIterationSteps" =>
                GraphxPageRank.withIterationSteps(sc, graph, args.stepNum)
                                            .vertices
                                            .cache()

        }

        pr.map(v =>(v._2,v._1))
            .sortByKey(false).foreach(println(_))

        println("Total page rank: " + pr.map(_._2).reduce(_ + _))
        if (! args.output.isEmpty) {
           pr.map{case (id, r) => id + "\t" + r}.saveAsTextFile(args.output)
        }

        sc.stop()

    }

    protected def pickPartitioner(v: String) : PartitionStrategy = {
        v match {
            case "RandomVertexCut" => RandomVertexCut
            case "EdgePartition1D" => EdgePartition1D
            case "EdgePartition2D" => EdgePartition2D
            case "CanonicalRandomVertexCut" => CanonicalRandomVertexCut
            case _ => throw new IllegalArgumentException("Invalid PartitionStrategy: " + v)
        }           
    }

    protected def parseArgs(args: Array[String]): RunArguments = { 

        if (args.length < 2 ) {
            printUsage
            sys.exit(0)
        }

        val host = args(0)
        val inFName = args(1)
        val arguments:Map[Symbol, Any] = Map(
                    'sparkHost -> host,
                    'inputFile -> inFName
        )

        val options = args.drop(2).map { arg =>
            arg.dropWhile(_ == '-').split('=') match {
                case Array(opt, v) => opt -> v
                case _ => throw new IllegalArgumentException("Invalid argument:" + arg)
            }
        } 
        var threshold : Float = 0.001F
        var exitCond = "withIterationSteps"
        var output = ""
        var stepNum = 30
        var partNum = 4
        var partStrategy: Option[PartitionStrategy] = None
        var useBagel = false
        options.foreach{
            case ("exitCond", v)     => exitCond = v
            case ("threshold", v)    => threshold = v.toFloat
            case ("stepNum", v)      => stepNum = v.toInt
            case ("output", v)       => output = v
            case ("partNum", v)      => partNum = v.toInt
            case ("partStrategy", v) => partStrategy = Some(pickPartitioner(v))
            case (opt, _)            => throw new IllegalArgumentException("Invalid option: " + opt)
        }
        if (List("withIterationSteps","withThreshold")
                .filter(_.equals(exitCond))
                .isEmpty) {
            throw new IllegalArgumentException(
                            "Invalid exitcond: %s".format(exitCond))
        }

        RunArguments(host, 
                     inFName, 
                     exitCond, 
                     threshold, 
                     stepNum, 
                     output, 
                     partNum, 
                     partStrategy)

    }

    protected def printUsage {
        println(
            """
            Usage: sbt 
                > run sparkHostName inputFileName [options]
            options:
                --exitCond     : Specify the exit conditions, the default is iteration steps.
                                 [withIterationSteps | withThreshold]
                --threshold    : Configue the convergence level. 
                                    The computation will stop when the changes of pagerank's value
                                    is lower than threshold 
                --stepNum      : Configue the steps of superstep.
                --output       : The file name of output
                --partNum      : The partition number to split the input file.
                --partStrategy : Specify the partition strategy.
                                 [RandomVertexCut|EdgePartition1D|EdgePartition2D|CanonicalRandomVertexCut]
            """)
    }
}