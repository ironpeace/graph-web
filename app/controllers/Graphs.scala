package controllers

import models._

import play.api._
import play.api.mvc._
import play.api.libs.json._
import play.api.Play.current
import play.api.libs.ws._

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import java.io.PrintWriter

import scala.concurrent.Future

import com.github.aselab.activerecord.dsl._
import com.dodopipe.spark.pagerank.GraphxPageRank


import concurrent._
import ExecutionContext.Implicits.global


object Graphs extends Controller {

	def create() = Action { request =>
		val req = request.body.asFormUrlEncoded
		val name = req.get("graphName").head
		val csv = req.get("graphCsv").head

		val graph = TGraph(name).create

		csv.split("\n").map(_.split(",") match{
			case Array(src, dist) => {
				val edge = TEdge(src.trim.toInt, dist.trim.toInt)
				graph.edges << edge
			}
		})

		Redirect(routes.Application.index())
	}

	def csv(id:Long) = Action {
		TGraph.find(id) match {
			case Some(g) => Ok(views.html.graph_csv(g.edges.toList)).as("text/csv")
			case _ => NotFound("<h1>ID(" + id + ") not found </h1>")
		}
	}

	def isJobServerRunning() = Action {
		Async {
	    	WS.url("http://localhost:8090").get().map { res =>
				if(res.status == 200) {
					Ok
				} else {
					BadGateway
				}
	    	}
    	}
	}

	def createTsvFile(id:Long) = Option[(Graph[Int, Int], SparkContext)] {
		TGraph.find(id) match {
			case Some(g) => {
				val csvlines = g.edges.toList.map(e => "%d %d".format(e.src, e.dist))
				val filename = "tmp/graph_" + id + ".tsv";
				val out = new PrintWriter(filename)
				csvlines.foreach(out.println(_))
				out.close
				val sc = new SparkContext("local", 
											"Application", 
											"~/dev/spark/spark-0.9.0-incubating", 
											List("target/scala-2.10/call-spark-sample_2.10-1.0-SNAPSHOT.jar"))
				(GraphLoader.edgeListFile(sc, filename), sc)
			}
			// case _ => None
		}
	}

	def getNeighbors(graph:Graph[Int, Int], vertexId:Int, depth:Int):Array[Int] = {
		//val ed = graph.triplets.map(t => t.relativeDirection(vertexId))
		graph.triplets.map{ t => 
			val ed = t.relativeDirection(vertexId)
			val neighbors = graph.collectNeighborIds(ed)
			if(depth > 0) {
				Array(vertexId) ++ neighbors.flatMap(nv => getNeighbors(graph, nv._1.toInt, depth - 1))
			}else{
				Array(vertexId) ++ neighbors
			}
		}
	}

	def filterNeighbors(graphId:Long, vertexId:Int, depth:Int) = Action {
		createTsvFile(graphId) match {
			case Some(gs) => {
				val graph = gs._1
				val neighbors = getNeighbors(graph, vertexId, depth)
				val filterdEdges = graph.edges.filter(e => neighbors(e.source) || neighbors(e.target)).toList
				Ok(views.html.graph_csv(filterdEdges)).as("text/csv")
			}
			case _ => NotFound("<h1>ID(" + graphId + ") not found </h1>")
		}
	}


	def info(id:Long) = Action {
		createTsvFile(id) match {
			case Some(gs) => {
				val graph = gs._1
				val sc = gs._2

				//curl -d "input.filename=/Users/teppei_tosa/dev/play-2.2.3/apps/call-spark-sample//tmp/graph_1.tsv" -d "input.command=basic" 'localhost:8090/jobs?appName=graphx&classPath=com.teppeistudio.GraphxJob'
/**
				Async {
			    	WS.url("http://localhost:8090/jobs")
			    		.withQueryString(
			    			"appName" -> "graphx", 
			    			"classPath" -> "com.teppeistudio.GraphxJob")
			    		.post(Map(
			    			"input.filename" -> Seq("/Users/teppei_tosa/dev/play-2.2.3/apps/call-spark-sample/tmp/graph_1.tsv")
			    			// "input.command" -> Seq("basic")
			    		))

			    		// .post(Map(
			    		// 	"appName" -> Seq("graphx"), 
			    		// 	"classPath" -> Seq("com.teppeistudio.GraphxJob"),
			    		// 	"input.filename" -> Seq("/Users/teppei_tosa/dev/play-2.2.3/apps/call-spark-sample/tmp/graph_1.tsv"),
			    		// 	"input.command" -> Seq("basic")
			    		// ))

			    		// .withQueryString(
			    		// 	"appName" -> "graphx", 
			    		// 	"classPath" -> "com.teppeistudio.GraphxJob",
			    		// 	"input.filename" -> "/Users/teppei_tosa/dev/play-2.2.3/apps/call-spark-sample/tmp/graph_1.tsv"
			    		// 	// "input.command" -> "basic"
			    		// ).get()

			    		.map { res =>
							if(res.status == 200) {
								Ok(res.json)
							} else {
								println("status : " + res.status)
								//BadGateway
								Ok(res.json)
							}
				    	}
		    	}
**/
				Logger.info(graph.toString)
				Logger.info(sc.toString)
				
				def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
					if (a._2 > b._2) a else b
				}

				// basic info
				val inDmax: (VertexId, Int) = graph.inDegrees.reduce(max)
				val outDmax : (VertexId, Int) = graph.outDegrees.reduce(max)
				val dMax : (VertexId, Int) = graph.degrees.reduce(max)

				// default page rank 
				val ranks = graph.pageRank(0.0001).vertices
				val ranks_sorted = ranks.collect.sortBy(_._2).reverse
				val rankArray = for(r <- ranks_sorted) yield {
					JsObject(Seq("vertex"->JsNumber(r._1), "rank"->JsNumber(r._2)))
				}

				// dodopipe page rank with threshold
				val ranks_dt = GraphxPageRank.withThreshold(graph, 0.001F).vertices
				val ranks_dt_sorted = ranks_dt.collect.sortBy(_._2).reverse
				val ranks_dt_array = for(r <- ranks_dt_sorted) yield {
					JsObject(Seq("vertex"->JsNumber(r._1), "rank"->JsNumber(r._2)))
				}

				// dodopipe page rank with iteration step
				val ranks_di = GraphxPageRank.withIterationSteps(sc, graph, 30).vertices
				val ranks_di_sorted = ranks_di.collect.sortBy(_._2).reverse
				val ranks_di_array = for(r <- ranks_di_sorted) yield {
					JsObject(Seq("vertex"->JsNumber(r._1), "rank"->JsNumber(r._2)))
				}

				// connected components
				val ccs = graph.connectedComponents().vertices
				val ccGrouped = ccs.map(c => (c._2.toInt, c._1.toString)).reduceByKey(_ + "," + _).collect

				val ccArray = for(c <- ccGrouped) yield {
					JsObject(Seq("cc"->JsNumber(c._1), "vertex"->JsString(c._2)))
				}

				// triangle count
				// val triCounts = graph.triangleCount().vertices
				// val triCounts_sorted = triCounts.collect.sortBy(_._2).reverse
				// val triCounts_array = for(r <- triCounts_sorted) yield {
				// 	JsObject(Seq("vertex"->JsNumber(r._1), "rank"->JsNumber(r._2)))
				// }

				val json: JsValue = JsObject(Seq(
										"edgesCount" -> JsNumber(graph.edges.count),
										"verticesCount" -> JsNumber(graph.vertices.count),
										"maxInDegrees" -> JsObject(Seq(
											"vertex"-> JsNumber(inDmax._1), 
											"degrees" -> JsNumber(inDmax._2)
										)),
										"maxOutDegrees" -> JsObject(Seq(
											"vertex"-> JsNumber(outDmax._1), 
											"degrees" -> JsNumber(outDmax._2)
										)),
										"maxDegrees" -> JsObject(Seq(
											"vertex"-> JsNumber(dMax._1), 
											"degrees" -> JsNumber(dMax._2)
										)),
										"ranks" -> JsArray(rankArray.toSeq),
										"ranks_dt" -> JsArray(ranks_dt_array.toSeq),
										"ranks_di" -> JsArray(ranks_di_array.toSeq),
										"connectedComponents" -> JsArray(ccArray.toSeq)
										// "triangleCount" -> JsArray(triCounts_array.toSeq)
									))
				
				sc.stop

				Ok(json)
			}
			case _ => NotFound("<h1>ID(" + id + ") not found </h1>")
		}


	}
}
