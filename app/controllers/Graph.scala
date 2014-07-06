package controllers

import models._
import play.api._
import play.api.mvc._
import play.api.libs.json._
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import com.github.aselab.activerecord.dsl._
import java.io.PrintWriter
import com.dodopipe.spark.pagerank.GraphxPageRank

object Graph extends Controller {

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

	def info(id:Long) = Action {

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

				val graph = GraphLoader.edgeListFile(sc, filename)

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
