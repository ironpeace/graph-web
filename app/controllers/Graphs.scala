package controllers

import models._

import play.api._
import play.api.mvc._
import play.api.libs.json._
import play.api.Play.current
import play.api.libs.ws._

import java.io.PrintWriter

import scala.concurrent.Future

import com.github.aselab.activerecord.dsl._

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

	//curl -d "" 'localhost:8090/contexts/graphx?num-cpu-cores=4&mem-per-node=512m'
	def registContext() = Action {
		Async {
	    	WS.url("http://localhost:8090/contexts/graphx?num-cpu-cores=4&mem-per-node=512m").post("").map { res =>
				if(res.status == 200) {
					Ok
				} else {
					Logger.info(res.json.toString)
					BadGateway
				}
	    	}
    	}
	}

	def callJobServer(id:Long, command:String) = Action {
		TGraph.find(id) match {
			case Some(g) => {
				val csvlines = g.edges.toList.map(e => "%d %d".format(e.src, e.dist))
				val filename = "tmp/graph_" + id + ".tsv";
				val out = new PrintWriter(filename)
				csvlines.foreach(out.println(_))
				out.close

				Async {
			    	WS.url("http://localhost:8090/jobs")
			    		.withQueryString(
			    			"appName" -> "graphx", 
			    			"classPath" -> "com.teppeistudio.GraphxJob",
			    			"context" -> "graphx",
			    			"sync" -> "true")
			    		.post(Map(
			    			// "input.filename" -> Seq("/Users/teppei_tosa/dev/play-2.2.3/apps/call-spark-sample/tmp/graph_1.tsv")
			    			"input.filename.command" -> Seq("graph_1.tsv___" + command)
			    		))
			    		.map { res =>
							if(res.status == 200) {
								Ok(res.json)
							} else {
								Logger.info(res.json.toString)
								BadGateway
							}
				    	}
		    	}
			}
			 case _ => NotFound("<h1>ID(" + id + ") not found </h1>")
		}
	}
}
