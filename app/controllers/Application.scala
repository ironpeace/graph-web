package controllers

import models._
import play.api._
import play.api.mvc._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Application extends Controller {

	def index = Action {
		Ok(views.html.index(TGraph))
	}

	def editor = Action {
		Ok(views.html.editor())
	}
}