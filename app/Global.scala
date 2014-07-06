import play.api._
import com.github.aselab.activerecord._
import com.github.aselab.activerecord.dsl._
import models._

object Global extends GlobalSettings {

  override def onStart(app: Application) {
    Logger.info("Application has started")

  	Tables.initialize(Map("schema" -> "models.Tables"))

  	if(TGraph.count == 0){
	  	val graph = TGraph("SampleGraph01").create
	  	val e1 = TEdge(1, 2).create
	  	val e2 = TEdge(2, 3).create
	  	val e3 = TEdge(3, 2).create
	  	val e4 = TEdge(3, 1).create
      graph.edges << e1
      graph.edges << e2
      graph.edges << e3
      graph.edges << e4
  	}
  }

  override def onStop(app: Application) {
    Logger.info("Application shutdown...")
    Tables.cleanup
  }

}
