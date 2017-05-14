package org.test.spark1

import scala.swing._
import BorderPanel.Position._
object MovieGui extends App{
  
  case class Movie(title:String,Year:Int,rating:String,description:String)
  var movies = List[Movie](Movie("Bambi",1976,"G","Hunter are bad!"))
  val movieList = new ListView(movies.map(_.title))
  val titleField = new TextField("")
  val yearField = new TextField("")
  val ratingComboBox = new ComboBox(List("G","PG","PG-13","R","NC-17","NR"))
  val descriptionArea = new TextArea 
  val frame = new MainFrame{
    title = "Movie Database"
    contents = new BorderPanel{
      layout += movieList -> West
      layout += new BorderPanel{
        layout += new GridPanel(3,1){
          contents += new BorderPanel {
            layout += new Label("Title")->West
            layout += titleField -> Center
          }
          contents += new BorderPanel {
            layout += new Label("Year")->West
            layout += yearField -> Center
          }
          contents += ratingComboBox
        }->North
        layout += descriptionArea -> Center
      }-> Center
    }
    size = new Dimension(800,600)
    centerOnScreen()
  }
  frame.visible = true
}