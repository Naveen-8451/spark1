package org.test.spark1

import scala.swing._


object FirstFrame extends App {
 
  val textArea = new TextArea
  def openFile{
    val chooser = new FileChooser
    if(chooser.showOpenDialog(null)==FileChooser.Result.Approve){
      val source = scala.io.Source.fromFile(chooser.selectedFile)
      textArea.text=source.mkString
      source.close()
    }
  }
  def saveFile{
    val chooser = new FileChooser
    if(chooser.showSaveDialog(null)==FileChooser.Result.Approve){
      val pw = new java.io.PrintWriter(chooser.selectedFile)
      pw.print(textArea.text)
      pw.close()
    }
    
  }
  val frame = new MainFrame {
    title = "My First Frame"
    contents = textArea
    
    menuBar = new MenuBar {
      // Menu Bar
      //open FIle
     
      contents += new Menu("File") {
        contents += new MenuItem(Action("open") {
          openFile
        })
        
        // save File
        
        contents += new MenuItem(Action("save") {
          saveFile
        })
        
        //Separator betweem two menu items
        
        contents += new Separator
        
        // Exit Item
        
        contents += new MenuItem(Action("Exit") {
          sys.exit(0)
        })
        
      }
      contents += new Menu("View") {

      }
      contents += new Menu("Edit") {

      }
    }
    import BorderPanel.Position._
    contents = new BorderPanel{
      layout += new Label("This is a label.")-> North
      layout += new Button("Clik")-> West
      layout += new TextField -> South
      layout += new ComboBox(List("This","is","a","lable"))->Center
    }
    size = new Dimension(500, 500)
    centerOnScreen()
  }
  frame.visible = true
}