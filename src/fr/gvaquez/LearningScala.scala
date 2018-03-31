package fr.gvaquez
import scala.io.{Source, BufferedSource}
import scala.collection.mutable.Builder

object Set {
  def input(file: String) = {
    Context.rdd = Some(Source.fromFile(file).getLines().toArray)
  }
}

object Context {
  var rdd : Option[Array[String]] = None;
}

object Input {
  var _separator = ""
  def foreach(codeBlock: (String => Unit)) {
    //System.out.println("Entering foreach")
    //Context.rdd.get.getLines().foreach(codeBlock)
    Context.rdd.get.foreach(codeBlock)
    
  }
  
  def separator(sep : String) = _separator = sep
  def separator = _separator
  
  def createRows(codeBlock: => Unit) = {
    //val b = List.newBuilder[String]  // to "build" the contents of inner
    codeBlock
    //val list = codeBlock
    Context.rdd.get.foreach { f => {
        val lineSplit = f.split(_separator)
        accumulator.getList.result().foreach(fi => fi.apply(lineSplit))
      }      
    }
    
  }
}

object accumulator {
  val b = List.newBuilder[Field]
  def add(f: Field) {
    b.+=(f)
  }
  
  def getList() = b
}

trait inputMngt {
  val set = Set
  val input = Input
  val field = Field
}

class Field(i : Integer) {
  var validationType = ""
  var convertionType = ""
  
  def validate(validateType: String) : Field = {
    validationType = validateType
    this
  }
  
  def convert(conversionType: String) : Field = {
    convertionType = conversionType
    return this
  }
  
  def getIndex = i
  
  def apply(fs : Array[String]) = {
    System.out.println("Field to get " + i + " validation: " + validationType + " conversion: "  + convertionType)
  }
}


object Field {
  def position(i : Int) = {
    val f = new Field(i)
    accumulator.add(f)
    f
  }
}

object LearningScala extends App with inputMngt {
  
  set input "fichier.csv"
  // A ce point, on a un context.rdd valorisé au contenu du fichier
  //System.out.println("Test loop")
  //Context.rdd.get.getLines().foreach(f => System.out.println(f))
  
  input separator ","
  System.out.println("Separator: " + input.separator)
  System.out.println("DSL loop")
  input foreach {f => System.out.println(f)}
  System.out.println("DSL loop 2")
  input foreach {f => System.out.println(f)}
  
  System.out.println("DSL ROW")
  input createRows {
    field position 1 validate "string"
    field position 2 validate "integer" convert "float"
  }

    
}