package axiom_extraction


import net.sansa_stack.rdf.spark.io._
import org.apache.spark.sql.SparkSession
import org.apache.jena.riot.Lang
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Main extends App {

  var DEBUG: Boolean = false
  
  override def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .appName(s"AxiomExtraction")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
     val resource = s"test_input.nt"
     val path = getClass.getResource(resource).getPath()
     val df: DataFrame = spark.read.rdf(Lang.NTRIPLES)(path)
       df.show
    val selectedData = df.select("s", "p", "o").where("p == 'friendOf' OR p == 'hates'")
    val distinctValuesDF = df.select("p").distinct
    val distinctSubjectsDF = df.select("s").distinct
    val distinctObjectsDF = df.select("o").distinct
    selectedData.show()
    distinctValuesDF.show()
    val instances = distinctSubjectsDF.union(distinctObjectsDF).distinct()
    distinctValuesDF.select("p").take((distinctValuesDF.count()).toInt).foreach(printWithNeg)
    
    instances.show()

  }

	def debug(out: String) {
	   if (DEBUG)
	      println(out)
	}
	
	def printWithNeg(x:Any) {
	  Console.println(x)	  
	  Console.println("[not_"+x.toString().takeRight(x.toString().length()-1))
	}
}




















