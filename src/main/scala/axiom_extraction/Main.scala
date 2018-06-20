package axiom_extraction


import net.sansa_stack.rdf.spark.io._
import org.apache.spark.sql.SparkSession
import org.apache.jena.riot.Lang
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._
object Main extends App {

  var DEBUG: Boolean = false
  
  override def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .appName(s"AxiomExtraction")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val resource = "test_input.nt"
    val path = getClass.getResource(resource).getPath()
    val df: DataFrame = spark.read.rdf(Lang.NTRIPLES)(path)
    df.show()
    
    //selecting all distinct predicates
    val distinctValuesDF = df.select("p").distinct
    //selecting all distinct instances
    val distinctSubjectsDF = df.select("s").distinct
    val distinctObjectsDF = df.select("o").distinct
    distinctValuesDF.show()
    
    //all distinct instances in a list
    val instances = distinctSubjectsDF.union(distinctObjectsDF).distinct().rdd.map(r => r(0)).collect()
    
    //list of subsamples for instance pair extraction
    var dfList : ListBuffer[DataFrame] = new ListBuffer()
    
    var transactions : ListBuffer [String] = new ListBuffer()
    
    //printing out distinct predicate values
    distinctValuesDF.select("p").take((distinctValuesDF.count()).toInt).foreach(printWithNeg)    
    
    //sampling the pair groups
    instances.foreach {x => getInstanceFilter(x,df,dfList)}
    
    
    //extracting pairs and generating transactions
    dfList.foreach(x=>extractTransactions(x,transactions))
    transactions.foreach(println)
  }

	def debug(out: String) {
	   if (DEBUG)
	      println(out)
	}
	
	def printWithNeg(x:Any) {
	  Console.println(x)	  
	  Console.println("[not_"+x.toString().takeRight(x.toString().length()-1))
	}
	
	def extractTransactions(df :DataFrame,transactions :ListBuffer[String]) {
	  var count:Int = 0
	  var transactions : ListBuffer[String] = new ListBuffer[String]
    var outString: String = ""
    var currObject: String = ""                        
    count = df.count().toInt         
    currObject = df.sort("o").first()(2).toString()
    outString = "(" + df.sort("o").first()(0).toString() + ":" + df.sort("o").first()(2).toString() + ") -> "
    df.sort("o").foreach{ x=>
              	    { Console.println(x.toString())
            	                                    	  
              	      outString += x(1) +","
              	      outString += "not_"+x(1).toString() +"," 
              	      Console.println(outString)              	    }  
              	   
      }
	   transactions.append(outString.take(outString.length()-1))
     Console.println("TR Size: " + transactions.size.toString())       	   
     Console.println("===================================================")              
	  }
	
	
	def getInstanceFilter(x:Any,df:DataFrame,lDf:ListBuffer[DataFrame]) {
	  val output=df.select("s","p","o").where("s == '"+x.toString()+"' ")
	  val instances = output.select("o").distinct().rdd.map(r => r(0)).collect()
	  instances.foreach(f =>{
	    val subOutput=output.select("s","p","o").where("o == '"+f.toString()+"' ")
  	  if(subOutput.count()>0)//if at least one paire exists
  	  {
  	    lDf.append(subOutput.sort(asc("o")))
  	  }
	  })

	}
}


















