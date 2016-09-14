package bigdata.ml

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object App {
  
  case class dframe(x: Double, y:Double, clusterID: Integer, Distance: Double)
  
  def distanceSquare(p1: (Double, Double), p2: (Double, Double)): Double = {
    //TODO return || p1 - p2 || ^ 2 - DONE (Return Euclidean Distance -> (x1-x2)^2 + (y1-y2)^2)
	val t1 = scala.math.pow((p1._1 - p2._1),2)
	val t2 = scala.math.pow((p1._2 - p2._2),2)	
	t1+t2
  }

  def findNearestCenter(centers: Array[(Double, Double)], p: (Double, Double)): (Double, Int) = {
    val distanceWithIndex = centers.zipWithIndex.map {
      case (c, i) => (distanceSquare(c, p), i)
    }
    // TODO : Find nearest center from point p. Return the distance and the center index - DONE
	var best_distance = distanceWithIndex(0)._1
    var best_center_id = distanceWithIndex(0)._2 
	for(i <- 0 until distanceWithIndex.length)
	{
	if(distanceWithIndex(i)._1 < best_distance)
	{
    best_distance = distanceWithIndex(i)._1
    best_center_id = distanceWithIndex(i)._2 
    }
	}
	(best_distance, best_center_id)
  }

  def main(args: Array[String]) {
    println("Arguments = " + args.mkString(" "))
    val infile = args(0) // path of order file
    val K = args(1).toInt // num of centers
    val N = args(2).toInt // max iterations
    val outfile = args(3) // file path  of final center points

    val sparkConf = new SparkConf().setAppName("KMeans")
    val sc = new SparkContext(sparkConf)
	val sqlContext= new org.apache.spark.sql.SQLContext(sc)
	import sqlContext.implicits._
    val input = sc.textFile(infile)
    val points = input.map {
      line =>
        {
          val arr = line.split(",")
          (arr(3).toDouble, arr(4).toDouble) // TODO: Correct the index - DONE
        }
    }.cache()
	
	val df = points.map{case(x,y) => (x,y,-1,-1)}.toDF("x","y","clusterID","distance")
	
    val KMeansKernel: ((Array[(Double, Double)], Double, Boolean), Int) => (Array[(Double, Double)], Double, Boolean) = {
      case ((centers, previousDistance, isEnd), iter) =>
        {
          if (isEnd) {
            (centers, previousDistance, isEnd)
          } else {
            var sumDistance = previousDistance
            
			var newCenters = centers
			
			// TODO : Group points by its nearest center. From each group, calculate a new center. - DONE
			
			var nCenters = df.map(row => { var tempC = findNearestCenter(newCenters,(row(0).asInstanceOf[Double],row(1).asInstanceOf[Double]));
			Row(row(0),row(1),tempC._2,tempC._1);})
            
			var saveData = nCenters.map{case row => dframe(row(0).asInstanceOf[Double],row(1).asInstanceOf[Double],row(2).asInstanceOf[Integer],row(3).asInstanceOf[Double])}.toDF().groupBy("clusterID").agg(sum("x"),sum("y"),count("x"),count("y"))
			
			newCenters = saveData.map( row => (row(1).asInstanceOf[Double]/row(3).asInstanceOf[Long],row(2).asInstanceOf[Double]/row(4).asInstanceOf[Long])).toArray
			
			sumDistance =  nCenters.map{case row => dframe(row(0).asInstanceOf[Double],row(1).asInstanceOf[Double],row(2).asInstanceOf[Integer],row(3).asInstanceOf[Double])}.toDF().agg(sum("Distance")).map(row => row(0).asInstanceOf[Double].toDouble).take(1).head
			
            println("iter = " + iter.toString() + "\tsum distance = " + sumDistance.toString())
            
			var filename = "Iteration" + iter.toString()
			
			saveData.rdd.saveAsTextFile(filename)
			
			(newCenters, sumDistance, previousDistance == sumDistance)
          }
        }
    }

    val minx = points.map(p => p._1).min()
    val maxx = points.map(p => p._1).max()
    val miny = points.map(p => p._2).min()
    val maxy = points.map(p => p._2).max()

	val p = points.zipWithIndex.map{case (k,v) => (v,k)}
    val randomCenters : Array[(Double, Double)] = Array.fill(K){(p.lookup(scala.util.Random.nextInt(K))(0)._1,p.lookup(scala.util.Random.nextInt(K))(0)._2)}// TODO : random K centers - DONE
	
    val (finalCenters, sumDistance, isEnd) = Range(0, N).foldLeft((randomCenters, Double.MaxValue, false))(KMeansKernel)

    sc.parallelize(finalCenters).saveAsTextFile(outfile)
  }

}


