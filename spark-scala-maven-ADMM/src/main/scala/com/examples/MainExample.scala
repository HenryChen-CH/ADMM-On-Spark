package com.examples

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext

import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j._
import org.apache.commons.math3.linear._
import org.apache.spark.rdd.RDD

object MainExample {

    def main(arg: Array[String]): Unit = {
        var logger = Logger.getLogger(this.getClass())

        val jobName = "MainExample"
        val conf = new SparkConf().setAppName(jobName).set("spark.executor.memory", "8g")
        val sc = new SparkContext(conf)
        sc.setCheckpointDir("/mnt1/s3/checkpoint")
        Logger.getRootLogger.setLevel(Level.ERROR)



        val lambda: Double = arg(1).toDouble
        val rho: Double = arg(2).toDouble
        val iterations: Int = arg(3).toInt
        val numBlocks: Int = arg(4).toInt
        val pathToFiles = arg(0)


        val X: RDD[Array[Double]] = sc.textFile(pathToFiles).map(a=>{
            val s = a.split(",")
            val res = new ArrayBuffer[Double]()
            for (i <- s) {
                res.append(i.toDouble)
            }
            res.toArray
        }).repartition(numBlocks)

        val dim: Int = X.first().length-1
        val n: Long = X.count()

        println("Data Row: "+ n +" Dim: "+ dim)
        println(s"lambda: $lambda rho: $rho iterations: $iterations numBlocks: $numBlocks")

        val start = System.nanoTime()
        val w = ADMM.solve_lasso(X, lambda, numBlocks, iterations, rho)

        val cost = X.map(a=>(a.slice(0,dim),a(dim))).map(a=>{
            val c = new ArrayRealVector(a._1).dotProduct(new ArrayRealVector(w)) - a._2
            c*c
        }).reduce(_+_)/n

        println(s"cost: $cost")

        w.foreach(a=>{print(a);print("\n")})
        println()
        println()
        println()

        val end: Long = System.nanoTime()
        val time = (end-start)/1000000000
        println("elapse time minute:"+time/60+"second:"+time%60)

    }

}
