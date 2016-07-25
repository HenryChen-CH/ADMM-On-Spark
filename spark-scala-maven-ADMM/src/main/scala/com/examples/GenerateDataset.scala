package com.examples

import java.io._
import scala.collection.mutable.ArrayBuffer
import util.Random


/**
  * Created by ChenHao on 6/6/16.
  */


object GenerateDataset {

    def RandomGenerateLS(row: Int, column: Int, file: String) {
        // w = [1, 1, 1, .... 1, 1]
        val pw = new PrintWriter(new File(file))
        for (r <- 1 to row) {
            val a = randomRowLS(column, row)
            val s = new StringBuilder()
            for (j<- 0 until column) {
                s.append(a(j))
                s.append(",")
            }
            s.append(a(column))
            s.append("\n")
            pw.write(s.toString())
        }
        pw.close()

    }

    def randomRowLS(n: Int, m: Int): Array[Double] = {
        val res = new ArrayBuffer[Double]()
        var sum: Double = 0.0
        var random: Double = 0.0
        for (i <- 1 to n) {
            random = Random.nextDouble()*math.min(0.1*m, 200)
            res += random
            sum = sum + random
        }
        sum += Random.nextGaussian()
        res += sum
        res.toArray
    }

}
