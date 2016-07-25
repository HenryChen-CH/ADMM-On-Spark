package com.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import breeze.linalg._
import scala.util.control.Breaks._

/**
  * Created by ChenHao on 6/3/16.
  */


object ADMM {
    def solve_lasso(X: RDD[Array[Double]], lambda: Double, numBlocks: Int, iterations: Int, rho: Double): Array[Double]={
        // 1/2||AX-B||.^2+lambda|x|
        val sc: SparkContext = X.sparkContext
        val dim: Int = X.first().length-1

        val data = X.repartition(numBlocks)

        val convergTol: Double = 1e-3
        val feastTol: Double = 1e-3

        // data.map(a=>(a.slice(0,dim), a(dim))).first()._1.foreach(a=>println(a))

        var D: RDD[(Int, (DenseMatrix[Double], DenseVector[Double], DenseVector[Double], DenseVector[Double],DenseVector[Double]))] =
            data.map(a=>(a.slice(0,dim), a(dim))).mapPartitionsWithIndex((i, a) =>{
                val ar = a.toArray
                val n: Int = ar.length

                val A = DenseMatrix.zeros[Double](n, dim)
                val b = DenseVector.zeros[Double](n)
                var j = 0
                ar.foreach(elem =>{
                    A(j,::) := new DenseVector[Double](elem._1).t
                    b(j) = elem._2
                    j = j + 1
                })

                // A, b, x, u, r
                Iterator((i, (pinv(A.t*A+DenseMatrix.eye[Double](dim):*rho), A.t*b, randomVector(dim), randomVector(dim), randomVector(dim))))
        }).persist()
        // println(D.first())

        //var z = sc.broadcast(randomVector(dim))
        var w = DenseVector.zeros[Double](dim)
        var z = sc.broadcast(randomVector(dim))
        D.checkpoint()

        breakable {
            for (iter <- 1 to iterations) {
                println("Iter: "+ iter)

                D = D.map(a=>{
                    val r = a._2._3 - z.value
                    val u = a._2._4+a._2._3-z.value
                    val x = a._2._1*(a._2._2+(z.value-a._2._4):*rho)
                    (a._1,(a._2._1, a._2._2, x, u, r))
                })

                D.checkpoint()

                w = D.map(a=>(a._2._3+a._2._4)).reduce(_+_):/ numBlocks.toDouble

                // println("D: "+D.first())
                // println("z: "+z)

                w = soft_threshold(w, lambda, rho*numBlocks)
                // println("Iter: "+iter+" Cost: "+computeCost(data, z.value))

                val t = D.map(a=>{
                    val m = a._2._3 - w
                    m.dot(m)
                }).reduce(_+_)/numBlocks.toDouble

                val z_dif = norm(z.value - w, 2)
                if (Math.sqrt(t) < feastTol && rho*Math.sqrt(numBlocks)*z_dif < convergTol) {
                    break()
                }
                z = sc.broadcast(w)
            }
        }

        w.toArray

    }


    def randomVector(n: Int): DenseVector[Double] = {
        DenseVector.rand(n)
    }


    def soft_threshold(x: DenseVector[Double], lambda: Double, rho: Double): DenseVector[Double] = {
        // argmin lambda|z| + rho/2*(z-x).^2

        val thres: Double = lambda/rho
        val res = DenseVector.zeros[Double](x.length)
        for (i <- 0 until x.length) {
            if (x(i) > thres) {
                res(i) = x(i) - thres
            } else if (x(i) < -thres) {
                res(i) = x(i) + thres
            }
        }
        res

    }

    def computeCost(X: RDD[Array[Double]], w: DenseVector[Double]): Double = {
        val dim = X.first().length-1
        val n = X.count()
        X.map(a=>{
            val res: Double = new DenseVector[Double](a.slice(0, dim)).dot(w) - a(dim)
            res*res
        }).reduce(_+_)/n
    }


}
