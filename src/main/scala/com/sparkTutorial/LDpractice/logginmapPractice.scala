package com.sparkTutorial.LDpractice

object logginmapPractice extends App {
  val t0: Long = System.currentTimeMillis()
  val tt0: Long = System.nanoTime()

  var prevValue: Double = null.asInstanceOf[Double]
  var prevValue_null: Double = _
  println(prevValue)
  println(prevValue_null)

  val t1: Long = System.currentTimeMillis()
  val tt1: Long = System.nanoTime()
  println("Execution duration: nanoTime: " + (tt1 - tt0).asInstanceOf[Double] /  1000000000)
}
