package com.example.spark.sql.util

/**
  * Created by yilong on 2018/7/3.
  */
abstract class CompletionIterator[ +A, +I <: Iterator[A]](sub: I) extends Iterator[A] {
  // scalastyle:on

  private[this] var completed = false
  def next(): A = sub.next()
  def hasNext: Boolean = {
    val r = sub.hasNext
    if (!r && !completed) {
      completed = true
      completion()
    }
    r
  }

  def completion(): Unit
}

object CompletionIterator {
  def apply[A, I <: Iterator[A]](sub: I, completionFunction: => Unit) : CompletionIterator[A, I] = {
    new CompletionIterator[A, I](sub) {
      def completion(): Unit = completionFunction
    }
  }
}
