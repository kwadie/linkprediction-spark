package de.tuberlin.dima.aim3.linkpredection




class SHelloWorld {
  def printMessage() = {
    println("Hello from Scala!")
  }
}

object SHelloWorld {
  def main(arguments: Array[String]) = {
    new SHelloWorld().printMessage()
    new JHelloWorld().printMessage()
  }
}