package pack_scala

object Math {
  def square(x: Int): Int = {
    x * x
  }

}

object basicsOfScala_Fuctions {
  def add(x: Int, y: Int): Int = {
    x + y
  }

  def subtract(x: Int, y: Int): Int = {
    x - y
  }

  def multiplyy(x: Int, y: Int): Int = {
    x * y
  }

  def divide(x: Double, y: Double): Double = {
    x / y
  }

  def print(x: Int,y: Int): Unit={
    println(x+y);
  }

  def main(args: Array[String]): Unit = {
    println("===============================Functions=========================")
    println(add(5, 10))
    println(subtract(5, 10))
    println(multiplyy(5, 10))
    println(divide(5, 10))
    println(Math.square(5))
    print(100,200)

    println("===============================Anonymous Functions===============")

    var addition = (x: Int,y:Int) => (x+y);
    println(add(20,50))



  }

}
