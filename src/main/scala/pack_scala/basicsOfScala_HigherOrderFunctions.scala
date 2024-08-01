package pack_scala

object basicsOfScala_HigherOrderFunctions {
  def math(x: Double, y: Double, z: Double, f: (Double, Double) => Double): Double = f(f(x, y), z)

  def maths(x: Double, y: Double, z: Double, m: Double, f: (Double, Double) => Double): Double = f(f(f(x, y), z), m)

  def main(args: Array[String]): Unit = {
    println("===========================Higher Order Functions========================")
    var result = math(50, 20, 10, (x, y) => x + y)
    println(result)
    var result_mul = math(50, 20, 10, (x, y) => x * y)
    println(result_mul)
    var result_min = math(50, 20, 10, (x, y) => x min y)
    println(result_min)
    var result_max = math(50, 20, 10, (x, y) => x max y)
    println(result_max)
    var result_max_4 = maths(50, 20, 10, 15, _ max _) // underscore (_) is a wildcard
    println(result_max_4)
  }

}
