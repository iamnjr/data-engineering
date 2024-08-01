package pack_scala

object basicsOfScala {
  def main(args: Array[String]): Unit = {
    println("Hello  World!!!!")

    val a = 10
    var b = 10
    b = 20
    println(a, b)

    println("=========================String Interpolation=========================")
    val name = "Neymar"
    val occ = "Footballer"
    val age = 35

    println(name + " is a " + occ + " who is " + age + " old") // General way to printing
    println(s"$name is a $occ who is $age old") // Using String interpolation
    println(s"Hello, \nWorld!!!") //Using escape sequence
    println(raw"Hello, \nWorld") //prints the string raw(as given)


    println("==========================Using If Condition=====================")
    var x = 20
    var result = " "
    x = x - 10
    if (x == 10) {
      result = "True"
    }
    else {
      result = "False"
    }

    println(result)
    println("=================================Loops============================")
    println("=============================While Loop===========================")

    var m = 0

    while (m < 10) {
      println("m = " + m)
      m += 1
    }

    println("=============================Do While Loop===========================")
    var n = 0

    do {
      println("n = " + n)
      n += 1
    } while (n < 10)

    println("==============================For Loop================================")

    for (i <- 1 to 5) {
      println("i using to " + i)
    }

    for (i <- 1.to(5)){
      println("i using to " + i)
    }

    for (i <- 1 until 5) {
      println("i using until " + i)
    }

    for (i <- 1 until 5 ; j <- 1 to 3) {
      println("i using multiple ranges " + i + " "+ j )
    }

    val lst = List(1,2,3,4,5,6,78,98,65,66,87,7)
    for(i <- lst){
      println("i using lst "+i)
    }

    for (i <- lst; if i < 6) {
      println("i using lst " + i)
    }


  }

}
