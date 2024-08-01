package pack_scala

object basicsOfSacla_2 {
  def main(args: Array[String]): Unit = {
    println("===========================Using Match Expression===================")

    val age = 70

    age match {
      case 20 => println(age);
      case 50 => println(age);
      case 60 => println(age);
      case _ => println("default")
    }

    val name ="njr"

    name match {
      case "abc" => println(name);
      case "njr" => println(name);
      case _ => println("default")
    }

    val num = 3
    num match {
      case 1 | 3| 5| 7|9 => println("odd");
      case 2 | 4| 6| 8|10 => println("even");
      case _ => println("default")
    }



  }

}
