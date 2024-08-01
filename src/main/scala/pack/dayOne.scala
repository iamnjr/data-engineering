package pack

object dayOne {
  def main(args: Array[String]): Unit = {

    val raw_list = List("State->TN~City->Chennai","State->UP~City->Lucknow")
    println
    println("===================raw_list=============================")
    println
    raw_list.foreach(println)

    val split = raw_list.flatMap(x => x.split("~"))
    println
    println("===================Split_list=============================")
    println
    split.foreach(println)

    val state = split.filter(x => x.contains("State"))
    println
    println("===================State_list=============================")
    println
    state.foreach(println)

    val city = split.filter(x => x.contains("City"))
    println
    println("===================city_list=============================")
    println
    city.foreach(println)

    val finalState = state.map(x => x.replace("State->",""))
    println
    println("===================State_list=============================")
    println
    finalState.foreach(println)

    val finalCity = city.map(x => x.replace("City->", ""))
    println
    println("===================City_list=============================")
    println
    finalCity.foreach(println)

    println("=======================The End===========================")




  }

}
