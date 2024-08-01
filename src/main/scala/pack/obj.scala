package pack

object obj {
  def main(args: Array[String]): Unit = {

    println("==================Started===================== ")

    val a = 2

    val b = 3

    val c = "zeyobron"

    println(a)
    println(b)
    println(c)
    println


    val list_1 = List(1,2,3,4,5)
    println(list_1)
    list_1.foreach(println)
    println
    println(list_1.size)
    println("==============Add 2 to each element================")
    val add_list = list_1.map(x => x+2)
    println(add_list)
    add_list.foreach(println)
    println

    println("===============Multiply 2 to each element==================")
    val mul_list = list_1.map(x => x*2)
    println(mul_list)
    mul_list.foreach(println)
    println

    println("==================Filter greater than 2 ==================")
    val filter_list = list_1.filter(x => x>2)
    println(filter_list)
    filter_list.foreach(println)
    println

    println("==================Raw list==============================")
    val raw_list = List("zeyo","zeyob","sai")
    println(raw_list)
    raw_list.foreach(println)
    println

    println("===================concat analytics=======================")
    val con_list = raw_list.map(x => x+" "+"analytics")
    println(con_list)
    con_list.foreach(println)
    println

    println("================== Contains Zeyo========================")
    val cont_list = raw_list.filter(x => x.contains("zeyo"))
    println(cont_list)
    cont_list.foreach(println)
    println

    println("====================Replace zeyo with tera=================")
    val rep_list = raw_list.map(x => x.replace("zeyo","tera"))
    println(rep_list)
    rep_list.foreach(println)
    println

    println("=================== Raw list FlatMap ============================")

    val raw_list_2 = List("A~B","C~D","E~F")
    val split = raw_list_2.flatMap(x => x.split("~"))
    println(split)
    split.foreach(println)
    println


    println("=================The End=========================================")
  }

}
