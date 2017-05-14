package org.test.spark1

class ADD {
  def add(x:Int,y:Int){
    val z = x+y
    println("and the add of two number is :"+z)
  }
}
object sol extends App{
val a = new ADD
println("enter the first number:")
val a1 = readInt()
println("enter the second2 number:")
val a2 = readInt()
a.add(a1, a2)
}