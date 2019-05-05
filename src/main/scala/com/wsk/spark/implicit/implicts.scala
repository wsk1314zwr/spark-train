package com.wsk.spark.`implicit`

import ImplicitsOop.{Age, Sex}

/**
  * 隐式转换使用
  */
class Man(val name: String)

/*object  Man{
  implicit def man2superMan(man:Man)=new SuperMan(man.name)
}*/
class SuperMan(val name: String) {
  def make2super = println(this.name + " super super")
}

object implicts {
  implicit def man2superMan(man: Man) = new SuperMan(man.name)
}

object Param{
  implicit val a = new Sex("妖怪")
  implicit val b = new Age(110)
}

object ImplicitsOop {

  def main(args: Array[String]): Unit = {
    import implicts._ //如果我们借助伴生对象这个来定义隐式转换的操作，我们可以定义object 隐式转换的方法
    //使用 的时候导入隐式转换的包就可以
    val man = new Man("spark")
    man.make2super //按照正常来说man对象不能直接调用make2super方法，但是我们可以看到
    //Man的伴生对象里面做了一次隐式转换的操作，所以man对象能调用了
    talk("scala")("spark")

    //    implicit val  context="hadoop1"
    implicit val context1 = "hadoop1"
    //    implicit val  context2="hadoop1"
    //    implicit val  context3="hadoop3"
    talk("scala") //也可以在上面直接定义这个context在使用的时候可以不传递，如果需要传递也可以按照正常的方法
    //传递或者修改的操作
    //    implicit val  context2="hadoop2" //若声明了多个string类型的隐士转换，编译器会混淆，导运行不成功
    talk("java") //也可以在上面直接定义这个context在使用的时候可以不传递，如果需要传递也可以按照正常的方法
    //传递或者修改的操作
    //    implicit val  context3="hadoop3"//若声明了多个string类型的隐士转换，编译器会混淆，导运行不成功
    talk("c++") //也可以在上面直接定义这个context在使用的时候可以不传递，如果需要传递也可以按照正常的方法
    //传递或者修改的操作

    //隐式转换多参数
    import Param._
    wskTest("王寿奎")
    wskTest("严欢")
    wskTest("刘欢")
    wskTest("吴锐")
  }

  def talk(name: String)(implicit context: String) = println(name + " talk to " + context)

  def wskTest(name: String)(implicit sex: Sex, age: Age):Unit = {
    println(name+":"+sex.sex+":"+age.age)
  }

  case class Sex(sex: String)

  case class Age(age: Int)



}

