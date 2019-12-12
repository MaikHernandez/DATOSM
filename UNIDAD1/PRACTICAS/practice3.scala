//Algoritmo 1
def fib(num:Int):Int={
  if(num<2)
  {
      return  num
      }
  else
  {
      return fib(num-1)+fib(num-2)
  }
}

println(fib(7))


//Fibonacci Algoritmo 2

def fibo(num2:Int): Double = {
  if(num2<2){
      return num2
  }
  else
  {
      var x=((1+math.sqrt(5))/2)
      var y=((math.pow(x,num2)-math.pow((1-x),num2))/(math.sqrt(5)))
      return y
  }
}

println(fibo(7))


//Fibonacci Algoritmo 3
def fibon(num3:Int):Int = {
  var a=0
  var b=1
  var c=0
  for(c <- Range(0,num3)) {
      val c = b + a
      a=b
      b=c
  }
  return a
}

println(fibon(7))

//Fibonacci Algoritmo 4
def fibona(num4:Int):Int={
  var a=0
  var b=1
  for( k <- Range(0,num4)){
       b = b + a
       a = b - a
  }
  return a
}

println(fibona(7))


//Fibonacci Algoritmo 5
def fibonac(num5: Int): Int = {
   if(num5<2){
       return num5
   }
   else{
       val f: Array[Int] = Array.ofDim[Int](num5 + 2)
       f(0) = 0
       f(1) = 1
       for (i <- Range (2, num5 + 1))
       {
           f(i) = f(i - 1) + f(i - 2)
       }
       return f(num5)
       }
}

println(fibonac(7))


//Fibonacci Algoritmo 6
def fibonacc (num6 : Int) : Double ={
  if (num6 <= 0)
  {
      return 0
  }
  var i = num6-1
  var auxOne = 0.0
  var auxTwo = 1.0
  var ab = Array(auxTwo,auxOne)
  var cd = Array(auxOne,auxTwo)
  while (i>0)
  {
      if (i % 2 != 0)
      {
          auxOne = cd(1) * ab(1) + cd(0) * ab(0)
          auxTwo = cd(1) * (ab(1)+ab(0)) + cd(0)*ab(1)
          ab(0) = auxOne
          ab(1) = auxTwo
      }
      auxOne = (math.pow(cd(0),2)) + (math.pow(cd(1),2))
      auxTwo = cd(1)* (2*cd(0) + cd(1))
      cd(0) = auxOne
      cd(1) = auxTwo
      i = i/2
  }
  return (ab(0) + ab(1))
}

println(fibonacc(7))
