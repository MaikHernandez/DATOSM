// Assessment 1/Practica 1
//1. Desarrollar un algoritmo en scala que calcule el radio de un circulo
def radio(Area:Double):Double={
    (math.sqrt(Area/3.1416))
}

radio(50)

radio(70.50)

//2. Desarrollar un algoritmo en scala que me diga si un numero es primo
def isPrime(num:Int):Boolean= (num>1) && !(2 to scala.math.sqrt(num).toInt).exists(x => num % x==0)

isPrime(18)

isPrime(13)

//3. Dada la variable bird = "tweet", utiliza interpolacion de string para
//   imprimir "Estoy ecribiendo un tweet"
var bird="tweet"
println(s"Estoy escribiendo un $bird")


//4. Dada la variable mensaje = "Hola Luke yo soy tu padre!" utiliza slilce para extraer la
//   secuencia "Luke"
var mensaje = "Hola Luke yo soy tu padre!"
mensaje
mensaje.slice(5,9)


//5. Cual es la diferencia en value y una variable en scala?
val Number1 = 5

//Number1 = 6

Number1


//6. Dada la tupla (2,4,5),(1,2,3),(3.1416,23))) regresa el numero 3.1416 
val tupla=((2,4,5), (1,2,3), (3.1416,23))
tupla
println(tupla._3._1)
