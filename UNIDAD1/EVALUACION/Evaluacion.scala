//EVALUACION UNIDAD 1

def breakingRecords(score1:List[Int]): Unit={  //(String)
    var i:Int= 10
    println("Score: "+ i)
    //Creacion de variables
    var min = score1(0)
    var minCount = 0;
    var max = score1(0)
    var maxCount = 0;

    //Ciclo condicional
    for(i <-score1){
        println(" "+ i)
        if(i<min){
            min=i
            minCount+=1
            println("Minimos: " + min)
        }
        else if(i > max){
            max=i
            maxCount+=1
            println("Maximos: " + max)
        }
    }
    println(score1)

println("Resultados: " + "Maximos: "+ maxCount + " " + "Minimos: " + minCount)
}
//Valores de ejemplo
var score1 = List(10,5,20,20,4,5,2,25,1)
var score2 = List(3,4,21,36,10,28,35,5,24,42)

//Llamadas a la funcion
breakingRecords(score1)
breakingRecords(score2)