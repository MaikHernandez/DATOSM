//Librerias a utilizar
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.apache.log4j._
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, VectorIndexer, OneHotEncoder}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.evaluation.MulticlassMetrics

//Para que no salga errores
Logger.getLogger("org").setLevel(Level.ERROR)

//Iniciamos la sesion en spark
val spark = SparkSession.builder().getOrCreate()

//Cargamos el dataset
val databank = spark.read.option("header","true").option("inferSchema", "true").option("delimiter",";").format("csv").load("BIG-DATA/BigData-master/Spark_LogisticRegression/bank-full.csv")

databank.printSchema()
databank.head(1)

val labelIndexer = new StringIndexer().setInputCol("y").setOutputCol("label").fit(databank)

//Se Crea un nuevo objecto VectorAssembler llamado assembler para los feature
val assembler = (new VectorAssembler().setInputCols(Array("age","balance","day","duration","campaign","pdays","previous")).setOutputCol("features"))

//Utilice randomSplit para crear datos de train y test divididos en 70/30
val Array(training, test) = databank.randomSplit(Array(0.7, 0.3), seed = 12345)

//Se Crea un nuevo objeto de  LogisticRegression llamado lr
val lr = new LogisticRegression()

//Se crea un nuevo  pipeline con los elementos: assembler, lr
val pipeline = new Pipeline().setStages(Array(labelIndexer,assembler,lr))

//Ajuste (fit) el pipeline para el conjunto de training
val model = pipeline.fit(training)

//Tome los Resultados en el conjuto Test con transform
val results = model.transform(test) 

//Convierta los resutalos de prueba (test) en RDD utilizando .as y .rdd
val predictionAndLabels = results.select($"prediction",$"label").as[(Double, Double)].rdd

//Inicialice un objeto MulticlassMetrics 
val metrics = new MulticlassMetrics(predictionAndLabels)

//Imprima la  Confusion matrix
println("Confusion matrix:")
println(metrics.confusionMatrix)
metrics.accuracy 

/*println(metrics.confusionMatrix)
11989.0  175.0                                                                  
1331.0   246.0  
*/