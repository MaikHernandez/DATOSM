// Importe una SparkSession con la libreria Logistic Regression
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession

// Optional: Utilizar el codigo de Error reporting
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

// Cree un sesion Spark 
val spark = SparkSession.builder().getOrCreate()

// Utilice Spark para leer el archivo csv Advertising.
val data = spark.read.option("header","true").option("inferSchema", "true").format("csv").load("BIG-DATA/BigData-master/Spark_LogisticRegression/advertising.csv")

// Imprima el Schema del DataFrame
data.printSchema()

///////////////////////
/// Despliegue los datos /////
/////////////////////

// Imprima un renglon de ejemplo 

////////////////////////////////////////////////////
//// Preparar el DataFrame para Machine Learning ////
//////////////////////////////////////////////////

// Hacer lo siguiente:
// - Renombre la columna "Clicked on Ad" a "label"
// - Tome la siguientes columnas como features "Daily Time Spent on Site","Age","Area Income","Daily Internet Usage","Timestamp","Male"
// - Cree una nueva clolumna llamada "Hour" del Timestamp conteniendo la "Hour of the click"
data.select("Clicked on Ad").show()
val timedata = data.withColumn("Hour",hour(data("Timestamp")))

val logregdata = (timedata.select(data("Clicked on Ad").as("label"),
$"Daily Time Spent on Site",$"Age",$"Area Income",
$"Daily Internet Usage",$"Hour",$"Male")
)
// Importe VectorAssembler y Vectors
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

// Cree un nuevo objecto VectorAssembler llamado assembler para los feature
val assembler = ( new VectorAssembler().setInputCols(
Array("Daily Time Spent on Site","Age","Area Income",
"Daily Internet Usage","Hour","Male")).setOutputCol("features")
)

// Utilice randomSplit para crear datos de train y test divididos en 70/30
val Array(training,test) = logregdata.randomSplit(Array(0.7,0.3),seed=12345)

///////////////////////////////
//  ### Configure un Pipeline ///////
/////////////////////////////

// Importe Pipeline
import org.apache.spark.ml.Pipeline
// Cree un nuevo objeto de LogisticRegression llamado lr
val lr = new LogisticRegression()
// Cree un nuevo pipeline con los elementos: assembler, lr
val pipeline = new Pipeline().setStages(Array(assembler,lr))

// Ajuste (fit) el pipeline para el conjunto de training.
val model = pipeline.fit(training)

// Se toma los Resultados en el conjuto Test con transform
val results = model.transform(test)

////////////////////////////////////
//// Evaluacion del modelo /////////////
//////////////////////////////////
// Para Metrics y Evaluation importe MulticlassMetrics
import org.apache.spark.mllib.evaluation.MulticlassMetrics

// Convierta los resutalos de prueba (test) en RDD utilizando .as y .rdd
val predictionAndLabels = results.select($"prediction",$"label").as[(Double,Double)].rdd

// Inicialice un objeto MulticlassMetrics 
val metrics = new MulticlassMetrics(predictionAndLabels)

// Se imprime la confusion matrix
println("confusion Matrix ")
println(metrics.confusionMatrix)