#------------------------------------------------------------------------------
#Librerias y configuracion de entorno 
import os
import warnings
warnings.filterwarnings('ignore')
os.environ["HADOOP_HOME"] = "C:\\hadoop"
from pyspark import SparkConf, SparkContext

#------------------------------------------------------------------------------
# Configuración Spark , SparkConf es el objeto que guarda toda la configuracion de la sesion 
#setAppName: Nombre de la aplicacion
#setMaster: la mas importante, define donde y como ejecutar spark , local[1] es para ejecutar en modo local con 1 hilo,
#           local[*] para usar todos los hilos disponibles, 
#           spark://host:port para conectar a un cluster spark, 
#           yarn para ejecutar en un cluster gestionado por YARN, 
#           mesos para ejecutar en un cluster gestionado por Mesos
#set("spark.driver.memory", "2g") y set("spark.executor.memory", "2g") para asignar memoria al driver y a los ejecutores respectivamente
# el driver es el proceso principal que controla la aplicacion spark, los ejecutores son los procesos que realizan el trabajo de 
#procesar datos en palalelo
conf = SparkConf() \
    .setAppName("NYC-Taxi-RDD") \
    .setMaster("local[*]") \
    .set("spark.driver.memory", "2g") \
    .set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
#------------------------------------------------------------------------------