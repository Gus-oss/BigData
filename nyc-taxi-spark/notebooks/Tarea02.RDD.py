#------------------------------------------------------------------------------
#Librerias y configuracion de entorno 
import os
import warnings
warnings.filterwarnings('ignore')
os.environ["HADOOP_HOME"] = "C:\\hadoop"
from pyspark import SparkConf, SparkContext

#------------------------------------------------------------------------------
# Configuración Spark
conf = SparkConf() \
    .setAppName("NYC-Taxi-RDD") \
    .setMaster("local[*]") \
    .set("spark.driver.memory", "2g") \
    .set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
#------------------------------------------------------------------------------
print("="*50)
print("ANÁLISIS NYC TAXI CON RDD")
print("="*50)

# Ruta de datos
DATA_PATH = "C:/Users/PC/Documents/DocumentosGustavo/Github/Maestria/BigData/nyc-taxi-spark/data/yellow/2024/yellow_tripdata_2024-01.parquet"

# Cargar como RDD
from pyspark.sql import SparkSession
spark = SparkSession(sc)
df = spark.read.parquet(DATA_PATH)

# Convertir a RDD de tuplas
rdd = df.select("fare_amount", "trip_distance", "passenger_count", "tpep_pickup_datetime") \
        .rdd \
        .map(lambda row: (row.fare_amount, row.trip_distance, row.passenger_count, row.tpep_pickup_datetime))

rdd = rdd.cache()

# ----------------------------------------------------------------------------
# OPERACIONES RDD

# 1. COUNT
print(f"\n1. Total registros: {rdd.count():,}")

# 2. FIRST
print(f"\n2. Primer registro: {rdd.first()}")

# 3. TAKE
print(f"\n3. Primeros 3 registros:")
for r in rdd.take(3):
    print(f"   {r}")

# 4. MAP + FILTER: Tarifas válidas
tarifas = rdd.map(lambda x: x[0]) \
             .filter(lambda x: x is not None and x > 0)

suma = tarifas.sum()
conteo = tarifas.count()
print(f"\n4. Tarifa promedio: ${suma/conteo:.2f}")

# 5. REDUCE: Suma total de tarifas
total_tarifas = tarifas.reduce(lambda a, b: a + b)
print(f"\n5. Suma total tarifas: ${total_tarifas:,.2f}")

# 6. MAP + REDUCEBYKEY: Viajes por pasajeros
por_pasajeros = rdd.map(lambda x: (x[2], 1)) \
                   .reduceByKey(lambda a, b: a + b) \
                   .collect()

print(f"\n6. Viajes por pasajeros:")
for p, n in sorted(por_pasajeros, key=lambda x: -x[1])[:5]:
    print(f"   {p} pasajero(s): {n:,}")

# 7. MAP + REDUCEBYKEY + SORTBY: Top horas
top_horas = rdd.map(lambda x: (x[3].hour, 1)) \
               .reduceByKey(lambda a, b: a + b) \
               .sortBy(lambda x: -x[1]) \
               .take(5)

print(f"\n7. Top 5 horas pico:")
for h, n in top_horas:
    print(f"   {h:02d}:00 -> {n:,} viajes")

# 8. DISTINCT: Valores únicos de pasajeros (filtrando None)
pasajeros_unicos = rdd.map(lambda x: x[2]) \
                      .filter(lambda x: x is not None) \
                      .distinct() \
                      .collect()
print(f"\n8. Valores únicos de pasajeros: {sorted(pasajeros_unicos)}")

# 9. STATS: Estadísticas de distancia
distancias = rdd.map(lambda x: x[1]).filter(lambda x: x is not None and x > 0)
stats = distancias.stats()
print(f"\n9. Estadísticas de distancia:")
print(f"   Count: {stats.count():,}")
print(f"   Mean: {stats.mean():.2f} mi")
print(f"   Stdev: {stats.stdev():.2f} mi")
print(f"   Min: {stats.min():.2f} mi")
print(f"   Max: {stats.max():.2f} mi")

# 10. AGGREGATEBYKEY: Promedio de tarifa por pasajeros
# (más eficiente que groupByKey para agregaciones)
suma_conteo = rdd.filter(lambda x: x[0] is not None and x[2] is not None) \
                 .map(lambda x: (x[2], (x[0], 1))) \
                 .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) \
                 .mapValues(lambda x: x[0]/x[1]) \
                 .collect()

print(f"\n10. Tarifa promedio por pasajeros:")
for p, avg in sorted(suma_conteo, key=lambda x: x[0])[:6]:
    print(f"    {p} pasajero(s): ${avg:.2f}")

print("\n" + "="*50)
print("FIN DEL ANÁLISIS RDD")
print("="*50)

sc.stop()
#------------------------------------------------------------------------------