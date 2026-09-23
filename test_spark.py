import os
import sys

# 1. Résolution dynamique du dossier PySpark dans le .venv
venv_pyspark = os.path.join(sys.prefix, "Lib", "site-packages", "pyspark")
jars_path = os.path.join(venv_pyspark, "jars", "*")

# 2. Configuration des chemins
os.environ["SPARK_HOME"] = venv_pyspark
os.environ["JAVA_HOME"] = r"C:\Java\jdk-21.0.12.1"
os.environ["HADOOP_HOME"] = r"C:\hadoop"

# 3. Alignement du PATH
os.environ["PATH"] = (
    f"{os.environ['JAVA_HOME']}\\bin;{os.environ['HADOOP_HOME']}\\bin;{venv_pyspark}\\bin;"
    + os.environ["PATH"]
)

from pyspark.sql import SparkSession

# 4. Injonction des options JVM requises pour Java 21+ et construction du Classpath
spark = (
    SparkSession.builder
    .appName("test_spark")
    .master("local[*]")
    .config("spark.driver.extraClassPath", jars_path)
    .config("spark.executor.extraClassPath", jars_path)
    .config(
        "spark.driver.extraJavaOptions",
        "-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.internal/sun.misc=ALL-UNNAMED"
    )
    .getOrCreate()
)

print("\n----------------------------------------")
print(" SUCCÈS : SparkSession initialisée !")
print(" Test de comptage :", spark.range(10).count())
print("----------------------------------------\n")

spark.stop()