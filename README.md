# Fase2
En este repositorio se almacenará toda la información referente a Big Data TCH disponible para público en general

Código para ejecutar jar: "MobileLine" en BlueMix

spark-submit --master yarn --class com.tchile.bigdata.MobileLine --files log4j.properties --verbose --conf spark.debug.maxToStringFields=100 MobileLine.jar config.txt
