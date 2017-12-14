#!/bin/bash
#################################################################################################################################
# NOMBRE PROCESO : run_processor.sh                                                                                             #
#################################################################################################################################
# PORVEEDOR       : INDRA                                                                                                       #
# AUTOR           : Felipe Cordova Jovenich                                                                                     #
# FECHA CREACION  : 25-11-2017                                                                                                  #
# DESCRIPCION     : Ejecuta procesos de carga en Hive                                                                           #
# FORMA EJECUCION : Invocación desde Control-M                                                                                  #
# ----------------------------------------------------------------------------------------------------------------------------- #
# CONTROL DE CAMBIOS                                                                                                            #
# ----------------------------------------------------------------------------------------------------------------------------- #
#                                                                                                                               #
# FECHA MODIFICACION :                                                                                                          #
# AUTOR              :                                                                                                          #
# DESCRIPCION        :                                                                                                          #
#################################################################################################################################
HADOOP_USER_NAME=srv_ingesta

klist -s
if [[ "$?" == "1" ]]; then
 kinit -kt /user/${HADOOP_USER_NAME}/${HADOOP_USER_NAME}.keytab ${HADOOP_USER_NAME}@BIGDATA.TCHILE.LOCAL  
#kinit -kt /home/TCHILE.LOCAL/$HADOOP_USER_NAME/.ssh/$HADOOP_USER_NAME.keytab $HADOOP_USER_NAME@BIGDATA.TCHILE.LOCAL
fi

##########################
# INICIO DE SHELL SCRIPT #
##########################


#RUTA ARCHIVO LOGS
#HADOOP_USER_NAME=srv_ingesta
PATH_EXE=/user/$HADOOP_USER_NAME/oozie/urm/
LOG_PATH=$PATH_EXE/logs/

#eliminar despues
echo "Hola Mundo" | hdfs dfs -put - /tmp/prueba_proceso/dato1.txt


# VERIFICA PARAMETROS DE ENTRADA
[ -z $1 ] && echo -e "SHELL: PARAMETRO 1 FALTANTE"
[ -z $2 ] && echo -e "SHELL: PARAMETRO 2 FALTANTE"

#eliminar despues
echo "Hola Mundo" | hdfs dfs -put - /tmp/prueba_proceso/dato2.txt

#VARIABLES DE LA SHELL
TABLE=`echo ${1} |tr '[:upper:]' '[:lower:]'`
FREQ=`echo ${2} |tr '[:upper:]' '[:lower:]'`
HADOOP_USER_NAME=srv_ingesta
SCHEMA=urm
#TABLE=call_cdr
#FREQ=diario
DELIMITER=\|
MODE=1

#RUTAS Y DIRECTORIOS
TEMPDIR=/tmp/$RANDOM
USER_PATH=/user/$HADOOP_USER_NAME/stage/${SCHEMA}

# PARAMETRO DE PID
ID_PROCC=$$

#CAMPOS PARA ANONIMIZAR
case ${TABLE} in
     mobile_line) COLUMNAS=2,3,5,6;;
     call_cdr) COLUMNAS=2,3,6,7;;
	 data_xdr) COLUMNAS=2,3;;
	 sms) COLUMNAS=2,3,5,6;;
	 mobile_line_*_traffic) COLUMNAS=5;;
	 mobile_line_*_tariff) COLUMNAS=5;;
	 mobile_service) COLUMNAS=4,5;;
     mobile_service_movement) COLUMNAS=4,5;;
     *) echo "NO COINCIDE CON NINGUNA TABLA" 
	 exit 1

esac  

#eliminar despues
echo "Hola Mundo" | hdfs dfs -put - /tmp/prueba_proceso/dato3.txt

echo `date +"%Y-%m-%d %T"`" ###########################################################"
echo `date +"%Y-%m-%d %T"`" # Inicio proceso run_processor.sh                         #"
echo `date +"%Y-%m-%d %T"`" #---------------------------------------------------------#"
echo `date +"%Y-%m-%d %T"`" # USER_PATH  =  ${USER_PATH}                               "
echo `date +"%Y-%m-%d %T"`" # LOG_PATH   =  ${LOG_PATH}                                "
echo `date +"%Y-%m-%d %T"`" # TEMPDIR    =  ${TEMPDIR}                                 "
echo `date +"%Y-%m-%d %T"`" # SCHEMA     =  ${SCHEMA}                                  "
echo `date +"%Y-%m-%d %T"`" # TABLE      =  ${TABLE}                                   "
echo `date +"%Y-%m-%d %T"`" # FREQ       =  ${FREQ}                                    "
echo `date +"%Y-%m-%d %T"`" # DELIMITER  =  ${DELIMITER}                               "
echo `date +"%Y-%m-%d %T"`" # ID PROCESO =  ${ID_PROCC}                                "
echo `date +"%Y-%m-%d %T"`" ###########################################################"



filein=$(hadoop fs -ls -r /stage/${SCHEMA}/${TABLE}/landing| tail -1 | awk '{print$8}')
echo `date +"%Y-%m-%d %T"`" # Archivo con Ruta :' ${filein} " 

filename="${filein##*/}"
filenameN="${filename%.*}"
echo `date +"%Y-%m-%d %T"`" # Nombre Archivo Sin Extension:' ${filenameN}" 
#fechas=`echo ${filenameN} | grep -oE [0-9T_]+$`
fechas=`echo ${filenameN} | grep -oE '([0-9]{8}T[0-9]{6}_){3}[0-9]{2}'`
nombre=`echo ${filenameN} | grep -Eo '^([a-Z]+_)+'`
nombreArchivo="${nombre%*_}"
fechaDesde=`echo $fechas | awk -F "_" '{print$1}'`
fechaHasta=`echo $fechas | awk -F "_" '{print$2}'`
fechaEjecucion=`echo $fechas | awk -F "_" '{print$3}'`
numeroArchivo=`echo $fechas | awk -F "_" '{print$4}'`

#eliminar despues
echo "Hola Mundo" | hdfs dfs -put - /tmp/prueba_proceso/dato4.txt

#VALIDACION FRECUENCIA PARA PARTICION
if [[ "$FREQ" == "mensual" ]]; 
	then
	partition_date=`echo $fechaDesde | cut -c1-6`
	else
	partition_date=`echo $fechaDesde | cut -c1-8`
fi

echo `date +"%Y-%m-%d %T"`" --------------------------------------"
echo `date +"%Y-%m-%d %T"`" # Fecha Desde    :' $fechaDesde       "
echo `date +"%Y-%m-%d %T"`" # Fecha Hasta    :' $fechaHasta       "
echo `date +"%Y-%m-%d %T"`" # Fecha Ejecucion:' $fechaEjecucion   "
echo `date +"%Y-%m-%d %T"`" # Numero Archivo :' $numeroArchivo    "
echo `date +"%Y-%m-%d %T"`" # Nombre Archivo :' $nombreArchivo    "
echo `date +"%Y-%m-%d %T"`" # Fecha Particion:' $partition_date   "
echo `date +"%Y-%m-%d %T"`" # Campos Spicy   :' $COLUMNAS         "
echo `date +"%Y-%m-%d %T"`" --------------------------------------"

#MOVER ARCHIVOS DE PROCESSING QUE PUEDAN HABER QUEDADO REZAGADOS , AL ARCHIVE
hadoop fs -mv /stage/${SCHEMA}/${TABLE}/processing/* /stage/${SCHEMA}/${TABLE}/archive/ &> /dev/null

#MOVER ARCHIVO A PROCESAR
echo -e "\nSE MOVERAN LOS SIGUIENTES ARCHIVOS AL DIRECTORIO DE PROCESO\n" 
hadoop fs -mv /stage/${SCHEMA}/${TABLE}/landing/${filenameN}.txt /stage/${SCHEMA}/${TABLE}/processing/${filenameN}.txt
        if [ $? -ne 0 ];    then
            echo -e "\n\nWARNING:  FALLO MIENTRAS SE TRANFERIA EL ARCHIVO ${filein} AL DIRECTORIO" 
            exit 1
		else 
		hadoop fs -ls /stage/${SCHEMA}/${TABLE}/processing
        fi

#Eliminar Despues
echo "Hola Mundo" | hdfs dfs -put - /tmp/prueba_proceso/dato5.txt

#EJECUCION DE HIVE  

path_archivo=`echo /stage/${SCHEMA}/${TABLE}/processing/`
input_spicy=$path_archivo

beeline -u "jdbc:hive2://prd7404.bigdata.tchile.local:2181,prd7432.bigdata.tchile.local:2181,prd7460.bigdata.tchile.local:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" \
           -f ${PATH_EXE}generic/${TABLE}.hql --hivevar SCHEMA=${SCHEMA} --hivevar USER_PATH=${path_archivo} --hivevar fecha=${partition_date}

	if [ $? -ne 0 ];    then
		echo "ERROR HIVE" | hdfs dfs -put - /tmp/prueba_proceso/ERROR1.txt    
		echo -e "\n\nWARNING:  FALLO LA CARGA EN HIVE PARA EL ARCHIVO ${filein} EN LA TABLA ${SCHEMA}.${TABLE}" 
				hadoop fs -mv /stage/${SCHEMA}/${TABLE}/processing/${filenameN}.txt /stage/${SCHEMA}/${TABLE}/landing/${filenameN}.txt
        exit 1
	else
	#Eliminar Despues
	echo "Hola Mundo" | hdfs dfs -put - /tmp/prueba_proceso/dato6.txt
	fi

#EJECUCION SI EL ARCHIVO TIENE ENRRIQUECIDO
if [ ${TABLE} == "mobile_line" ] || [ ${TABLE} == "call_cdr" ] || [ ${TABLE} == "data_xdr" ] ; then

echo `date +"%Y-%m-%d %T"`"Archivo con Ejecucion de Extraccion " 

hadoop fs -rm -r -skipTrash /stage/${SCHEMA}/${TABLE}/output/*	
	
path_archivo_out=`echo /stage/${SCHEMA}/${TABLE}/output/`
input_spicy=$path_archivo_out

beeline -u "jdbc:hive2://prd7404.bigdata.tchile.local:2181,prd7432.bigdata.tchile.local:2181,prd7460.bigdata.tchile.local:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" \
            -f ${PATH_EXE}generic/out_${TABLE}.hql --hivevar SCHEMA=${SCHEMA} --hivevar USER_PATH=${path_archivo_out} --hivevar fecha=${partition_date} 

	if [ $? -ne 0 ];    then
		echo "ERROR HIVE" | hdfs dfs -put - /tmp/prueba_proceso/ERROR2.txt
		echo -e "\n\nWARNING:  FALLO LA EJECUCION DE SPICY Y COMPRESION A BZ2 DE ARCHIVO EXTRACCION" 
		exit 1
	else
	#Eliminar Despues
		echo "Hola Mundo" | hdfs dfs -put - /tmp/prueba_proceso/dato7.txt
	fi

hadoop fs -text ${path_archivo_out}*.deflate | hadoop fs -put - ${path_archivo_out}${filenameN}.txt

fi		
#Eliminar Despues
echo "Hola Mundo" | hdfs dfs -put - /tmp/prueba_proceso/dato8.txt
#EJECUCION SPICY Y BZ2

hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar -Dmapreduce.job.queue.name=hdpappHIGH -Dmapred.output.compress=true \
           -Dmapred.compress.map.output=true -Dmapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec -Dmapred.reduce.tasks=1 \
           -input ${input_spicy}${filenameN}.txt -output ${TEMPDIR}/ \
           -mapper "/usr/bin/spicy -columns=${COLUMNAS} -salt="*********************************" -sep="'|'" -skip=false" 
      
        if [ $? -ne 0 ];    then
            echo -e "\n\nWARNING:  FALLO LA EJECUCION DE SPICY Y COMPRESION A BZ2" 
#Eliminar Despues
echo "Hola error" | hdfs dfs -put - /tmp/prueba_proceso/error3.txt
            exit 1
		else
		echo "ANONIMIZADO Y COMPRIMIDO OK , SE MUEVE ARCHIVO ORIGEN A ARCHIVO" 
		hadoop fs -mv /stage/${SCHEMA}/${TABLE}/processing/${filenameN}.txt /stage/${SCHEMA}/${TABLE}/archive/${filenameN}.txt 
		echo "SE MUEVE ARCHIVO COMPRIMIDO A GLOBAL" 
		hadoop fs -mkdir /stage/${SCHEMA}/CL/${nombreArchivo}/${fechaDesde}_${fechaHasta}/ &> /dev/null
		hadoop fs -mv ${TEMPDIR}/part-00000.bz2 /stage/${SCHEMA}/CL/${nombreArchivo}/${fechaDesde}_${fechaHasta}/${filenameN}.bz2	 
		fi
#Eliminar Despues
echo "Hola Mundo" | hdfs dfs -put - /tmp/prueba_proceso/dato9.txt
		
hadoop fs -rm -r -skipTrash ${TEMPDIR}/* &> /dev/null
echo "Hola Mundo" | hdfs dfs -put - /tmp/prueba_proceso/dato10.txt
hadoop fs -rm -r -skipTrash ${TEMPDIR} &> /dev/null
echo "Hola Mundo" | hdfs dfs -put - /tmp/prueba_proceso/dato11.txt




