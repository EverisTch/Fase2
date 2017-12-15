#!/bin/bash
#################################################################################################################################
# NOMBRE PROCESO : run_spicy.sh                                                                                                 #
#################################################################################################################################
# PORVEEDOR       : INDRA                                                                                                       #
# AUTOR           : Felipe Cordova Jovenich                                                                                     #
# FECHA CREACION  : 25-11-2017                                                                                                  #
# DESCRIPCION     : Ejecuta procesos de Anonimizacion                                                                           #
# FORMA EJECUCION : Invocación desde Oozie                                                                                      #
# ----------------------------------------------------------------------------------------------------------------------------- #
# CONTROL DE CAMBIOS                                                                                                            #
# ----------------------------------------------------------------------------------------------------------------------------- #
#                                                                                                                               #
# FECHA MODIFICACION :                                                                                                          #
# AUTOR              :                                                                                                          #
# DESCRIPCION        :                                                                                                          #
#################################################################################################################################
#FILE_NAME=$1
#COLUMNAS=$2
#INPUT_SPICY=$3

HADOOP_USER_NAME=srv_ingesta

klist -s
if [[ "$?" == "1" ]]; then
 kinit -kt /user/${HADOOP_USER_NAME}/${HADOOP_USER_NAME}.keytab ${HADOOP_USER_NAME}@BIGDATA.TCHILE.LOCAL  
fi

##########################
# INICIO DE SHELL SCRIPT #
##########################

# VERIFICA PARAMETROS DE ENTRADA
[ -z $1 ] && echo -e "SHELL: PARAMETRO 1 FALTANTE"
[ -z $2 ] && echo -e "SHELL: PARAMETRO 2 FALTANTE"
[ -z $3 ] && echo -e "SHELL: PARAMETRO 2 FALTANTE"

#DIRECTORIOS
TEMPDIR=/tmp/$RANDOM

#DESCOMPOSICION DE VARIABLES
FILE_NAME=$1
COLUMNAS=$2
INPUT_SPICY=$3
filenameN="${FILE_NAME%.*}"
#echo `date +"%Y-%m-%d %T"`" # Nombre Archivo Sin Extension:' ${filenameN}" 
fechas=`echo ${filenameN} | grep -oE '([0-9]{8}T[0-9]{6}_){3}[0-9]{2}'`
nombre=`echo ${filenameN} | grep -Eo '^([a-Z]+_)+'`
nombreArchivo="${nombre%*_}"
fechaDesde=`echo $fechas | awk -F "_" '{print$1}'`
fechaHasta=`echo $fechas | awk -F "_" '{print$2}'`
fechaEjecucion=`echo $fechas | awk -F "_" '{print$3}'`
numeroArchivo=`echo $fechas | awk -F "_" '{print$4}'`


#EJECUCION SPICY Y BZ2

hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar -Dmapreduce.job.queue.name=hdpappHIGH -Dmapred.output.compress=true \
           -Dmapred.compress.map.output=true -Dmapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec -Dmapred.reduce.tasks=1 \
           -input ${input_spicy}${filenameN}.txt -output ${TEMPDIR}/ \
           -mapper "/usr/bin/spicy -columns=${COLUMNAS} -salt="yMo2r7fp5L5RiUah/O65MR30tkfL43b4eDDc2tFs0MM=" -sep="'|'" -skip=false" 
      
        if [ $? -ne 0 ];    then
            echo -e "\n\nWARNING:  FALLO LA EJECUCION DE SPICY Y COMPRESION A BZ2" 
            exit 1
		else
#		echo "ANONIMIZADO Y COMPRIMIDO OK , SE MUEVE ARCHIVO ORIGEN A ARCHIVO" 
		hadoop fs -mv /stage/${SCHEMA}/${TABLE}/processing/${filenameN}.txt /stage/${SCHEMA}/${TABLE}/archive/${filenameN}.txt 
#		echo "SE MUEVE ARCHIVO COMPRIMIDO A GLOBAL" 
		hadoop fs -mkdir /stage/${SCHEMA}/CL/${nombreArchivo}/${fechaDesde}_${fechaHasta}/ &> /dev/null
		hadoop fs -mv ${TEMPDIR}/part-00000.bz2 /stage/${SCHEMA}/CL/${nombreArchivo}/${fechaDesde}_${fechaHasta}/${filenameN}.bz2	 
		fi
		
hadoop fs -rm -r ${TEMPDIR}/* &> /dev/null
hadoop fs -rm -r ${TEMPDIR}
