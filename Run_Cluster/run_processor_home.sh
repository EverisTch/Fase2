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

klist -s
if [[ "$?" == "1" ]]; then
# kinit -kt /user/$HADOOP_USER_NAME/$HADOOP_USER_NAME.keytab $HADOOP_USER_NAME@BIGDATA.TCHILE.LOCAL  
kinit -kt /home/TCHILE.LOCAL/$HADOOP_USER_NAME/.ssh/$HADOOP_USER_NAME.keytab $HADOOP_USER_NAME@BIGDATA.TCHILE.LOCAL
fi

##########################
# INICIO DE SHELL SCRIPT #
##########################


#RUTA ARCHIVO LOGS
HADOOP_USER_NAME=srv_ingesta
PATH_EXE=/home/TCHILE.LOCAL/$HADOOP_USER_NAME/oozie/urm/
LOG_PATH=$PATH_EXE/logs/

#VALIDACION DIRECTORIO LOGS
[ -w ${LOG_PATH} ] && VRS_FILE_LOG="${LOG_PATH}${1}_${2}_`date +"%Y-%m-%d_%H-%M-%S"`.log"     # Archivo Log
[ $? -ne 0 ]  && echo -e "\n######################################\n# DIRECTORIO LOG ${LOG_PATH} SIN PERMISO DE ESCRITURA #\n######################################" && exit 777

# VERIFICA PARAMETROS DE ENTRADA
[ -z $1 ] && echo -e "SHELL: PARAMETRO 1 FALTANTE" >> ${VRS_FILE_LOG} && echo -e "\nCODIGO DE ERROR [101]" >> ${VRS_FILE_LOG} && cat ${VRS_FILE_LOG} && exit 101
[ -z $2 ] && echo -e "SHELL: PARAMETRO 2 FALTANTE" >> ${VRS_FILE_LOG} && echo -e "\nCODIGO DE ERROR [101]" >> ${VRS_FILE_LOG} && cat ${VRS_FILE_LOG} && exit 101

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
mkdir -p $TEMPDIR
USER_PATH=/user/$HADOOP_USER_NAME/stage/$SCHEMA

# PARAMETRO DE PID
ID_PROCC=$$

#CAMPOS PARA ANONIMIZAR
case $TABLE in
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

echo `date +"%Y-%m-%d %T"`" ###########################################################" > ${VRS_FILE_LOG}
echo `date +"%Y-%m-%d %T"`" # Inicio proceso run_processor.sh                         #" >> ${VRS_FILE_LOG}
echo `date +"%Y-%m-%d %T"`" #---------------------------------------------------------#" >> ${VRS_FILE_LOG}
echo `date +"%Y-%m-%d %T"`" # USER_PATH  =  ${USER_PATH}                               " >> ${VRS_FILE_LOG}
echo `date +"%Y-%m-%d %T"`" # LOG_PATH   =  ${LOG_PATH}                                " >> ${VRS_FILE_LOG}
echo `date +"%Y-%m-%d %T"`" # TEMPDIR    =  ${TEMPDIR}                                 " >> ${VRS_FILE_LOG}
echo `date +"%Y-%m-%d %T"`" # SCHEMA     =  ${SCHEMA}                                  " >> ${VRS_FILE_LOG}
echo `date +"%Y-%m-%d %T"`" # TABLE      =  ${TABLE}                                   " >> ${VRS_FILE_LOG}
echo `date +"%Y-%m-%d %T"`" # FREQ       =  ${FREQ}                                    " >> ${VRS_FILE_LOG}
echo `date +"%Y-%m-%d %T"`" # DELIMITER  =  ${DELIMITER}                               " >> ${VRS_FILE_LOG}
echo `date +"%Y-%m-%d %T"`" # ID PROCESO =  ${ID_PROCC}                                " >> ${VRS_FILE_LOG}
echo `date +"%Y-%m-%d %T"`" ###########################################################" >> ${VRS_FILE_LOG}



filein=$(hadoop fs -ls -r /stage/$SCHEMA/$TABLE/landing| tail -1 | awk '{print$8}')
echo `date +"%Y-%m-%d %T"`" # Archivo con Ruta :' ${filein} "  >> ${VRS_FILE_LOG}

filename="${filein##*/}"
filenameN="${filename%.*}"
echo `date +"%Y-%m-%d %T"`" # Nombre Archivo Sin Extension:' ${filenameN}"  >> ${VRS_FILE_LOG}
#fechas=`echo $filenameN | grep -oE [0-9T_]+$`
fechas=`echo $filenameN | grep -oE '([0-9]{8}T[0-9]{6}_){3}[0-9]{2}'`
nombre=`echo $filenameN | grep -Eo '^([a-Z]+_)+'`
nombreArchivo="${nombre%*_}"
fechaDesde=`echo $fechas | awk -F "_" '{print$1}'`
fechaHasta=`echo $fechas | awk -F "_" '{print$2}'`
fechaEjecucion=`echo $fechas | awk -F "_" '{print$3}'`
numeroArchivo=`echo $fechas | awk -F "_" '{print$4}'`

#VALIDACION FRECUENCIA PARA PARTICION
if [[ "$FREQ" == "mensual" ]]; 
	then
	partition_date=`echo $fechaDesde | cut -c1-6`
	else
	partition_date=`echo $fechaDesde | cut -c1-8`
fi

echo `date +"%Y-%m-%d %T"`" --------------------------------------" >> ${VRS_FILE_LOG}
echo `date +"%Y-%m-%d %T"`" # Fecha Desde    :' $fechaDesde       " >> ${VRS_FILE_LOG}
echo `date +"%Y-%m-%d %T"`" # Fecha Hasta    :' $fechaHasta       " >> ${VRS_FILE_LOG}
echo `date +"%Y-%m-%d %T"`" # Fecha Ejecucion:' $fechaEjecucion   " >> ${VRS_FILE_LOG}
echo `date +"%Y-%m-%d %T"`" # Numero Archivo :' $numeroArchivo    " >> ${VRS_FILE_LOG}
echo `date +"%Y-%m-%d %T"`" # Nombre Archivo :' $nombreArchivo    " >> ${VRS_FILE_LOG}
echo `date +"%Y-%m-%d %T"`" # Fecha Particion:' $partition_date   " >> ${VRS_FILE_LOG}
echo `date +"%Y-%m-%d %T"`" # Campos Spicy   :' $COLUMNAS         " >> ${VRS_FILE_LOG}
echo `date +"%Y-%m-%d %T"`" --------------------------------------" >> ${VRS_FILE_LOG}

#MOVER ARCHIVOS DE PROCESSING QUE PUEDAN HABER QUEDADO REZAGADOS , AL ARCHIVE
hadoop fs -mv /stage/$SCHEMA/$TABLE/processing/* /stage/$SCHEMA/$TABLE/archive/ &> /dev/null

#MOVER ARCHIVO A PROCESAR
echo -e "\nSE MOVERAN LOS SIGUIENTES ARCHIVOS AL DIRECTORIO DE PROCESO\n"  >> ${VRS_FILE_LOG}
hadoop fs -mv /stage/$SCHEMA/$TABLE/landing/$filenameN.txt /stage/$SCHEMA/$TABLE/processing/$filenameN.txt
        if [ $? -ne 0 ];    then
            echo -e "\n\nWARNING:  FALLO MIENTRAS SE TRANFERIA EL ARCHIVO ${filein} AL DIRECTORIO"  >> ${VRS_FILE_LOG}
            exit 1
		else 
		hadoop fs -ls /stage/$SCHEMA/$TABLE/processing
        fi



#EJECUCION DE HIVE  

echo "beeline -u jdbc:hive2://prd7404.bigdata.tchile.local:2181,prd7432.bigdata.tchile.local:2181,prd7460.bigdata.tchile.local:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2 \
        -f ${PATH_EXE}${TABLE}/${TABLE}.hql --hivevar SCHEMA=${SCHEMA} --hivevar USER_PATH='/stage/${SCHEMA}/${TABLE}/processing/' --hivevar fecha=${partition_date}"  >> ${VRS_FILE_LOG}
		
path_archivo=`echo /stage/${SCHEMA}/${TABLE}/processing/`
input_spicy=$path_archivo

(beeline -u "jdbc:hive2://prd7404.bigdata.tchile.local:2181,prd7432.bigdata.tchile.local:2181,prd7460.bigdata.tchile.local:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" \
           -f ${PATH_EXE}${TABLE}/${TABLE}.hql --hivevar SCHEMA=${SCHEMA} --hivevar USER_PATH=${path_archivo} --hivevar fecha=${partition_date}) >> ${VRS_FILE_LOG}

        if [ $? -ne 0 ];    then
            echo -e "\n\nWARNING:  FALLO LA CARGA EN HIVE PARA EL ARCHIVO ${filein} EN LA TABLA ${SCHEMA}.${TABLE}"  >> ${VRS_FILE_LOG}
            hadoop fs -mv /stage/$SCHEMA/$TABLE/processing/$filenameN.txt /stage/$SCHEMA/$TABLE/landing/$filenameN.txt
            exit 1
        fi

#EJECUCION SI EL ARCHIVO TIENE ENRRIQUECIDO
if [ ${TABLE} == "mobile_line" ] || [ ${TABLE} == "call_cdr" ] || [ ${TABLE} == "data_xdr" ] ; then

echo `date +"%Y-%m-%d %T"`"Archivo con Ejecucion de Extraccion " 

hadoop fs -rm -r -skipTrash /stage/${SCHEMA}/${TABLE}/output/*	>> ${VRS_FILE_LOG}
	
path_archivo_out=`echo /stage/${SCHEMA}/${TABLE}/output/`
input_spicy=$path_archivo_out

(beeline -u "jdbc:hive2://prd7404.bigdata.tchile.local:2181,prd7432.bigdata.tchile.local:2181,prd7460.bigdata.tchile.local:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" \
            -f ${PATH_EXE}${TABLE}/out_${TABLE}.hql --hivevar SCHEMA=${SCHEMA} --hivevar USER_PATH=${path_archivo_out} --hivevar fecha=${partition_date})  >> ${VRS_FILE_LOG}

			if [ $? -ne 0 ];    then
			 echo -e "\n\nWARNING:  FALLO LA EJECUCION DE SPICY Y COMPRESION A BZ2 DE ARCHIVO EXTRACCION"  >> ${VRS_FILE_LOG}
			
			exit 1
			fi

hadoop fs -text ${path_archivo_out}*.deflate | hadoop fs -put - ${path_archivo_out}${filenameN}.txt >> ${VRS_FILE_LOG}

fi		

#EJECUCION SPICY Y BZ2

hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar -Dmapreduce.job.queue.name=hdpappHIGH -Dmapred.output.compress=true \
           -Dmapred.compress.map.output=true -Dmapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec -Dmapred.reduce.tasks=1 \
           -input ${input_spicy}${filenameN}.txt -output ${TEMPDIR}/ \
           -mapper "/usr/bin/spicy -columns=${COLUMNAS} -salt="yMo2r7fp5L5RiUah/O65MR30tkfL43b4eDDc2tFs0MM=" -sep="'|'" -skip=false"  >> ${VRS_FILE_LOG}
      
        if [ $? -ne 0 ];    then
            echo -e "\n\nWARNING:  FALLO LA EJECUCION DE SPICY Y COMPRESION A BZ2"  >> ${VRS_FILE_LOG}

            exit 1
		else
		echo "ANONIMIZADO Y COMPRIMIDO OK , SE MUEVE ARCHIVO ORIGEN A ARCHIVO" 
		hadoop fs -mv /stage/${SCHEMA}/${TABLE}/processing/${filenameN}.txt /stage/${SCHEMA}/${TABLE}/archive/${filenameN}.txt  >> ${VRS_FILE_LOG}
		echo "SE MUEVE ARCHIVO COMPRIMIDO A GLOBAL"  >> ${VRS_FILE_LOG}
		hadoop fs -mkdir /stage/${SCHEMA}/CL/${nombreArchivo}/${fechaDesde}_${fechaHasta}/ &> /dev/null
		hadoop fs -mv ${TEMPDIR}/part-00000.bz2 /stage/${SCHEMA}/CL/${nombreArchivo}/${fechaDesde}_${fechaHasta}/${filenameN}.bz2	 
		fi
		
rm -r $TEMPDIR/* &> /dev/null >> ${VRS_FILE_LOG}
rm -r $TEMPDIR




