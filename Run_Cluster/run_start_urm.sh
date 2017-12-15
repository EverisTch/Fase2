#!/bin/bash
#################################################################################################################################
# NOMBRE PROCESO : run_start_urm.sh                                                                                             #
#################################################################################################################################
# PORVEEDOR       : INDRA                                                                                                       #
# AUTOR           : Felipe Cordova Jovenich                                                                                     #
# FECHA CREACION  : 15-12-2017                                                                                                  #
# DESCRIPCION     : Ejecuta procesos de carga en Hive                                                                           #
# FORMA EJECUCION : Invocación desde Oozie                                                                                 #
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
fi

##########################
# INICIO DE SHELL SCRIPT #
##########################


#RUTA ARCHIVO LOGS
PATH_EXE=/user/$HADOOP_USER_NAME/oozie/urm/
LOG_PATH=$PATH_EXE/logs/

# VERIFICA PARAMETROS DE ENTRADA
[ -z $1 ] && echo -e "SHELL: PARAMETRO 1 FALTANTE"
[ -z $2 ] && echo -e "SHELL: PARAMETRO 2 FALTANTE"

#VARIABLES DE LA SHELL
TABLE=`echo ${1} |tr '[:upper:]' '[:lower:]'`
FREQ=`echo ${2} |tr '[:upper:]' '[:lower:]'`
SCHEMA=urm
DELIMITER=\|
MODE=1

#RUTAS Y DIRECTORIOS
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

#echo `date +"%Y-%m-%d %T"`" ###########################################################"
#echo `date +"%Y-%m-%d %T"`" # Inicio proceso run_processor.sh                         #"
#echo `date +"%Y-%m-%d %T"`" #---------------------------------------------------------#"
#echo `date +"%Y-%m-%d %T"`" # USER_PATH  =  ${USER_PATH}                               "
#echo `date +"%Y-%m-%d %T"`" # LOG_PATH   =  ${LOG_PATH}                                "
#echo `date +"%Y-%m-%d %T"`" # SCHEMA     =  ${SCHEMA}                                  "
#echo `date +"%Y-%m-%d %T"`" # TABLE      =  ${TABLE}                                   "
#echo `date +"%Y-%m-%d %T"`" # FREQ       =  ${FREQ}                                    "
#echo `date +"%Y-%m-%d %T"`" # DELIMITER  =  ${DELIMITER}                               "
#echo `date +"%Y-%m-%d %T"`" # ID PROCESO =  ${ID_PROCC}                                "
#echo `date +"%Y-%m-%d %T"`" ###########################################################"



filein=$(hadoop fs -ls -r /stage/${SCHEMA}/${TABLE}/landing| tail -1 | awk '{print$8}')
#echo `date +"%Y-%m-%d %T"`" # Archivo con Ruta :' ${filein} " 

filename="${filein##*/}"
filenameN="${filename%.*}"
#echo `date +"%Y-%m-%d %T"`" # Nombre Archivo Sin Extension:' ${filenameN}" 
fechas=`echo ${filenameN} | grep -oE '([0-9]{8}T[0-9]{6}_){3}[0-9]{2}'`
nombre=`echo ${filenameN} | grep -Eo '^([a-Z]+_)+'`
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

#echo `date +"%Y-%m-%d %T"`" --------------------------------------"
#echo `date +"%Y-%m-%d %T"`" # Fecha Desde    :' $fechaDesde       "
#echo `date +"%Y-%m-%d %T"`" # Fecha Hasta    :' $fechaHasta       "
#echo `date +"%Y-%m-%d %T"`" # Fecha Ejecucion:' $fechaEjecucion   "
#echo `date +"%Y-%m-%d %T"`" # Numero Archivo :' $numeroArchivo    "
#echo `date +"%Y-%m-%d %T"`" # Nombre Archivo :' $nombreArchivo    "
#echo `date +"%Y-%m-%d %T"`" # Fecha Particion:' $partition_date   "
#echo `date +"%Y-%m-%d %T"`" # Campos Spicy   :' $COLUMNAS         "
#echo `date +"%Y-%m-%d %T"`" --------------------------------------"

#MOVER ARCHIVOS DE PROCESSING QUE PUEDAN HABER QUEDADO REZAGADOS , AL ARCHIVE O AL LANDING PARA PROCESAR
hadoop fs -test -f /stage/${SCHEMA}/CL/${nombreArchivo}/${fechaDesde}_${fechaHasta}/${filenameN}.bz2
	if [ $? -ne 0 ];    then
	hadoop fs -mv /stage/${SCHEMA}/${TABLE}/processing/* /stage/${SCHEMA}/${TABLE}/archive/ &> /dev/null
	else
	hadoop fs -mv /stage/${SCHEMA}/${TABLE}/processing/${filenameN}.txt /stage/${SCHEMA}/${TABLE}/landing/${filenameN}.txt
	exit 1001
	fi
		 
hadoop fs -mv /stage/${SCHEMA}/${TABLE}/processing/* /stage/${SCHEMA}/${TABLE}/archive/ &> /dev/null

#MOVER ARCHIVO A PROCESAR
#echo -e "\nSE MOVERAN LOS SIGUIENTES ARCHIVOS AL DIRECTORIO DE PROCESO\n" 
hadoop fs -mv /stage/${SCHEMA}/${TABLE}/landing/${filenameN}.txt /stage/${SCHEMA}/${TABLE}/processing/${filenameN}.txt
        if [ $? -ne 0 ];    then
            echo -e "\n\nWARNING:  FALLO MIENTRAS SE MOVIA EL ARCHIVO ${filein} AL DIRECTORIO" 
            exit 1002
		else 
		hadoop fs -ls /stage/${SCHEMA}/${TABLE}/processing
        fi

#VARIABLES DE SALIDA
path_archivo=`echo /stage/${SCHEMA}/${TABLE}/processing/`
input_spicy=$path_archivo

echo "FILE_NAME=${filename}"
echo "COLUMNAS=${COLUMNAS}"
echo "INPUT_SPICY=${input_spicy}"
echo "PARTITION_DATE=${partition_date}"
echo "PATH_HQL=${PATH_EXE}generic/${TABLE}.hql"
echo "SCHEMA=${SCHEMA}"
