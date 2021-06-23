#!/bin/sh

if [ $# -lt 2 ]
then
    echo "usage: $0 [config_file_path] [pod_name_suffix]"
    echo "Example："
    echo "./main.sh ./framework/ucmifs-test/source/configs/cn_ddl_ppc.cfg 01"
    exit 1
fi

CONFIG_FILE_WITH_FULL_PATH=$1
POD_NAME_PREFIX=$2

PY_PATH="local:\/\/\/opt\/spark\/examples\/src\/main\/python\/ucmifs-test\/source\/ddl\/"
HQL_WRAPPER="local:\/\/\/opt\/spark\/examples\/src\/main\/python\/app\/hql_wrapper.py"
HQL_PATH="\/opt\/spark\/examples\/src\/main\/python\/ucmifs-test\/source\/ddl\/"
YAML_TEMPLATE="/home/infra-build/k8s_onegoapp/proteus.yaml"
YAML_PATH="/home/infra-build/k8s_onegoapp/"

gcloud auth activate-service-account aml-innov-bpid-810834@appspot.gserviceaccount.com --key-file=/home/infra-build/aml-innov.json;

#echo $CONFIG_FILE_WITH_FULL_PATH
#echo $POD_NAME_PREFIX
a=0

while read line
do

	if [ "$a" -lt 1 ]
	then
	  let a++
	  continue
	fi

	ACTIVE=`echo $line | awk -F"|" '{print $7}'`
    #if [ "$a" -gt 0 ] && [ "$ACTIVE" = "Y" ]
	#echo $line

	JOB_NAME=`echo $line | awk -F"|" '{print $1}' | tr '[A-Z]' '[a-z]' | sed 's/_//g'`
	PARENT_JOB=`echo $line | awk -F"|" '{print $2}' | tr '[A-Z]' '[a-z]' | sed 's/_//g'`
	IFS=',' read -r -a array <<< "$PARENT_JOB"
	JOB_TYPE=`echo $line | awk -F"|" '{print $3}'`
	JOB_FILE=`echo $line | awk -F"|" '{print $4}'`

	#echo $PARENT_JOB
	LEN=${#array[@]}
	#echo $LEN

	#check dependency jobs are completed or not.
	while ( [ "$PARENT_JOB" ] && [ $LEN -gt 0 ] )
	do
		sleep 5
		NUM=$LEN
		for element in "${array[@]}"
		do
			CNT=`kubectl get pod "$element""$POD_NAME_PREFIX-driver" | grep "Completed" | wc -l`
			if [ $CNT -gt 0 ]
			then
				let NUM--
			fi
		done
		if [ $NUM -eq 0 ]
		then
			break
		fi
		echo "waiting the dependence job to be completed..."
	done

	sed "s/<POD_NAME>/"$JOB_NAME""$POD_NAME_PREFIX"/g" "$YAML_TEMPLATE" > "$YAML_PATH"temp.yaml

	if [ $JOB_TYPE = "PY" ]
	then
		sed -i "s/<APPLICATION_FILE>/$PY_PATH$JOB_FILE/g" "$YAML_PATH"temp.yaml
		sed -i "s/<ARGUMENT>//g" "$YAML_PATH"temp.yaml
	elif [ $JOB_TYPE = "HQL" ]
	then
        sed -i "s/<APPLICATION_FILE>/$HQL_WRAPPER/g" "$YAML_PATH"temp.yaml
		sed -i "s/<ARGUMENT>/$HQL_PATH$JOB_FILE/g" "$YAML_PATH"temp.yaml
	else
		echo "not support job type: $JOB_TYPE"
	fi

	echo "trigger: [kubectl apply -f "$YAML_PATH"temp.yaml]"
	kubectl apply -f "$YAML_PATH"temp.yaml

    let a++
done < $CONFIG_FILE_WITH_FULL_PATH
