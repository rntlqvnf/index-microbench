#!/bin/bash

KEY_TYPE=monoint
for WORKLOAD_TYPE in a b c d e; do
  echo workload${WORKLOAD_TYPE} > workload_config.inp
  echo ${KEY_TYPE} >> workload_config.inp
  for TEST_TYPE in performance coverage; do
    python gen_workload.py workload_config.inp ${TEST_TYPE}
    mv workloads/${TEST_TYPE}_load_${KEY_TYPE}_workload${WORKLOAD_TYPE} workloads/${TEST_TYPE}_mono_inc_load_zipf_int_100M_${WORKLOAD_TYPE}.dat
    mv workloads/${TEST_TYPE}_txn_${KEY_TYPE}_workload${WORKLOAD_TYPE} workloads/${TEST_TYPE}_mono_inc_txns_zipf_int_100M_${WORKLOAD_TYPE}.dat
  done
done

KEY_TYPE=randint
for WORKLOAD_TYPE in a b c d e; do
  echo workload${WORKLOAD_TYPE} > workload_config.inp
  echo ${KEY_TYPE} >> workload_config.inp
  for TEST_TYPE in performance coverage; do
    python gen_workload.py workload_config.inp ${TEST_TYPE}
    mv workloads/${TEST_TYPE}_load_${KEY_TYPE}_workload${WORKLOAD_TYPE} workloads/${TEST_TYPE}_rand_int_load_zipf_int_100M_${WORKLOAD_TYPE}.dat
    mv workloads/${TEST_TYPE}_txn_${KEY_TYPE}_workload${WORKLOAD_TYPE} workloads/${TEST_TYPE}_rand_int_txns_zipf_int_100M_${WORKLOAD_TYPE}.dat
  done
done

KEY_TYPE=email
for WORKLOAD_TYPE in a b c d e; do
  echo workload${WORKLOAD_TYPE} > workload_config.inp
  echo ${KEY_TYPE} >> workload_config.inp
  for TEST_TYPE in performance coverage; do
    python gen_workload.py workload_config.inp ${TEST_TYPE}
    mv workloads/${TEST_TYPE}_load_${KEY_TYPE}_workload${WORKLOAD_TYPE} workloads/${TEST_TYPE}_email_load_zipf_int_100M_${WORKLOAD_TYPE}.dat
    mv workloads/${TEST_TYPE}_txn_${KEY_TYPE}_workload${WORKLOAD_TYPE} workloads/${TEST_TYPE}_email_txns_zipf_int_100M_${WORKLOAD_TYPE}.dat
  done
done