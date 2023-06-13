import subprocess

subprocess.run(['python', 'kafka_spark_producer_case.py'])
subprocess.run(['python', 'kafka_spark_producer_region.py'])
subprocess.run(['python', 'kafka_spark_producer_timeprovince.py'])

exit()