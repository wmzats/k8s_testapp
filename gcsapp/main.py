import os
import sys
import subprocess

logs = subprocess.check_output("python /opt/spark/examples/src/main/python/dbBasic.py", 
                                shell=True).decode('utf8')
print(logs)