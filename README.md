# PythonProjects
All my python projects 

Module Information:

| Module | Info | external dependencies |
| --- | --- | --- |
| MsSqldbUtility.py | wrapper to connect to azure sql db or mssql server | require odbc drivers to be installed |
| SqoopUtility.py | wrapper to sqoop data from rdbms to hive | |
| HiveDbStats.py | Wrapper to fetch hdfs size and record count of hive tables | |


Misic info:

    For MAC : 
    
        brew unlink freetds
        
        brew install freetds@0.91 
        
        brew link --force freetds@0.91
        
        brew install unixodbc


