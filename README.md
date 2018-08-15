# PythonProjects
All my python projects 


Requirements :

| Module | External dependencies |
| --- | --- |
| MsSqldbUtility.py | Needs odbc driver to work with pyodbc |





Misic info:

For MAC : 

brew unlink freetds
brew install freetds@0.91
brew link --force freetds@0.91
brew install unixodbc


Module Information:

| Module | Info |
| --- | --- |
| MsSqldbUtility.py | wrapper to connect to azure sql db or mssql server |
| SqoopUtility.py | wrapper to sqoop data from rdbms to hive |
