# dbtafutil
Utility Package for dbt -> Airflow

Introduction:
=============
This package consists of Airflow utilities that you can use with your dbt project. It has been developed with the aim to enable individuals and teams fast track their development work.

## Utilities:
### 1. Generate Airflow DAGs
This utility can help with generating Airflow DAGs out from dbt project. Further details about this utility are given down [below](#generate-airflow-dag-utility-gendag) in the document

Getting Started:
===
### Installation:
dbtafutil can be installed directly from Github. Please follow the below steps to install the package:

1. Create Python Virtual Environment:
   - Open terminal (mac/VSCode) / powershell (MS Windows). Bash terminal is preferable on Windows
   - Check python version (version >= 3.8.10 is required for installation to work):
        ```
        python --version
        ```
        if you have multiple versions of python installed, then probably you would need to replace "python" with python3 in command above and the next command

   - Create virtual environment:
        ```
        python -m venv venv
        ```
        PS: Above command creates a venv folder in your current working folder. if your current working folder is version controlled in git, make sure to include venv/ in .gitignore file

   - Active virtual enviroment:
        <br>On bash terminal
        ```
        source ./venv/Scripts/activate
        ```
        <br>On windows powershell
        ```
        ./venv/Scripts/activate
        ```
   - Upgrade pip in activated venv
        ```
        pip install --upgrade pip
        ```
   - Install dbtafutil package from git
        ```
        pip install git+https://github.com/nitindt2/dbtafutil.git
        ```
   - 
2. 

Generate Airflow DAG Utility (gendag):
===


