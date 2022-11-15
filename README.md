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

1. Python Virtual Environment:
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
2. Install dbtafutil package from git
   - pip install command
        ```
        pip install -e git+https://github.com/nitindt2/dbtafutil.git#egg=dbtafutil
        ```
   - Check that dbtafutil is installed successfully
        ```
        dbtafutil --version
        ```

if above command displays version information, than that indicates that package has been installed correctly.

You can now execute the command you need to run or run command "dbtafutil --help" to check all the options available.

Generate Airflow DAG Utility (gendag):
=============
You can generate airflow dag files with this utility, based on your dbt project. To run gendag utility following command can be used
```
dbtafutil gendag 
```

To check all arguments that can be used with the above command, run
```
dbtafutil gendag --help
```

## Optional arguments available with gendag:
1. Model Selector:
    ```
    -m or --models or -s or --select
    ```
    Generation of dags can be based on model names with the above argument. Either single model name can ben passed to this argument e.g. "dbtafutil gendag -m modelA" or multiple model names can be passed e.g. "dbtafutil -m modelA modelB".
    With the model name "+" can be included on either side. Including this sign on the left would instruct to pick all downstream models in dag and on right would pick upstream models. If included on both sides, it would pick both downstream and upstream models
2. Tag Selector:
    ```
    -t or --tag
    ```
    Generation of dags can also be based on tag names that are added to dbt models. Either single tag name can ben passed to this argument e.g. "dbtafutil gendag -t tagA" or multiple tag names can be passed e.g. "dbtafutil -t tagA tagB".
    With tag options, all the models associated to that tag are selected and then depending on refs, dag tasks order of execution is identified

    Please note: either of "models" or "tag" selector is required. If none of these are passed in the command, it would raise error

3. Skip Tests flag
    ```
    -st or --skip-tests
    ```
    By default tests associated to the selected models are also included in generated dags. If tests are not to be included in generated dags, then this flag can be used.


