# airflow-utils

Apache Airflow has become the de facto standard for Data Orchestration. However, throughout the years and versions, it accumulated a set of nuances and bugs which can hinder its production usage.

This package aims at creating a centralized source of tools to overcome some of the limitations found in Airflow and to extend some of its features.

# Current Features

|      Feature                                                  |Description                          |
|--------------------------------------------------------------|-------------------------------|
|Correct Task (not DAG) catchup| Provide each Task the ability to catchup / correct previous failures       |
|Retry task combinations / dependencies          | Allow Task combinations to be marked for Retry (for instance, a `Run Task` and the corresponding `Sensor Task`)         |
|Fail task combinations / dependencies          |Allow Task combinations to be marked as Fail (for instance, a `Run Task` and the corresponding `Sensor Task`) |
|Retrieve upstream `TaskInstances`          | Get a set of upstream `TaskInstance`s either by name or by depth. \n Note: Used for task combination / dependency retry and failure  |
|Generate Azure Data Factory triggers          | Create Azure Data Factory Runs with the corresponding dependencies and appropriate Grouped Retries / Failures |

# Supporting Articles and Tutorials

- [Airflow Production Tips — Proper Task (Not DAG) catchup](https://gbanhudo.medium.com/airflow-production-tips-proper-task-not-dag-catchup-c6c8ef1f6ba7)
