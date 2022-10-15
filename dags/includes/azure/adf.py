import logging

from datetime import datetime, timedelta
from typing import Any, Dict, Tuple

# airflow utilities
from airflow.utils.dates import days_ago
from airflow.utils.edgemodifier import Label

# airflow operators
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator

# azure provider
from airflow.providers.microsoft.azure.hooks.azure_data_factory import AzureDataFactoryHook
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.providers.microsoft.azure.sensors.data_factory import AzureDataFactoryPipelineRunStatusSensor
from includes.utils.airflow import mark_upstream_tasks_failed, mark_upstream_tasks_retry


def get_latest_pipeline_run_status(
    conn_id: str,
    pipeline_name: str,
    factory_name: str,
    resource_group_name: str,
) -> str:
    """
    Retrieves the status of the latest pipeline run for a given pipeline.

    :param conn_id: The Connection ID to use for connecting to Azure Data Factory.
    :type conn_id: str
    :param pipeline_name: The name of the pipeline to execute.
    :type pipeline_name: str
    :param factory_name: The data factory name.
    :type factory_name: str
    :param resource_group_name: The resource group name.
    :type resource_group_name: str
    """

    from azure.mgmt.datafactory.models import (
        RunQueryFilter,
        RunFilterParameters,
        RunQueryOrder,
        RunQueryOrderBy,
        RunQueryFilterOperand,
        RunQueryFilterOperator,
        RunQueryOrderByField,
    )

    logging.info(
        f"get_latest_pipeline_run_status() running for conn id {conn_id} pipeline_name {pipeline_name} name {factory_name} resource group name {resource_group_name}")

    # Create a ``RunQueryFilter`` object checking for a pipeline with the provided name.
    run_filter = RunQueryFilter(
        operand=RunQueryFilterOperand.PIPELINE_NAME,
        operator=RunQueryFilterOperator.EQUALS,
        values=[pipeline_name],
    )
    # Create a ``RunQueryOrderBy`` object to ensure pipeline runs are ordered by descending run-start time.
    run_order = RunQueryOrderBy(
        order_by=RunQueryOrderByField.RUN_START, order=RunQueryOrder.DESC)
    # Create a ``RunFilterParameters`` object to check pipeline runs within the past 7 days.
    filter_params = RunFilterParameters(
        last_updated_before=datetime.utcnow(),
        last_updated_after=days_ago(7),
        filters=[run_filter],
        order_by=[run_order],
    )

    # Retrieve runs for the given pipeline within a given factory.
    logging.info(
        f"Checking for the latest status for the {pipeline_name} pipeline.")

    hook = AzureDataFactoryHook(azure_data_factory_conn_id=conn_id)
    query_response = hook.get_conn().pipeline_runs.query_by_factory(
        resource_group_name=resource_group_name, factory_name=factory_name, filter_parameters=filter_params
    )

    # Check if pipeline runs were found within the filter and date/time parameters.
    if query_response.value:
        pipeline_status = query_response.value[0].status
        logging.info(
            f"Found the latest pipeline run for {pipeline_name} pipeline within factory {factory_name} has a "
            f"status of {pipeline_status}."
        )

        return pipeline_status
    else:
        # It is possible a pipeline exists but has never run before or within the time window. As long as the
        # pipeline exists, a "good" status should still be returned. Otherwise, return a status that signifies
        # the pipeline doesn't exist.
        logging.info(
            f"The pipeline {pipeline_name} does exists but no runs have executed within the specified time "
            "window. Checking if pipeline exists."
        )
        pipeline = hook._pipeline_exists(
            pipeline_name=pipeline_name,
            resource_group_name=resource_group_name,
            factory_name=factory_name,
        )
        if pipeline:
            logging.info(f"A pipeline named {pipeline_name} does exist.")

            return "NoRunInTimeWindow"
    return "DoesNotExist"


def generate_adf_task(
    adf_conn_id: str,
    pipeline_name: str,
    factory_name: str,
    resource_group_name: str,
    pipeline_parameters: Dict[str, Any] = {},
    is_allow_concurrent: bool = True,
    override_base_name: str = None
) -> Tuple[PythonOperator, AzureDataFactoryPipelineRunStatusSensor]:
    """Generates a set of Azure Data Factory initialization, monitoring and logging tasks

    Args:
        adf_conn_id (str): The connection used to trigger and monitor the Azure Data Factory instance
        pipeline_name (str): The name of the pipeline to run
        factory_name (str): The name of the factory to run
        resource_group_name (str): The resource group containing the Azure Data Factory instance
        pipeline_parameters (Dict[str, Any], optional): A list of parameters to send to the created instance. Defaults to {}.
        is_allow_concurrent (bool, optional): Should multiple instances be allowed to run. Defaults to True.
        override_base_name (str, optional): A string to be used to name the generated tasks. Defaults to None.

    Returns:
        Tuple[PythonOperator, AzureDataFactoryPipelineRunStatusSensor]: The generated pipeline start and end operators
    """

    logging.info(
        f"utils.adf::generate_adf_task starting with parameters: {pipeline_parameters}")

    task_base_name: str = override_base_name if override_base_name else pipeline_name

    if is_allow_concurrent:
        run_extract_pipeline = AzureDataFactoryRunPipelineOperator(
            task_id=f"run_{task_base_name}_pipeline",
            azure_data_factory_conn_id=adf_conn_id,
            factory_name=factory_name,
            pipeline_name=pipeline_name,
            wait_for_termination=False,
            retry_delay=timedelta(minutes=5),
            retries=3,
            sla=timedelta(minutes=30),
            parameters=pipeline_parameters
        )

        wait_for_extract_pipeline_run = AzureDataFactoryPipelineRunStatusSensor(
            task_id=f"wait_for_{task_base_name}_run",
            azure_data_factory_conn_id=adf_conn_id,
            factory_name=factory_name,
            run_id=run_extract_pipeline.output["run_id"],
            poke_interval=10,
            on_failure_callback=mark_upstream_tasks_failed,
            on_retry_callback=mark_upstream_tasks_retry,
            params={"task_upstream_retry_depth": 1,
                    "task_upstream_fail_depth": 1}
        )

        run_extract_pipeline >> wait_for_extract_pipeline_run

        return run_extract_pipeline, wait_for_extract_pipeline_run
    else:
        get_latest_extract_pipeline_run_status = PythonOperator(
            task_id=f'get_latest_{task_base_name}_run_status',
            provide_context=True,
            python_callable=get_latest_pipeline_run_status,
            retry_delay=timedelta(minutes=5),
            op_kwargs={
                "pipeline_name": pipeline_name,
                "conn_id": adf_conn_id,
                "factory_name": factory_name,
                "resource_group_name": resource_group_name
            },
        )

        is_extract_pipeline_running = ShortCircuitOperator(
            task_id=f"is_{task_base_name}_running",
            python_callable=lambda x: x not in [
                "InProgress", "Queued", "Canceling"],
            op_args=[get_latest_extract_pipeline_run_status],
        )

        run_extract_pipeline = AzureDataFactoryRunPipelineOperator(
            task_id=f"run_{task_base_name}_pipeline",
            azure_data_factory_conn_id=adf_conn_id,
            factory_name=factory_name,
            pipeline_name=pipeline_name,
            wait_for_termination=False,
            retry_delay=timedelta(minutes=5),
            retries=3,
            sla=timedelta(minutes=30),
            parameters=pipeline_parameters,
        )

        wait_for_extract_pipeline_run = AzureDataFactoryPipelineRunStatusSensor(
            task_id=f"wait_for_{task_base_name}_run",
            azure_data_factory_conn_id=adf_conn_id,
            factory_name=factory_name,
            run_id=run_extract_pipeline.output["run_id"],
            poke_interval=10,
            on_failure_callback=mark_upstream_tasks_failed,
            on_retry_callback=mark_upstream_tasks_retry,
            params={"task_upstream_retry_depth": 1,
                    "task_upstream_fail_depth": 1}
        )

        get_latest_extract_pipeline_run_status >> is_extract_pipeline_running >> Label(
            "ADF is not Running") >> run_extract_pipeline >> wait_for_extract_pipeline_run

        return get_latest_extract_pipeline_run_status, wait_for_extract_pipeline_run
