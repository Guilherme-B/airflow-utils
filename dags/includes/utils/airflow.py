from typing import Any, Dict, List, Optional

from sqlalchemy.orm.session import Session

from airflow.models import taskinstance
from airflow.models.dagrun import DagRun
from airflow.utils.db import provide_session
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import State, TaskInstanceState


@provide_session
def get_previous_ti_by_state(task_instance: taskinstance.TaskInstance, task_state: TaskInstanceState, session: Session = NEW_SESSION) -> Optional[taskinstance.TaskInstance]:
    """Gets the latest taskinstance.TaskInstance whose state corresponds to task_state

    Args:
        task_instance (taskinstance.TaskInstance): The target taskinstance.TaskInstance
        task_state (TaskInstanceState): The TaskInstanceState to use as a filter
        session (Session, optional): The DB Session. Defaults to NEW_SESSION.

    Returns:
        Optional[taskinstance.TaskInstance]: The fetched taskinstance.TaskInstance, if any
    """
    # retrieve the latest Task Instance model whose TaskInstanceState corresponds to the specified task instance state
    return session.query(taskinstance.TaskInstance).filter_by(state=task_state, task_id=task_instance.task_id).order_by(taskinstance.TaskInstance.start_date.desc()).first()


@provide_session
def get_previous_ti_success(task_instance: taskinstance.TaskInstance, session: Session = NEW_SESSION) -> Optional[taskinstance.TaskInstance]:
    """Fetches the latest TraskInstance whose state is TaskInstanceState.SUCCESS

    Args:
        task_instance (taskinstance.TaskInstance):  The target taskinstance.TaskInstance
        session (Session, optional): The DB Session. Defaults to NEW_SESSION.

    Returns:
        Optional[taskinstance.TaskInstance]: The fetched taskinstance.TaskInstance, if any
    """
    return get_previous_ti_by_state(task_instance=task_instance, task_state=TaskInstanceState.SUCCESS, session=session)


@provide_session
def get_previous_ti_dagrun_by_state(task_instance: taskinstance.TaskInstance, task_state: TaskInstanceState, session: Session = NEW_SESSION) -> Optional[DagRun]:
    """Gets the DAGRun associated with the latest taskinstance.TaskInstance whose state corresponds to task_state

    Args:
        task_instance (taskinstance.TaskInstance): The target taskinstance.TaskInstance
        task_state (TaskInstanceState): The TaskInstanceState to use as a filter
        session (Session, optional): The DB Session. Defaults to NEW_SESSION.

    Returns:
        Optional[DagRun]: The retrieved DAGRun instance, if any
    """

    # the previously ran task according to the specified state
    prev_task_instance: taskinstance.TaskInstance = get_previous_ti_by_state(
        task_instance=task_instance, task_state=task_state, session=session)

    if prev_task_instance:
        # the DAG Run instance associated with the previously ran task instance according to the specified state
        dag_run: DagRun = session.query(DagRun).filter(
            DagRun.dag_id == prev_task_instance.dag_id, DagRun.run_id == prev_task_instance.run_id).one()

        return dag_run

    return None


@provide_session
def get_previous_ti_sucess_dagrun(task_instance: taskinstance.TaskInstance, session: Session = NEW_SESSION) -> Optional[DagRun]:
    """Gets the DAGRun associated with the latest taskinstance.TaskInstance whose state is TaskInstanceState.SUCCESS

    Args:
        task_instance (taskinstance.TaskInstance): The target taskinstance.TaskInstance
        session (Session, optional): The DB Session. Defaults to NEW_SESSION.

    Returns:
        Optional[DagRun]: The retrieved DAGRun instance, if any
    """

    return get_previous_ti_dagrun_by_state(task_instance=task_instance, task_state=TaskInstanceState.SUCCESS, session=session)


def _get_upstream_task_instance_list(
    context: Dict[str, Any],
    param_name_upstream_task_list: str,
    param_name_upstream_task_depth: str,
) -> List[taskinstance.TaskInstance]:
    """Gets a list of upstream tasks based on the provided context and task search definition

    Args:
        context (Dict[str, Any]): The DAGRun context
        param_name_upstream_task_list (str): The name of the context params key containing
            the list of upstream task names to track
        param_name_upstream_task_depth (str): The name of the context params key containing
            the depth of upstream task names to track

    Raises:
        ValueError: If arguments {param_name_upstream_task_list} and {param_name_upstream_task_depth} have
            not been passed
        ValueError: If argument {param_name_upstream_task_depth} is used but a non-linear dependency
            was detected

    Returns:
        List[taskinstance.TaskInstance]: The list of upstream taskinstance.TaskInstances
    """
    tasks: List[taskinstance.TaskInstance] = context["dag_run"].get_task_instances()
    context_dag: DagRun = context["dag"]

    # the list of upstream task names
    task_list: List[str] = context["params"].get(
        param_name_upstream_task_list, [])

    # the depth of upstream tasks
    upstream_task_upstream_depth: int = context["params"].get(
        param_name_upstream_task_depth)

    if not task_list and not upstream_task_upstream_depth:
        raise ValueError(
            "At least one of the arguments {task_list} or {task_upstream_retry_depth} must be set")

    if upstream_task_upstream_depth and upstream_task_upstream_depth <= 0:
        raise ValueError(
            "The specified parameter {upstream_task_upstream_depth}'s value must be a positive integer")

    upstream_task_instances: List[taskinstance.TaskInstance] = []

    if task_list:
        upstream_named_task_instances = [
            ti for ti in task_list if ti.task_id in tasks]

        upstream_task_instances.extend(upstream_named_task_instances)

    # retrieve all upstream tasks for depth {upstream_task_upstream_depth}
    # note: only suitable for linear dependencies
    if upstream_task_upstream_depth:

        upstream_task_ids: List[str] = []
        current_iteration: int = 0
        current_task: taskinstance.TaskInstance = context['task']

        # iterated through each depth level
        while current_iteration < upstream_task_upstream_depth:

            #num_upstream_tasks: int = len(current_task.upstream_task_ids)
            num_upstream_tasks: int = len(
                current_task.get_direct_relative_ids(upstream=True))

            if num_upstream_tasks != 1:
                raise ValueError(
                    f'The number of upstream tasks of "{current_task}" must be 1, but is {num_upstream_tasks} instead. Only linear dependencies are supported.')

            upstream_task_id: List[str] = list(
                current_task.get_direct_relative_ids(upstream=True))[0]
            upstream_task_ids.append(upstream_task_id)
            upstream_task = [
                t for t in context_dag.tasks if t.task_id == upstream_task_id][0]

            current_task = upstream_task
            current_iteration += 1

        mapped_task_instances: Dict[int, taskinstance.TaskInstance] = {
            t_ins.task_id: t_ins for t_ins in context['dag_run'].get_task_instances()}

        depth_task_instances_to_retry: List[taskinstance.TaskInstance] = [
            mapped_task_instances[tid] for tid in upstream_task_ids[::-1]]

        upstream_task_instances.extend(depth_task_instances_to_retry)

    if upstream_task_instances:
        # remove duplicates
        upstream_task_instances = list(set(upstream_task_instances))

        return upstream_task_instances

    return []


@provide_session
def mark_upstream_tasks_retry(context: Dict[str, Any], session: Session = NEW_SESSION) -> None:
    """Marks the upstream tasks to be retried.

    Args:
        context (Dict[str, Any]): The DAGRun context
        session (Session, optional): The Session object. Defaults to NEW_SESSION.
    """
    task_instances_to_retry: List[taskinstance.TaskInstance] = _get_upstream_task_instance_list(
        context=context,
        param_name_upstream_task_list="task_upstream_retry_list",
        param_name_upstream_task_depth="task_upstream_retry_depth"
    )

    if task_instances_to_retry:
        context_dag = context["dag"]

        taskinstance.clear_task_instances(
            tis=task_instances_to_retry,
            session=session,
            dag=context_dag)


@provide_session
def mark_upstream_tasks_failed(context: Dict[str, Any], session: Session = NEW_SESSION) -> None:
    """Marks the upstream tasks as failed.

    Args:
        context (Dict[str, Any]): The DAGRun context
        session (Session, optional): The Session object. Defaults to NEW_SESSION.
    """
    task_instances_to_mark_failed: List[taskinstance.TaskInstance] = _get_upstream_task_instance_list(
        context=context,
        param_name_upstream_task_list="task_upstream_fail_list",
        param_name_upstream_task_depth="task_upstream_fail_depth"
    )

    if task_instances_to_mark_failed:
        for task in task_instances_to_mark_failed:
            task.state = State.FAILED
            session.merge(task)
