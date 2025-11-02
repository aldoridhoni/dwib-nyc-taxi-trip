from typing import Optional, List
from airflow.providers.standard.operators.bash import BashOperator
import shlex


class UvOperator(BashOperator):
    """
    Custom operator to run Python scripts using uv.
    This operator uses uv run directly, which handles virtualenvs and dependencies automatically.

    The script should have its dependencies defined inline using uv's script metadata format.

    :param script_path: Path to the Python script to execute
    :param script_args: Optional list of arguments to pass to the script

    Example usage:

        from operators.uv_operator import UvOperator

        # Your Python script (my_script.py) should have dependencies at the top:
        # /// script
        # dependencies = ["requests", "pandas==2.2.0"]
        # ///

        task = UvOperator(
            task_id='run_analysis',
            script_path='/path/to/my_script.py',
            script_args=['--input', 'data.csv', '--output', 'results.json'],
            dag=dag
        )
    """

    def __init__(
        self,
        script_path: str,
        script_args: Optional[List[str]] = None,
        **kwargs
    ):
        self.script_path = script_path
        self.script_args = script_args or []

        # Build the bash command
        cmd_parts = ['uv', 'run', script_path]
        cmd_parts.extend(self.script_args)

        # Properly quote arguments for bash
        bash_command = ' '.join(shlex.quote(part) for part in cmd_parts)

        kwargs['bash_command'] = bash_command

        super().__init__(**kwargs)

