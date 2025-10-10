from airflow.sdk import BaseOperator
from airflow.utils.state import State


class XComExecStatusOperator(BaseOperator):

    def __init__(
        self,
        *,
        failed: bool,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.failed = failed

    def execute(self, context):
        ti = context["ti"]  # type: ignore

        status = State.FAILED if self.failed else State.SUCCESS
        ti.xcom_push(key="exec_status", value=status)

        print(f"exec_status pushed to XCom: {status}")

        return None
