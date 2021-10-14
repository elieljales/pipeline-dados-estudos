from airflow.plugins_manager import AirflowPlugin
from operators.g1_operator import G1Operator

class G1AirflowPlugin(AirflowPlugin):
    name = "g1"
    operators = [G1Operator]