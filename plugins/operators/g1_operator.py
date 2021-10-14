import json
from datetime import datetime, timedelta
from airflow.models import DAG, BaseOperator, TaskInstance
from airflow.utils.decorators import apply_defaults
from hooks.g1_hook import G1Hook

class G1Operator(BaseOperator):
    
    template_fields = [
        'pages'
    ]
    
    @apply_defaults
    def __init__(
        self,
        pages = 4, 
        conn_id=None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.pages = pages
        self.conn_id = conn_id
    
    def execute(self, context):
            hook = G1Hook(
            pages=self.pages,
            conn_id=self.conn_id,
            )
            for pg in hook.run():
                print(json.dumps(pg, indent=4, sort_keys=True))

if __name__ == "__main__":
    with DAG(dag_id="G1_Crawler") as dag:
        to = G1Operator(
            pages=4,
            task_id="g1_crawler_run",
        )
        ti = TaskInstance(task=to, execution_date=datetime.now() - timedelta(days=1))
        ti.run()
    