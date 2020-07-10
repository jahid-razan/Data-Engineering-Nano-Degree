from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Defining the operators params (with defaults)
                 redshift_conn_id='',
                 dq_checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Mapping the params
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        # self.log.info('DataQualityOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        count_errors = 0
        failed_tests = []
        self.log.info('Data Quality checks is taking place')
        for check in self.dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
            records = redshift.get_records(sql)[0]
            if exp_result != records[0]:
                count_errors += 1
                failed_tests.append(sql)
        if count_errors > 0:
            self.log.info('Tests failed')
            self.log.info(failed_tests)
            raise ValueError('Data quality check has failed')
        self.log.info('Data Quality check passed!')
        