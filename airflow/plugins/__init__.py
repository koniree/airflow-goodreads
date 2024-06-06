from airflow.plugins_manager import AirflowPlugin

import operators

# Defining the plugin class
class GoodReadsPlugin(AirflowPlugin):
    name = "goodreads_plugin"
    operators = [
        operators.DataQualityOperator,
        operators.LoadAnalyticsOperator
    ]

