import src.core.engine.pipeline_control as engine


class SQLStatementNode(engine.Node):
    """
    config
    ------
        <SQL-statement-node-name>:
            type: datasets.pipelines.SQLStatementNode
            processes:
                <process-name>:
                    priority: <priority-number> (optional)
                    parameters: [<parameter-key>, ...] (optional)
                    statement: <statement-string>
                ...

    example
    -------
        create_data_date_reference_transaction:
            type: 'datasets.pipelines.SQLStatementNode'
            processes:
                process_01:
                    priority: 1
                    parameters: ['']
                    statement: ''
    """

    def __init__(self, ps_node_name: str, **kwargs):
        super(SQLStatementNode, self).__init__(
            ps_node_name,
            kwargs.pop('processes', None)
        )


class PyFunctionNode(engine.Node):
    """
    config
    ------
        <Python-function-node-name>:
            type: datasets.pipelines.PyFunctionNode
            processes:
                <process-name>:
                    priority: <priority-number> (optional)
    """
    def __init__(self, ps_node_name: str, **kwargs):
        super(PyFunctionNode, self).__init__(
            ps_node_name,
            kwargs.pop('processes', None)
        )
