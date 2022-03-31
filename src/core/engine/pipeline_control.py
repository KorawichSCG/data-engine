from typing import Any, Dict, List


class Node:
    def __init__(self, ps_node_name: str, ps_node_processes: Dict[str, Any]):
        self.ps_node_name = ps_node_name
        self.ps_node_processes = ps_node_processes

    @property
    def processes(self) -> List:
        return list(self.ps_node_processes.keys())

    def __str__(self):
        return f'{self.__class__.__name__}({self.ps_node_name})'


class Pipeline:
    """
    config
    ------
        <pipeline-name>:
            type: datasets.pipelines.Pipeline
            nodes:
    """
