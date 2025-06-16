
from madsci.common.types.action_types import (
    ActionResult,
    ActionNotReady,
    ActionFailed,
    ActionSucceeded
)
from madsci.common.types.node_types import RestNodeConfig
from madsci.node_module.rest_node_module import RestNode

from madsci.client.resource_client import ResourceClient
from madsci.client.workcell_client import WorkcellClient
from madsci.common.types.location_types import LocationArgument
from madsci.common.types.workflow_types import Workflow
from pathlib import Path
from threading import Lock



class TransferConfig(RestNodeConfig):
    """Configuration for a Big Kahuna Node"""
    workflow_directory: Path
    resource_server_url: str
    workcell_server_url: str
    nodes: list[str]
    transfer_map: dict


    


class TransferNode(RestNode):
    """Node Module Implementation for the Big Kahuna Instruments"""

    config_model = TransferConfig
    

    def startup_handler(self):
        self.reserved_dict = {}
        for node in self.config.nodes:
            self.reserved_dict[node] = False
        self.workcell_client = WorkcellClient(workcell_manager_url=self.config.workcell_server_url)
        self.resource_client = ResourceClient(self.config.resource_server_url)
        self.reservation_lock = Lock()


    @action
    def transfer(
        self,
        source: LocationArgument,
        target: LocationArgument
    ) -> ActionResult:
        """Transfers between two locations protocol"""
        transfer_edge = self.config.transfer_map[source.location_name][target.location_name]
        workflow = self.config.workflow_directory / transfer_edge["workflow"]
        parameters = transfer_edge["parameters"]
        source_resource = self.resource_client.get_resource(source.resource_id)
        target_resource = self.resource_client.get_resource(target.resource_id)
        if source_resource.quantity == 0:
                return ActionFailed(
                    errors="Resource manager: Plate does not exist at source!"
                )
        elif target_resource.quantity == target_resource.capacity:
                return ActionNotReady(
                    errors="Resource manager: Target is occupied by another plate!"
                )
        workflow = Workflow.from_yaml(workflow)
        nodes = set()
        for step in workflow.steps:
             nodes.add(step.node)
        self.reservation_lock.acquire()
        can_reserve = True
        for node in nodes:
            can_reserve = can_reserve and not(self.reserved_dict[node])
        if can_reserve:
             for node in nodes:
                  self.reserved_dict[node] = True
             self.reservation_lock.release()
        else:
             self.reservation_lock.release()
             return ActionNotReady(errors=["Nodes are busy"])
        workflow = self.workcell_client.submit_workflow(workflow, parameters)
        self.reservation_lock.acquire()
        for node in nodes:
             self.reserved_dict[node] = False
        self.reservation_lock.release()
        return ActionSucceeded(data={"workflow": workflow.model_dump()})
        


   
 

if __name__ == "__main__":
    transfer_node = TransferNode()
    transfer_node.start_node()