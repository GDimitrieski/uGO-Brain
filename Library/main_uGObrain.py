from Library.login import login
from Library.credentials import credentials
from engine.ugo_robot_client import UgoRobotClient
from engine.command_layer import TaskCatalog, CommandSender

token = login(credentials["url"], credentials["user"], credentials["password"])

robot = UgoRobotClient(base_url=credentials["url"], token=token)
catalog = TaskCatalog.from_file("Available_Tasks.json")
sender = CommandSender(robot=robot, catalog=catalog)

# This will generate EXACT payload structure expected:
# {"Navigate": {"taskName":"Navigate","AMR_PosTarget":"2","AMR_Footprint":"1","AMR_DOCK":"1"}}
res = sender.run("Navigate", overrides={"AMR_PosTarget": "2"}, task_name="Go to InputStation")
#res = sender.run("Pick", overrides={}, task_name="Pick")
print(res)
print("Receiver:", res.get("receiver"))
print("Dispatch path:", " -> ".join(res.get("dispatch_path", [])))


