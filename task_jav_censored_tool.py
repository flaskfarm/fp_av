from .setup import *
from pathlib import Path
ModelSetting = P.ModelSetting
from .tool import ToolExpandFileProcess, UtilFunc
from .model_jav_censored import ModelJavCensoredItem


class TaskCensoredJavTool:

    @F.celery.task
    def start(*args):
        logger.info("Jav Censored Task 시작")
        logger.info(d(args))
        if args[0] == 'gds':
            if args[1] == "make_actor":
                from .task_gds_actor_db import Task as TaskGdsActorDb
                TaskGdsActorDb.start()
            elif args[1] == 'make_yaml':
                from .task_make_yaml import Task as TaskMakeYaml
                TaskMakeYaml.start()

        

config = {
    "root": "/mnt/AV_MP/GDS/자막1",
    "finish_folder_path": "/data/db/jav_censored_gds_finish.txt",
    "check_file": [
        "movie.yaml",
        "movie.nfo",
        "poster.jpg",
        "fanart.jpg",
    ],
}









