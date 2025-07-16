from .setup import *
from pathlib import Path
ModelSetting = P.ModelSetting
from .tool import ToolExpandFileProcess, UtilFunc
from .model_jav_censored import ModelJavCensoredItem
import sqlite3
from support.expand.rclone import SupportRclone


config = {
    "db_filepath": "/data/plugins_dev/metadata/files/jav_actors2.db",
    "mount_path": "/mnt/AV_MP/GDS/ActorImages/jav/actors",
    "config_path": "/data/rclone.conf",
    "remote": "admin:{1XADj8-03xsfOuhEA1gAfwZ7WgSyMGoeX}/GDS/ActorImages/jav/actors",

}

class Task:

    @F.celery.task
    def start(*args):
        logger.info("gds_actor_db 시작")
        logger.info(d(args))

        conn = sqlite3.connect(config['db_filepath'])
        cursor = conn.cursor()

        for folder in os.listdir(config['mount_path']):
            path_folder = os.path.join(config['mount_path'], folder)
            
            data = SupportRclone.lsjson(f"{config['remote']}/{folder}", config_path=config['config_path'])
            #logger.info(f"폴더: {folder}, 데이터: {data}")

            for item in data:
                profile_img_path = f"jav/actors/{folder}/{item['Path']}"

                cursor.execute('''UPDATE actors SET google_fileid = ? WHERE profile_img_path = ?''', (item['ID'], profile_img_path))

        conn.commit()
        conn.close()



