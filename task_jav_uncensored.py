from .setup import *
from pathlib import Path
import json, base64
ModelSetting = P.ModelSetting
from support import SupportYaml, SupportDiscord
from .task_make_yaml import Task as TaskMakeYaml

class Task:
    @F.celery.task
    def start(*args):
        #metamodule = Task.get_meta_module()
        logger.info(args)
        if args[0] == 'default':
            try:
                yaml_data = SupportYaml.read_yaml(ModelSetting.get('jav_uncensored_filepath'))
                for job in yaml_data.get('작업', []):
                    if job.get('사용', True) == False:
                        continue
                    Task.run(job)
            except Exception as e:
                logger.error(f"YAML 파일 처리 중 오류 발생: {str(e)}")
                

    def run(config):
        Task.config = config
        logger.error(d(config))
        
        for basefolder in Task.config['다운로드폴더']:
            for base, dirs, files in os.walk(basefolder):
                for file in files:
                    if not file.lower().endswith(('.mp4', '.mkv', '.avi', '.mov', '.wmv', '.srt', '.smi', '.ass', '.ssa', '.sup', '.vtt')):
                        continue
                    file_path = Path(base).joinpath(file)
                    if file_path.is_file() == False:
                        continue
                            
                    try:
                        Task.process_file(file_path)
                    except Exception:
                        logger.exception("개별 파일 처리 중 예외: %s", file)
                    else:
                        #if entity.move_type is not None:
                        #    entity.save()
                        pass


    def process_file(filepath):
        metamodule = Task.get_meta_module()
        logger.info(filepath)
        filename = os.path.basename(filepath)

        for key, site in metamodule.site_map.items():
            if any(k in filename.lower() for k in site['keyword']):
                instance = site['instance']
                match = re.match(site['regex'], filename)
                if match:
                    code = match.group('code')
                else:
                    return

                data = instance.search(code, do_trans=False, manual=False)
                if data.get('ret') != 'success':
                    logger.error(f"검색 실패: {data.get('msg')}")
                    return
                data = data.get('data', [])
                if data and len(data) > 0 and data[0]["score"] >= 90:
                    meta_info = instance.info(data[0]["code"])
                    if meta_info['ret'] != 'success':
                        return
                    meta_info = meta_info['data']

                    #logger.error(f"메타 정보: {meta_info}")

                target_dir = Task.config['메타매칭시이동폴더']
                foldername = f"{meta_info['title']} ({meta_info['year']})"

                if Task.config.get('서브폴더사용', False):
                    if key in ['1pondo', '10musume', 'carib' ]:
                        my = meta_info['code'][6:8] + meta_info['code'][2:4]
                    elif key == 'heyzo':
                        my = meta_info['code'][2:4]
                    elif key == 'fc2':
                        my = meta_info['code'][2:5]

                    target_dir = os.path.join(target_dir, key, my, foldername)
                else:
                    target_dir = os.path.join(target_dir, key, foldername)
                os.makedirs(target_dir, exist_ok=True)
                shutil.move(filepath, target_dir)

                TaskMakeYaml.make_files(
                    meta_info,
                    str(target_dir),
                    make_yaml=True,
                    make_nfo=True,
                    make_image=True,
                )

                try:
                    if Task.config.get('방송', False):
                        bot = {
                            't1': 'gds_tool',
                            't2': 'fp',
                            't3': 'av',
                            'data': {
                                'gds_path': target_dir.replace('/mnt/AV/MP/GDS', '/ROOT/GDRIVE/VIDEO/AV') + "/" + filename,
                            }
                        }
                        hook = base64.b64decode(b'aHR0cHM6Ly9kaXNjb3JkLmNvbS9hcGkvd2ViaG9va3MvMTM5OTkxMDg4MDE4NzEyNTgxMS84SFY0bk93cGpXdHhIdk5TUHNnTGhRbDhrR3lGOXk4THFQQTdQVTBZSXVvcFBNN21PWHhkSVJSNkVmcmIxV21UdFhENw==').decode('utf-8')
                        SupportDiscord.send_discord_bot_message(json.dumps(bot), hook)
                except Exception as e:
                    logger.error("방송 메시지 전송 실패: %s", e)
            
                return





    metadata_module = None
    def get_meta_module():
        try:
            if Task.metadata_module is None:
                Task.metadata_module = F.PluginManager.get_plugin_instance("metadata").get_module("jav_uncensored")
            return Task.metadata_module
        except Exception as exception:
            logger.debug("Exception:%s", exception)
            logger.debug(traceback.format_exc())


