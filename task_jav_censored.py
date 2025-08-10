from .setup import *
from pathlib import Path
import base64, json
ModelSetting = P.ModelSetting
from .tool import ToolExpandFileProcess, UtilFunc
from .model_jav_censored import ModelJavCensoredItem
from support import SupportYaml, SupportUtil, SupportDiscord
from .task_make_yaml import Task as TaskMakeYaml

class TaskBase:
    @F.celery.task
    def start(*args):
        logger.info(args)
        if args[0] == 'default':
            config = {
                "이름": "default",
                "사용": True,
                "처리실패이동폴더": ModelSetting.get("jav_censored_temp_path").strip(),
                "다운로드폴더": ModelSetting.get("jav_censored_download_path").splitlines(),
                "라이브러리폴더": ModelSetting.get("jav_censored_target_path").splitlines(),
                "최소크기": ModelSetting.get_int("jav_censored_min_size"),
                "최대기간": ModelSetting.get_int("jav_censored_max_age"),
                "파일처리하지않을파일명": ModelSetting.get_list("jav_censored_filename_not_allowed_list", "|"),

                "이동폴더포맷": ModelSetting.get("jav_censored_folder_format"),
                "메타사용": ModelSetting.get("jav_censored_use_meta"),
                "파일명변경": ModelSetting.get_bool("jav_censored_change_filename"),
                "원본파일명포함여부": ModelSetting.get_bool("jav_censored_include_original_filename"),
                "원본파일명처리옵션": ModelSetting.get("jav_censored_include_original_filename_option"),

                #"메타검색에공식사이트만사용": ModelSetting.get_bool("jav_censored_meta_dvd_use_dmm_only"),
                "메타매칭시이동폴더": ModelSetting.get("jav_censored_meta_dvd_path").strip(),
                "VR영상이동폴더": ModelSetting.get("jav_censored_meta_dvd_vr_path").strip(),
                "메타매칭실패시이동폴더": ModelSetting.get("jav_censored_meta_no_path").strip(),

                "메타매칭제외레이블": ModelSetting.get_list("jav_censored_meta_dvd_labels_exclude", ","),
                "메타매칭포함레이블": ModelSetting.get_list("jav_censored_meta_dvd_labels_include", ','),
                "배우조건매칭시이동폴더포맷": ModelSetting.get("jav_censored_folder_format_actor").strip(),
                "메타매칭실패시이동": False,

                "재시도": True,
                "방송": False,
            }
            Task.start(config)
        else:
            try:
                yaml_data = SupportYaml.read_yaml(ModelSetting.get('jav_censored_yaml_filepath'))
                for job in yaml_data.get('작업', []):
                    if job.get('사용', True) == False:
                        continue
                    job['재시도'] = False
                    Task.start(job)
            except Exception as e:
                logger.error(f"YAML 파일 처리 중 오류 발생: {str(e)}")
                


class Task:
    config = None
    def start(config):
        Task.config = config
        logger.error(d(config))
        no_censored_path = Task.config['처리실패이동폴더'].strip()
        #if not no_censored_path:
        #    logger.warning("'처리 실패시 이동 폴더'가 지정되지 않음. 작업 중단!")
        #    return
        #no_censored_path = Path(no_censored_path)
        #if not no_censored_path.is_dir():
        #    logger.warning("'처리 실패시 이동 폴더'가 존재하지 않음. 작업 중단: %s", no_censored_path)
        #    return

        src_list = Task.get_path_list(Task.config['다운로드폴더'])
        if config['재시도']:
            src_list += Task.__add_meta_no_path()
        if not src_list:
            logger.warning("'다운로드 폴더'가 지정되지 않음. 작업 중단!")
            return

        # 전처리
        files = []
        for src in src_list:
            ToolExpandFileProcess.preprocess_cleanup(
                src, 
                min_size = Task.config.get('최소크기', 0),
                max_age = Task.config.get('최대기간', 0),
            )
            _f = ToolExpandFileProcess.preprocess_listdir(
                src, 
                no_censored_path, 
                min_size = Task.config.get('최소크기', 0), 
                disallowed_keys = Task.config.get('파일처리하지않을파일명', [])
            )
            files += _f or []
        logger.info(f"처리할 파일 {len(files)}개")

        # 본처리
        for idx, file in enumerate(files):
            logger.debug("[%03d/%03d] %s", idx + 1, len(files), file.name)
            try:
                entity = Task.__task(file)
                if entity is None:
                    continue
            except Exception:
                logger.exception("개별 파일 처리 중 예외: %s", file)
            else:
                if entity.move_type is not None:
                    entity.save()


    def __task(file):
        newfilename = ToolExpandFileProcess.change_filename_censored(file.name)
        newfilename = Task.check_newfilename(file.name, newfilename, str(file))

        # 검색용 키워드
        search_name = ToolExpandFileProcess.change_filename_censored(newfilename)
        search_name = search_name.split(".")[0]
        #search_name = os.path.splitext(search_name)[0].replace("-", " ")
        search_name = re.sub(r"\s*\[.*?\]", "", search_name).strip()
        match = re.search(r"(?P<cd>cd\d{1,2})$", search_name)
        if match:
            search_name = search_name.replace(match.group("cd"), "")
        logger.debug("search_name=%s", search_name)

        #
        # target_dir를 결정하라!
        #
        target_dir, move_type, meta_info = None, None, None
        if Task.config.get('메타사용', "not_using") == "not_using":
            move_type = "normal"
            target = Task.get_path_list(Task.config['라이브러리폴더'])
            folders = Task.process_folder_format(move_type, search_name)
            # 2025.07.12 by soju6jan
            # 아래 로직 의미를 모르겠음

            ## 첫번째 자식폴더만 타겟에서 찾는다.
            #for tmp in target:
            #    tmp_dir = Path(tmp).joinpath(folders[0])
            #    if tmp_dir.exists():
            #        target_dir = tmp_dir
            #        break
            ## 없으면 첫번째 타겟으로
            #if target_dir is None and target:
            #    target_dir = Path(target[0]).joinpath(*folders)
            target_dir = Path(target[0]).joinpath(*folders)
        else:
            # 메타 처리
            try:
                target_dir, move_type, meta_info = Task.__get_target_with_meta(search_name)
            except Exception:
                logger.exception("메타를 이용한 타겟 폴더 결정 중 예외:")
        logger.debug("target_dir: %s", target_dir)

        if move_type == "no_meta" and Task.config.get('메타매칭실패시이동', True) == False:
            return

        # 2021-04-30
        try:
            match = re.compile(r"\d+\-?c(\.|\].)|\(").search(file.stem.lower())
            if match:
                for cd in ["1", "2", "4"]:
                    cd_name = f'{search_name.replace(" ", "-")}cd{cd}{file.suffix}'
                    cd_file = Path(target_dir).joinpath(cd_name)
                    if cd_file.exists():
                        newfilename = f'{search_name.replace(" ", "-")}cd3{file.suffix}'
                        break
        except Exception as e:
            logger.debug("Exception:%s", e)
            logger.debug(traceback.format_exc())

        #
        # 실제 이동
        #
        entity = ModelJavCensoredItem(Task.config['이름'], str(file.parent), file.name)
        if move_type is None or target_dir is None:
            logger.warning("타겟 폴더를 결정할 수 없음")
            return entity.set_move_type(None)

        if not target_dir.exists():
            target_dir.mkdir(parents=True)
        else:
            #logger.error("타겟 폴더가 이미 존재함: %s", target_dir)
            pass

        if move_type == "no_meta":
            newfile = target_dir.joinpath(file.name)  # 원본 파일명 그대로
            if file == newfile:
                # 처리한 폴더를 다시 처리했을 때 중복으로 삭제되지 않아야 함
                return entity.set_move_type(None)
            UtilFunc.move(file, newfile)
            return entity.set_target(newfile).set_move_type(move_type)

        newfile = target_dir.joinpath(newfilename)

        if UtilFunc.is_duplicate(file, newfile):
            logger.debug("동일 파일(크기, 이름 기준)이 존재함: %s -> %s", file, newfile.parent)
            remove_path = Task.config['처리실패이동폴더']
            move_type += "_already_exist"
            
            if not file.exists():
                logger.warning(f"파일이 처리 도중 사라졌습니다 (다른 프로세스가 먼저 처리한 것으로 추정): {file.name}")
                return entity.set_move_type("already_processed_by_other") # DB에 기록할 상태 변경

            if remove_path == "":
                file.unlink()
                logger.debug("Deleted: %s", file)
                return entity.set_move_type(move_type)
            else:
                dup = Path(remove_path).joinpath(file.name)
                UtilFunc.move(file, dup)
                return entity.set_target(dup).set_move_type(move_type)

        if file.exists():
            if Task.config.get('파일명변경', False):
                shutil.move(file, newfile)
            else:
                shutil.move(file, newfile.parent)
            
            if meta_info is not None:
                TaskMakeYaml.make_files(
                    meta_info,
                    str(newfile.parent),
                    make_yaml=Task.config.get('부가파일생성_YAML', False),
                    make_nfo=Task.config.get('부가파일생성_NFO', False),
                    make_image=Task.config.get('부가파일생성_IMAGE', False),
                )
            try:
                if Task.config.get('방송', False):
                    bot = {
                        't1': 'gds_tool',
                        't2': 'fp',
                        't3': 'av',
                        'data': {
                            'gds_path': str(newfile).replace('/mnt/AV/MP/GDS', '/ROOT/GDRIVE/VIDEO/AV'),
                        }
                    }
                    hook = base64.b64decode(b'aHR0cHM6Ly9kaXNjb3JkLmNvbS9hcGkvd2ViaG9va3MvMTM5OTkxMDg4MDE4NzEyNTgxMS84SFY0bk93cGpXdHhIdk5TUHNnTGhRbDhrR3lGOXk4THFQQTdQVTBZSXVvcFBNN21PWHhkSVJSNkVmcmIxV21UdFhENw==').decode('utf-8')
                    SupportDiscord.send_discord_bot_message(json.dumps(bot), hook)
            except Exception as e:
                logger.error("방송 메시지 전송 실패: %s", e)

        return entity.set_target(newfile).set_move_type(move_type)
    

    def __get_target_with_meta_dvd(search_name):
        meta_module = Task.get_meta_module()
        label = search_name.split("-")[0]
        meta_dvd_labels_exclude = map(str.strip, Task.config.get('메타매칭제외레이블', []))
        if label in map(str.lower, meta_dvd_labels_exclude):
            logger.info("'정식발매 영상 제외 레이블'에 포함: %s", label)
            return None, None

        target_root = Task.config['메타매칭시이동폴더'].strip()
        #if target_root is None:
        #    raise NotADirectoryError("'정식발매 영상 매칭시 이동 경로'가 지정되지 않음")
        target_root = Path(target_root)
        if not target_root.is_dir():
            raise NotADirectoryError("'정식발매 영상 매칭시 이동 경로'가 존재하지 않음")

        
        #if Task.config['메타검색에공식사이트만사용']:
        #    logger.info("정식발매 영상 판단에 DMM+MGS만 사용")
        #    data = (
        #        meta_module.search2(search_name, "dmm", manual=False)
        #        or meta_module.search2(search_name, "mgstage", manual=False)
        #        or []
        #    )
        #else:
        #    data = meta_module.search(search_name, manual=False)

        default_site_list = [
            {'사이트': 'dmm', '점수': 95},
            {'사이트': 'mgstage', '점수': 95}
        ]

        site_list = Task.config.get('메타검색에사용할사이트', default_site_list)

        for site in site_list:
            try:
                logger.info(f"메타매칭 시작: {site['사이트']}   {search_name}")
                tmp = search_name
                #if site == "javdb":
                #    tmp = search_name.replace(" ", "-").upper()
                data = meta_module.search2(tmp, site['사이트'], manual=False)

                if data and len(data) > 0 and data[0]["score"] >= site['점수']:
                    meta_info = meta_module.info(data[0]["code"])
                    if meta_info is not None:
                        folders = Task.process_folder_format("dvd", meta_info)
                        if any(x in (meta_info["genre"] or []) for x in ["고품질VR", "VR전용"]) or any(
                            x in (meta_info["title"] or "") for x in ["[VR]", "[ VR ]", "【VR】"]
                        ):
                            vr_path = Task.config.get('VR영상이동폴더', '').strip()
                            if vr_path != "":
                                vr_path = Path(vr_path)
                                if vr_path.is_dir():
                                    target_root = vr_path
                                else:
                                    logger.warning("'정식발매 VR영상 이동 경로'가 존재하지 않음")
                        logger.info(f"메타매칭 성공: {meta_info['code']}")
                        return target_root.joinpath(*folders), meta_info
            except Exception as e:
                logger.debug(f"메타매칭 중 예외 발생: {site['사이트']}\n{e}")
                logger.debug(traceback.format_exc())

        meta_dvd_labels_include = map(str.strip, Task.config.get('메타매칭포함레이블', []))
        if label in map(str.lower, meta_dvd_labels_include):
            folders = Task.process_folder_format("normal", search_name)
            logger.info("정식발매 영상 매칭 실패하였지만 '정식발매 영상 포함 레이블'에 포함: %s", label)
            return target_root.joinpath(*folders), None
        return None, None


    def __get_target_with_meta(search_name):
        target_dir, meta_info = Task.__get_target_with_meta_dvd(search_name)
        if target_dir is not None:
            return target_dir, "dvd", meta_info

        move_type = "no_meta"
        # NO META인 경우 폴더 구조를 만들지 않음
        target_root = Task.config['메타매칭실패시이동폴더'].strip()
        if not (target_root and Path(target_root).exists()):
            target_root = Path(Task.config['처리실패이동폴더'].strip()).joinpath("[NO META]")
        logger.info("메타 없음으로 최종 판별")
        return Path(target_root), move_type, None


    def __add_meta_no_path():
        meta_no_path = Task.config['메타매칭실패시이동폴더'].strip()
        if not meta_no_path:
            meta_no_path = Path(Task.config['처리실패이동폴더'].strip()).joinpath("[NO META]")
            if not meta_no_path.is_dir():
                return []
            meta_no_path = str(meta_no_path)
        meta_no_retry_every = ModelSetting.get_int("jav_censored_meta_no_retry_every")
        if meta_no_retry_every <= 0:
            return []
        meta_no_last_retry = datetime.fromisoformat(ModelSetting.get("jav_censored_meta_no_last_retry"))
        if (datetime.now() - meta_no_last_retry).days < meta_no_retry_every:
            return []
        ModelSetting.set("jav_censored_meta_no_last_retry", datetime.now().isoformat())
        return [meta_no_path]


    def process_folder_format(meta_type, meta_info):
        folders = None
        folder_format = Task.config['이동폴더포맷'].strip()
        if meta_type in ["normal", "no_meta"]:
            data = {
                "code": meta_info.replace(" ", "-").upper(),
                "label": meta_info.split(" ")[0].upper(),
                "label_1": meta_info.split(" ")[0].upper()[0],
                "year": "",
                "actor":"",
                "actor_2": "",
                "actor_3": "",
                "studio": "NO_STUDIO",
            }
            if data['label_1'].isdigit():
                data['label_1'] = "#"
            folders = folder_format.format(**data)
        elif meta_type == "dvd":
            if meta_info['actor'] == None:
                meta_info['actor'] = []
            actor_names = [actor.get('name', '') for actor in meta_info['actor'][:3]]
            actor_names = [name for name in actor_names if name and name.strip()]
            data = {
                "studio": meta_info.get("studio", None) or "NO_STUDIO",
                "code": meta_info["originaltitle"],
                "label": meta_info["originaltitle"].split("-")[0],
                "actor": f"{','.join(actor_names[:1])}",
                "actor_2": f"{','.join(actor_names[:2])}",
                "actor_3": f"{','.join(actor_names[:3])}",
                "year": meta_info.get("year", ""),
            }
            match = re.compile(r"\d{2}id", re.I).search(data['label'])
            if match:
                data['label'] = "ID"
            data['label_1'] = data['label'][0]
            if data['label_1'].isdigit():
                data['label_1'] = "#"


            folder_format_actor = Task.config.get('배우조건매칭시이동폴더포맷', '')
            if (
                folder_format_actor
                and meta_info["actor"] is not None
                and len(meta_info["actor"]) == 1
                and meta_info["actor"][0]["originalname"] != meta_info["actor"][0]["name"]
                and meta_info["actor"][0]["name"] != ""
            ):
                folders = folder_format_actor.format(**data)
            else:
                folders = folder_format.format(**data)
        
        folders = re.sub(r'\{.*?\}', '', folders)
        folders = folders.replace("[]", "").replace("{}", "").replace("()", "")
        folders = re.sub(r'\s{2,}', ' ', folders).strip()                
        folders = folders.split("/")
        return folders


    def check_newfilename(filename, newfilename, file_path):
        # 이미 파일처리를 한거라면..
        # newfilename 과 filename 이 [] 제외하고 같다면 처리한파일로 보자
        # 그런 파일은 다시 원본파일명 옵션을 적용하지 않아야한다.
        # logger.debug(filename)
        # logger.debug(newfilename)
        # adn-091-uncenrosed.mp4
        # 같이 시작하더라도 [] 가 없다면... 변경
        # [] 없거나, 시작이 다르면..  완벽히 일치 하지 않으면

        # 2021-04-21 ??????
        if filename == newfilename and filename.find("[") == -1 and filename.find("]") == -1:
            newfilename = Task.change_filename_censored_by_save_original(filename, newfilename, file_path)
        elif filename != newfilename and (
            (filename.find("[") == -1 or filename.find("]") == -1)
            or not os.path.splitext(filename)[0].startswith(os.path.splitext(newfilename)[0])
        ):
            newfilename = Task.change_filename_censored_by_save_original(filename, newfilename, file_path)
        else:
            # 이미 한번 파일처리를 한것으로 가정하여 변경하지 않는다.
            newfilename = filename
            # 기존에 cd1 [..].mp4 는 []를 제거한다
            match = re.search(r"cd\d(?P<remove>\s\[.*?\])", newfilename)
            if match:
                newfilename = newfilename.replace(match.group("remove"), "")

        logger.debug("%s => %s", filename, newfilename)
        return newfilename


    def change_filename_censored_by_save_original(original_filename, new_filename, original_filepath):
        """원본파일명 보존 옵션에 의해 파일명을 변경한다."""
        try:
            if not Task.config.get('원본파일명포함여부', True):
                return new_filename

            new_name, new_ext = os.path.splitext(new_filename)
            part = None
            match = re.search(r"(?P<part>cd\d+)$", new_name)
            if match:
                # cd1 앞에가 같아야함.
                return new_filename
                # part = match.group("part")
                # new_name = new_name.replace(part, "")

            ori_name, _ = os.path.splitext(original_filename)
            # 2019-07-30
            ori_name = ori_name.replace("[", "(").replace("]", ")").strip()
            if part is not None:
                # 안씀
                return f"{new_name} [{ori_name}] {part}{new_ext}"

            option = Task.config.get('원본파일명처리옵션', 'original')
            if option == "original" or original_filepath is None:
                return f"{new_name} [{ori_name}]{new_ext}"
            if option == "original_bytes":
                str_size = os.stat(original_filepath).st_size
                return f"{new_name} [{ori_name}({str_size})]{new_ext}"
            if option == "original_giga":
                str_size = SupportUtil.sizeof_fmt(os.stat(original_filepath).st_size, suffix="B")
                return f"{new_name} [{ori_name}({str_size})]{new_ext}"
            if option == "bytes":
                str_size = os.stat(original_filepath).st_size
                return f"{new_name} [{str_size}]{new_ext}"
            return f"{new_name} [{ori_name}]{new_ext}"
        except Exception as exception:
            logger.debug("Exception:%s", exception)
            logger.debug(traceback.format_exc())



    def get_path_list(value):
        tmps = map(str.strip, value)
        ret = []
        for t in tmps:
            if not t or t.startswith("#"):
                continue
            if t.endswith("*"):
                dirname = os.path.dirname(t)
                listdirs = os.listdir(dirname)
                for l in listdirs:
                    ret.append(os.path.join(dirname, l))
            else:
                ret.append(t)
        return ret





    metadata_module = None
    def get_meta_module():
        try:
            if Task.metadata_module is None:
                Task.metadata_module = F.PluginManager.get_plugin_instance("metadata").get_module("jav_censored")
            return Task.metadata_module
        except Exception as exception:
            logger.debug("Exception:%s", exception)
            logger.debug(traceback.format_exc())


