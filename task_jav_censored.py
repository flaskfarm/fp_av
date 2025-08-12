from .setup import *
from pathlib import Path
import base64, json
import traceback  # traceback import 추가
import re # re import 추가
import os # os import 추가
import shutil # shutil import 추가
from datetime import datetime # datetime import 추가

ModelSetting = P.ModelSetting
from .tool import ToolExpandFileProcess, UtilFunc
from .model_jav_censored import ModelJavCensoredItem
from support import SupportYaml, SupportUtil, SupportDiscord
from .task_make_yaml import Task as TaskMakeYaml


class TaskBase:
    @F.celery.task
    def start(*args):
        logger.info(args)
        job_type = args[0]

        if job_type == 'default':
            # 1. Default 작업에 대한 config 생성
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

                "메타검색에공식사이트만사용": ModelSetting.get_bool("jav_censored_meta_dvd_use_dmm_only"),
                "메타매칭시이동폴더": ModelSetting.get("jav_censored_meta_dvd_path").strip(),
                "VR영상이동폴더": ModelSetting.get("jav_censored_meta_dvd_vr_path").strip(),
                "메타매칭실패시이동폴더": ModelSetting.get("jav_censored_meta_no_path").strip(),

                "메타매칭제외레이블": ModelSetting.get_list("jav_censored_meta_dvd_labels_exclude", ","),
                "메타매칭포함레이블": ModelSetting.get_list("jav_censored_meta_dvd_labels_include", ','),
                "배우조건매칭시이동폴더포맷": ModelSetting.get("jav_censored_folder_format_actor").strip(),
                "메타매칭실패시이동": False,

                "재시도": True,
                "방송": False,
                # 부가파일 생성 옵션 추가
                "부가파일생성_YAML": ModelSetting.get_bool("jav_censored_make_yaml"),
                "부가파일생성_NFO": ModelSetting.get_bool("jav_censored_make_nfo"),
                "부가파일생성_IMAGE": ModelSetting.get_bool("jav_censored_make_image"),
            }
            # 2. 공통 실행 헬퍼 호출
            TaskBase.__run_task(config)

        elif job_type == 'yaml':
            try:
                # jav_censored_yaml 모듈의 설정을 사용
                yaml_filepath = ModelSetting.get('jav_censored_yaml_filepath') 
                if not yaml_filepath:
                    logger.error("YAML 파일 경로가 설정되지 않았습니다.")
                    return

                yaml_data = SupportYaml.read_yaml(yaml_filepath)
                for job in yaml_data.get('작업', []):
                    if not job.get('사용', True):
                        continue
                    job['재시도'] = False # YAML 작업은 재시도 비활성화

                    # 공통 실행 헬퍼 호출 (각 job에 대해)
                    TaskBase.__run_task(job)
            except Exception as e:
                logger.error(f"YAML 파일 처리 중 오류 발생: {str(e)}")
                logger.error(traceback.format_exc())


    @staticmethod
    def __run_task(config):
        """
        작업 설정을 받아 공통 로직(사이트 목록 추가 등)을 적용하고 실제 Task를 실행하는 헬퍼 메소드
        """
        # 1. 사이트 목록 로드
        site_list_to_search = []
        # YAML 작업에서 이 옵션을 개별적으로 설정할 수 있게 하려면 config.get()을 사용
        use_dmm_mgs_only = config.get('메타검색에공식사이트만사용', ModelSetting.get_bool("jav_censored_meta_dvd_use_dmm_only"))

        if use_dmm_mgs_only:
            logger.info(f"작업 [{config.get('이름', 'N/A')}] : DMM+MGS만 사용하도록 설정됨.")
            site_list_to_search = ['dmm', 'mgstage']
        else:
            logger.info(f"작업 [{config.get('이름', 'N/A')}] : 메타데이터 플러그인 설정을 따르도록 설정됨.")
            try:
                meta_module = F.PluginManager.get_plugin_instance("metadata").get_module("jav_censored")
                if meta_module:
                    site_list_to_search = meta_module.P.ModelSetting.get_list('jav_censored_order', ',')
                    if not site_list_to_search:
                        raise ValueError("메타데이터 플러그인의 사이트 순서 설정이 비어있습니다.")
                    logger.debug(f"메타데이터 플러그인에서 가져온 검색 순서: {site_list_to_search}")
                else:
                    raise ModuleNotFoundError("메타데이터 플러그인을 로드할 수 없습니다.")
            except Exception as e:
                logger.error(f"메타데이터 플러그인에서 사이트 목록을 가져오는 데 실패했습니다: {e}")
                logger.warning("기본 검색 순서(dmm, mgstage)를 사용합니다.")
                site_list_to_search = ['dmm', 'mgstage']

        # 2. 로드한 사이트 목록을 config에 추가
        config['사이트목록'] = site_list_to_search

        # 3. 실제 Task 실행
        Task.start(config)


class Task:
    config = None
    metadata_module = None

    @staticmethod
    def start(config):
        Task.config = config
        logger.error(d(config))

        no_censored_path = Task.config['처리실패이동폴더'].strip()
        if not no_censored_path:
            logger.warning("'처리 실패시 이동 폴더'가 지정되지 않음. 작업을 중단합니다!")
            return

        no_censored_path_obj = Path(no_censored_path)
        if not no_censored_path_obj.is_dir():
            logger.warning(f"'처리 실패시 이동 폴더'가 존재하지 않는 디렉토리입니다. 작업을 중단합니다: {no_censored_path}")
            return

        src_list = Task.get_path_list(Task.config['다운로드폴더'])
        if config.get('재시도', False):
            src_list += Task.__add_meta_no_path()

        if not src_list:
            logger.warning("'다운로드 폴더'가 지정되지 않음. 작업을 중단합니다!")
            return

        files = []
        for src_item in src_list:
            src = Path(src_item)

            ToolExpandFileProcess.preprocess_cleanup(
                src,
                min_size=Task.config.get('최소크기', 0),
                max_age=Task.config.get('최대기간', 0),
            )

            _f = ToolExpandFileProcess.preprocess_listdir(
                src,
                no_censored_path_obj,
                min_size=Task.config.get('최소크기', 0),
                disallowed_keys=Task.config.get('파일처리하지않을파일명', [])
            )
            files += _f or []

        logger.info(f"처리할 파일 {len(files)}개")

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


    @staticmethod
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
                logger.error(f"파일 '{file.name}' 처리 중 메타 검색 단계에서 오류가 발생하여 건너뜁니다.")
                target_dir, move_type, meta_info = None, None, None

        # target_dir이 결정된 후, None이 아니고 str 타입이라면 즉시 Path 객체로 변환
        if target_dir is not None and isinstance(target_dir, str):
            # logger.debug(f"target_dir이 문자열 타입({target_dir})이므로 Path 객체로 변환합니다.")
            target_dir = Path(target_dir)

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


    @staticmethod
    def __get_target_with_meta_dvd(search_name):
        meta_module = Task.get_meta_module()
        if meta_module is None:
            logger.error("메타데이터 플러그인을 찾을 수 없습니다. 메타 검색을 건너뜁니다.")
            return None, None

        label = search_name.split("-")[0]
        meta_dvd_labels_exclude = map(str.strip, Task.config.get('메타매칭제외레이블', []))
        if label in map(str.lower, meta_dvd_labels_exclude):
            logger.info("'정식발매 영상 제외 레이블'에 포함: %s", label)
            return None, None

        target_root_str = Task.config.get('메타매칭시이동폴더', '').strip()
        if not target_root_str:
            raise ValueError("'정상 매칭시 이동 경로'가 지정되지 않았습니다.")

        target_root_path = Path(target_root_str)
        if not target_root_path.is_dir():
            raise NotADirectoryError(f"'정상 매칭시 이동 경로'가 존재하지 않음: {target_root_str}")

        # Task.config에서 미리 로드된 사이트 목록을 가져옴
        site_list_to_search = Task.config.get('사이트목록', [])
        if not site_list_to_search:
            logger.warning("검색할 사이트 목록이 비어있습니다. 메타 검색을 건너뜁니다.")
            return None, None

        # 순차적으로 사이트 검색 및 조기 탈출
        for site in site_list_to_search:
            try:
                logger.debug(f"메타 검색 시도: 사이트=[{site}], 검색어=[{search_name}]")
                search_result_list = meta_module.search2(search_name, site, manual=False)

                if not search_result_list:
                    continue

                # 95점 이상인 첫 번째 결과를 찾으면 바로 사용
                best_match = next((item for item in search_result_list if item.get('score', 0) >= 95), None)

                if best_match:
                    logger.info(f"매칭 성공! 사이트=[{site}], 코드=[{best_match['code']}], 점수=[{best_match['score']}]")
                    # metadata 플러그인의 info 메소드 직접 호출
                    meta_info = meta_module.info(best_match["code"])
                    if meta_info:
                        folders = Task.process_folder_format("dvd", meta_info)
                        current_target_root = target_root_path

                        # VR 영상 처리
                        is_vr = any(x in (meta_info.get("genre") or []) for x in ["고품질VR", "VR전용"]) or \
                                any(x in (meta_info.get("title") or "") for x in ["[VR]", "[ VR ]", "【VR】"])

                        if is_vr:
                            vr_path_str = Task.config.get('VR영상이동폴더', '').strip()
                            if vr_path_str:
                                vr_path = Path(vr_path_str)
                                if vr_path.is_dir():
                                    current_target_root = vr_path
                                    logger.info(f"VR 영상으로 판단되어 이동 경로를 변경합니다: {vr_path}")
                                else:
                                    logger.warning("'VR영상 이동 경로'가 존재하지 않음: %s. 기본 경로를 사용합니다.", vr_path_str)

                        logger.info(f"메타매칭 최종 성공: {meta_info['code']}")
                        return current_target_root.joinpath(*folders), meta_info
                    else:
                        logger.warning(f"검색은 성공했으나 메타 정보({best_match['code']})를 가져오지 못했습니다. 다음 사이트를 검색합니다.")

            except Exception as e:
                logger.error(f"'{site}' 사이트 검색 중 예외 발생: {e}")
                logger.debug(traceback.format_exc())

        logger.info(f"'{search_name}'에 대한 유효한(95점 이상) 메타 정보를 찾지 못했습니다.")

        # 메타 검색 실패 후 처리
        meta_dvd_labels_include = map(str.strip, Task.config.get('메타매칭포함레이블', []))
        if label in map(str.lower, meta_dvd_labels_include):
            folders = Task.process_folder_format("normal", search_name)
            logger.info("메타 매칭에 실패했지만 '정식발매 영상 포함 레이블'에 해당되어 처리: %s", label)
            return target_root_path.joinpath(*folders), None

        return None, None


    @staticmethod
    def __get_target_with_meta(search_name):
        target_dir, meta_info = Task.__get_target_with_meta_dvd(search_name)
        if target_dir is not None:
            # __get_target_with_meta_dvd는 Path 객체를 반환하므로 안전
            return target_dir, "dvd", meta_info

        # 'no_meta' 처리 케이스
        move_type = "no_meta"

        # 설정에서 '메타 없는 영상 이동 경로'를 가져옴
        no_meta_path_str = Task.config.get('메타매칭실패시이동폴더', '').strip()

        final_path_obj = None
        # 1. 설정된 경로가 있고, 실제로 존재하는 디렉토리인지 확인
        if no_meta_path_str and Path(no_meta_path_str).is_dir():
            # 유효한 경로이므로 Path 객체로 변환하여 사용
            final_path_obj = Path(no_meta_path_str)
            logger.info(f"메타 없음: 설정된 폴더로 이동합니다 - {final_path_obj}")
        else:
            # 2. 설정된 경로가 없거나 유효하지 않으면, '처리실패이동폴더' 아래에 [NO META] 폴더를 사용
            temp_path_str = Task.config.get('처리실패이동폴더', '').strip()
            # Task.start에서 temp_path_str의 유효성은 이미 검증됨
            final_path_obj = Path(temp_path_str).joinpath("[NO META]")
            logger.info(f"메타 없음: 기본 [NO META] 폴더로 이동합니다 - {final_path_obj}")

        return final_path_obj, move_type, None


    @staticmethod
    def __add_meta_no_path():
        meta_no_path_str = Task.config.get('메타매칭실패시이동폴더', '').strip()
        if not meta_no_path_str:
            temp_path_str = Task.config.get('처리실패이동폴더', '').strip()
            # temp_path_str는 start에서 유효성 검증됨
            meta_no_path = Path(temp_path_str).joinpath("[NO META]")
        else:
            meta_no_path = Path(meta_no_path_str)
        
        if not meta_no_path.is_dir():
            return []

        meta_no_retry_every = ModelSetting.get_int("jav_censored_meta_no_retry_every")
        if meta_no_retry_every <= 0:
            return []
        
        meta_no_last_retry = datetime.fromisoformat(ModelSetting.get("jav_censored_meta_no_last_retry"))
        if (datetime.now() - meta_no_last_retry).days < meta_no_retry_every:
            return []
        
        ModelSetting.set("jav_censored_meta_no_last_retry", datetime.now().isoformat())
        # Path 객체를 리스트에 담아 반환
        return [meta_no_path]


    @staticmethod
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


    @staticmethod
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


    @staticmethod
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


    @staticmethod
    def get_path_list(value):
        tmps = map(str.strip, value)
        ret = []
        for t in tmps:
            if not t or t.startswith("#"):
                continue
            if t.endswith("*"):
                # os.path.dirname은 문자열을 반환하므로 Path()로 감싸줍니다.
                dirname = Path(os.path.dirname(t)) 
                if not dirname.is_dir():
                    continue
                listdirs = os.listdir(dirname)
                for l in listdirs:
                    # dirname이 이미 Path 객체이므로 .joinpath를 사용할 수 있습니다.
                    ret.append(dirname.joinpath(l)) 
            else:
                # 문자열을 Path 객체로 변환하여 추가합니다.
                ret.append(Path(t))
        return ret


    metadata_module = None
    @staticmethod
    def get_meta_module():
        try:
            if Task.metadata_module is None:
                Task.metadata_module = F.PluginManager.get_plugin_instance("metadata").get_module("jav_censored")
            return Task.metadata_module
        except Exception as exception:
            logger.debug("Exception:%s", exception)
            logger.debug(traceback.format_exc())
            return None # 모듈 로딩 실패 시 None 반환
