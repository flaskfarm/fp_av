from .setup import *
from pathlib import Path
import base64, json
import traceback
import re
import os
import shutil
import time
import string
from datetime import datetime

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
                "중복파일이동폴더": ModelSetting.get("jav_censored_remove_path").strip(),
                "다운로드폴더": ModelSetting.get("jav_censored_download_path").splitlines(),
                "라이브러리폴더": ModelSetting.get("jav_censored_target_path").splitlines(),

                "최소크기": ModelSetting.get_int("jav_censored_min_size"),
                "최대기간": ModelSetting.get_int("jav_censored_max_age"),
                "품번파싱제외키워드": ModelSetting.get_list("jav_censored_filename_cleanup_list", "|"),
                "파일처리하지않을파일명": ModelSetting.get_list("jav_censored_filename_not_allowed_list", "|"),

                "이동폴더포맷": ModelSetting.get("jav_censored_folder_format"),
                "메타사용": ModelSetting.get("jav_censored_use_meta"),
                "파일명변경": ModelSetting.get_bool("jav_censored_change_filename"),
                "분할파일처리": ModelSetting.get_bool("jav_censored_process_part_files"),
                "원본파일명포함여부": ModelSetting.get_bool("jav_censored_include_original_filename"),
                "원본파일명처리옵션": ModelSetting.get("jav_censored_include_original_filename_option"),

                "메타검색에공식사이트만사용": ModelSetting.get_bool("jav_censored_meta_dvd_use_dmm_only"),
                "메타매칭시이동폴더": ModelSetting.get("jav_censored_meta_dvd_path").strip(),
                "VR영상이동폴더": ModelSetting.get("jav_censored_meta_dvd_vr_path").strip(),
                "메타매칭실패시이동폴더": ModelSetting.get("jav_censored_meta_no_path").strip(),

                "메타매칭제외레이블": ModelSetting.get_list("jav_censored_meta_dvd_labels_exclude", ","),
                "메타매칭포함레이블": ModelSetting.get_list("jav_censored_meta_dvd_labels_include", ','),
                "배우조건매칭시이동폴더포맷": ModelSetting.get("jav_censored_folder_format_actor").strip(),
                "메타매칭실패시이동": ModelSetting.get_bool("jav_censored_meta_no_move"),
                "메타매칭실패시파일명변경": ModelSetting.get_bool("jav_censored_meta_no_change_filename"),

                "재시도": True,
                "방송": False,
                # 부가파일 생성 옵션 추가
                "부가파일생성_YAML": ModelSetting.get_bool("jav_censored_make_yaml"),
                "부가파일생성_NFO": ModelSetting.get_bool("jav_censored_make_nfo"),
                "부가파일생성_IMAGE": ModelSetting.get_bool("jav_censored_make_image"),

                # etc
                "파일당딜레이": ModelSetting.get_int("jav_censored_delay_per_file"),
            }
            # 2. 공통 실행 헬퍼 호출
            TaskBase.__task(config)

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
                    TaskBase.__task(job)
            except Exception as e:
                logger.error(f"YAML 파일 처리 중 오류 발생: {str(e)}")
                logger.error(traceback.format_exc())


    @staticmethod
    def __task(config):
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
            # logger.info(f"작업 [{config.get('이름', 'N/A')}] : 메타데이터 플러그인 설정을 따르도록 설정됨.")
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

        # 2. 파싱 규칙을 config에 추가
        Task._load_parsing_rules(config)

        # 3. 실제 Task 실행
        Task.start(config)


class SafeFormatter(string.Formatter):
    def get_value(self, key, args, kwargs):
        if isinstance(key, str):
            return kwargs.get(key, f'{{{key}}}')
        else:
            return super().get_value(key, args, kwargs)


class Task:
    config = None
    metadata_module = None

    @staticmethod
    def start(config):
        Task.config = config

        # 1-1. [Extract] 파일 목록 수집
        logger.debug(f"처리 파일 목록 생성")
        files = Task.__collect_initial_files()
        if not files:
            logger.info("처리할 파일이 없습니다.")
            return

        # 1-2. [Transform] 파싱 및 기본 정보 추출
        logger.debug(f"파싱 및 기본 정보 추출")
        execution_plan = []
        for file in files:
            parsing_rules = Task.config.get('파싱규칙')
            cleanup_list = Task.config.get('품번파싱제외키워드')
            info = Task.__prepare_initial_info(file, parsing_rules, cleanup_list, mode='censored')
            # ▲▲▲▲▲ [수정] 이 함수 내부 호출도 새 시그지처에 맞게 변경 ▲▲▲▲▲
            if info:
                execution_plan.append(info)
        
        # 1-3. [Enrichment] -c 파트 모호성 해결
        logger.debug(f"'-C' 파트 처리")
        Task.__resolve_c_part_ambiguity(execution_plan)

        # 1-4. [Assemble] 최종 파일명 및 경로 조립
        for info in execution_plan:
            Task.__assemble_final_plan(info)
            
        # 1-5. [Load] 메타데이터 처리 및 실제 파일 이동
        Task.__execute_plan(execution_plan)


    # ====================================================================
    # --- 헬퍼 함수들 (Helper Functions) ---
    # ====================================================================

    @staticmethod
    def _load_parsing_rules(config):
        """ metadata 플러그인에서 통합 파싱 규칙을 로드하여 config에 추가합니다."""
        try:
            meta_module = F.PluginManager.get_plugin_instance("metadata").get_module("jav_censored")
            if meta_module and hasattr(meta_module, 'get_jav_settings'):
                jav_settings = meta_module.get_jav_settings()
                parsing_rules = jav_settings.get('jav_parsing_rules', {})
                config['파싱규칙'] = parsing_rules
                if parsing_rules:
                    # 각 규칙 리스트의 길이를 계산하여 로그 메시지 생성
                    rule_counts = [f"{key}: {len(value)}개" for key, value in parsing_rules.items() if isinstance(value, list)]
                    logger.debug(f"메타데이터 플러그인에서 통합 파싱 규칙을 로드했습니다. (규칙 수: {', '.join(rule_counts)})")
                else:
                    logger.debug("메타데이터 플러그인에서 통합 파싱 규칙을 로드했으나, 규칙이 비어있습니다.")
            else:
                logger.warning("메타데이터 플러그인 또는 get_jav_settings 메서드를 찾을 수 없어 파싱 규칙을 로드하지 못했습니다.")
                config['파싱규칙'] = {}
        except Exception as e:
            logger.error(f"파싱 규칙 로드 중 오류: {e}")
            config['파싱규칙'] = {}


    @staticmethod
    def __collect_initial_files():
        """파일 시스템에서 처리할 초기 파일 목록을 수집합니다."""
        no_censored_path = Path(Task.config['처리실패이동폴더'].strip())
        if not no_censored_path.is_dir():
            logger.warning("'처리 실패시 이동 폴더'가 유효하지 않아 작업을 중단합니다.")
            return []

        src_list = Task.get_path_list(Task.config['다운로드폴더'])
        # 재시도 로직은 메타 검색 단계에서 별도로 처리하는 것이 더 좋으므로 여기서는 제외.

        all_files = []
        for src in src_list:
            ToolExpandFileProcess.preprocess_cleanup(src, Task.config.get('최소크기', 0), Task.config.get('최대기간', 0))
            _f = ToolExpandFileProcess.preprocess_listdir(src, no_censored_path, Task.config)
            all_files.extend(_f or [])

        # 파일 목록을 반환하기 전에 자연수 정렬
        if all_files:
            # logger.debug(f"총 {len(all_files)}개의 파일을 자연수 기준으로 정렬합니다.")
            all_files.sort(key=lambda p: [int(c) if c.isdigit() else c.lower() for c in re.split('([0-9]+)', p.name)])

        return all_files


    @staticmethod
    def __prepare_initial_info(file, parsing_rules, cleanup_list, mode='censored'):
        """파일을 파싱하여 메타 검색 이전에 가능한 모든 정보를 추출합니다."""

        parsed = ToolExpandFileProcess.parse_jav_filename(file.name, parsing_rules, cleanup_list, mode=mode)
        if not parsed:
            logger.warning(f"파싱 실패: {file.name}")
            return None

        # 파트 넘버를 미리 해석하여 저장
        parsed_part_type = ToolExpandFileProcess._parse_part_number(parsed['part'])

        info = {
            'original_file': file,
            'file_size': file.stat().st_size,
            'pure_code': parsed['code'],
            'raw_part': parsed['part'],

            'parsed_part_type': parsed_part_type,
            'ext': parsed['ext'],
            'meta_info': None,
        }

        logger.debug(
            f"- Parsed Code: '{info['pure_code']}', Part: '{info['parsed_part_type']}'"
        )

        return info


    @staticmethod
    def __resolve_c_part_ambiguity(plan_list):
        """전체 목록의 문맥을 사용하여 '-c' 파트의 모호성을 해결합니다."""
        # 품번별로 파일 인덱스를 그룹화하여 검색 속도 향상
        code_map = {}
        for i, info in enumerate(plan_list):
            code = info['pure_code']
            if code not in code_map:
                code_map[code] = []
            code_map[code].append(i)

        for i, info in enumerate(plan_list):
            # 'c' 파트이고, 아직 해석되지 않은 경우에만('cd3') 검사
            if info['raw_part'].strip(' ._-').lower() == 'c' and info['parsed_part_type'] == 'cd3':
                is_series = False
                # 동일 품번 그룹 내에서만 다른 파트를 찾음
                for other_index in code_map.get(info['pure_code'], []):
                    if i == other_index: continue
                    other_info = plan_list[other_index]
                    other_part = other_info['raw_part'].strip(' ._-').lower()
                    if other_part in ['a', 'b', 'd']:
                        is_series = True
                        logger.debug(f"{info['pure_code']}-c: 멀티 파트로 처리(cd3)")
                        break

                # 시리즈가 아닌 것으로 판명되면, 해석된 파트를 무효화
                if not is_series:
                    info['parsed_part_type'] = "" # 자막 등으로 간주하고 파트 정보 제거
                    logger.debug(f"{info['pure_code']}-c: 멀티 파트가 아닌 것으로 처리")


    @staticmethod
    def __assemble_final_plan(info):
        """최종 search_name, newfilename, target_dir을 조립합니다."""
        config = Task.config
        info['search_name'] = info['pure_code'] # search_name은 항상 순수 품번
        
        part_to_use = info['parsed_part_type'] if config.get('분할파일처리', True) else ""

        if not config.get('파일명변경', True):
            info['newfilename'] = info['original_file'].name
        elif part_to_use:
            info['newfilename'] = f"{info['search_name']}{part_to_use}{info['ext']}"
        else:
            base_filename = f"{info['search_name']}{info['ext']}"
            info['newfilename'] = Task.check_newfilename(
                info['original_file'].name, base_filename, str(info['original_file']), info['file_size']
            )


    @staticmethod
    def __execute_plan(execution_plan):
        """메타 검색 및 실제 파일 처리를 실행합니다."""
        config = Task.config

        # 품번 그룹화
        code_groups = {}
        for info in execution_plan:
            code = info['search_name']
            if code not in code_groups: code_groups[code] = []
            code_groups[code].append(info)

        logger.info(f"처리할 품번 그룹 {len(code_groups)}개 (총 파일 {len(execution_plan)}개)")
        delay_seconds = config.get('파일당딜레이', 0)

        total_files = len(execution_plan)
        processed_count = 0

        for idx, (pure_code, group_infos) in enumerate(code_groups.items()):
            if delay_seconds > 0 and idx > 0:
                time.sleep(delay_seconds)

            try:
                # 그룹당 1회 메타 검색
                meta_target_dir, meta_move_type, meta_info = None, None, None
                if config.get('메타사용') == 'using':
                    meta_target_dir, meta_move_type, meta_info = Task.__get_target_with_meta(pure_code)

                # 그룹 내 각 파일 처리
                for info in group_infos:
                    processed_count += 1
                    logger.info(f"[{processed_count:03d}/{total_files:03d}] {info['original_file'].name}")

                    # 메타 사용 여부에 따라 최종 이동 경로와 타입을 결정
                    if config.get('메타사용') == 'using':
                        target_dir = meta_target_dir
                        move_type = meta_move_type
                    else: # 메타 미사용 시
                        move_type = "normal"
                        target_paths = Task.get_path_list(config['라이브러리폴더'])
                        folders = Task.process_folder_format(move_type, pure_code)
                        target_dir = Path(target_paths[0]).joinpath(*folders)

                    # 실제 파일 이동/중복 처리 로직 호출
                    entity = Task.__file_move_logic(
                        info, 
                        info['newfilename'], 
                        target_dir, 
                        move_type, 
                        meta_info,
                        ModelJavCensoredItem
                    )
                    if entity and entity.move_type is not None:
                        entity.save()

            except Exception as e:
                logger.error(f"'{pure_code}' 그룹 처리 중 예외 발생: {e}")
                logger.error(traceback.format_exc())


    @staticmethod
    def __file_move_logic(info, newfilename, target_dir, move_type, meta_info, model_class):
        """실제 파일 이동, 중복 처리, DB 기록, 방송 등을 담당합니다."""
        config = Task.config
        file = info['original_file'] # 원본 파일 객체

        entity = model_class(config['이름'], str(file.parent), file.name)

        if move_type is None or target_dir is None:
            logger.warning(f"'{file.name}'의 최종 이동 경로를 결정할 수 없어 건너뜁니다.")
            return entity.set_move_type(None)

        if not target_dir.exists():
            target_dir.mkdir(parents=True)

        newfile = target_dir.joinpath(newfilename)

        # 1. 매칭 실패 (no_meta) 케이스 처리
        if move_type == "no_meta":
            if not config.get('메타매칭실패시이동', False):
                return entity.set_move_type(None)

            # no_meta의 경우 newfilename은 조립된 이름/원본명 중 하나.
            if newfile.exists():
                logger.warning(f"메타 없는 영상 이동 경로에 동일 파일이 존재하여 삭제합니다: {newfile}")
                file.unlink()
                return entity.set_move_type("no_meta_deleted_due_to_duplication")
            else:
                shutil.move(file, newfile)
                return entity.set_target(newfile).set_move_type(move_type)

        # 2. 타겟 경로 내 중복 처리
        if UtilFunc.is_duplicate(file, newfile):
            logger.info(f"타겟 경로에 동일 파일이 존재합니다: {newfile}")
            remove_path_str = config.get('중복파일이동폴더', '').strip()

            if not file.exists():
                return entity.set_move_type("already_processed_by_other")

            if not remove_path_str:
                file.unlink()
                logger.info("중복 파일을 삭제했습니다.")
                return entity.set_move_type(move_type + "_deleted_due_to_duplication")
            else:
                remove_path = Path(remove_path_str)
                if not remove_path.exists(): remove_path.mkdir(parents=True)

                final_dup_path = remove_path.joinpath(newfilename)
                if final_dup_path.exists():
                    timestamp = int(datetime.now().timestamp())
                    unique_filename = f"[{timestamp}] {newfilename}"
                    final_dup_path = remove_path.joinpath(unique_filename)

                shutil.move(file, final_dup_path)
                logger.info(f"중복 파일을 다음 경로로 이동했습니다: {final_dup_path}")
                return entity.set_target(final_dup_path).set_move_type(move_type + "_already_exist")

        # 3. 최종 라이브러리로 이동
        if file.exists():
            move_success = False
            try:
                # 3a. shutil.move만 단독으로 예외 처리
                shutil.move(file, newfile)
                logger.info(f"파일 이동 성공: -> {newfile}")
                move_success = True
            except Exception as e:
                logger.error(f"최종 파일 이동 중 오류 발생: {file} -> {newfile}")
                logger.error(f"오류: {e}")
                logger.error(traceback.format_exc())
                return entity.set_move_type("move_fail")

            # 3b. 파일 이동이 성공했을 경우에만 후속 작업 실행
            if move_success:
                # 부가 파일 (NFO, YAML, 이미지) 생성
                if meta_info:
                    TaskMakeYaml.make_files(
                        meta_info,
                        str(newfile.parent),
                        make_yaml=config.get('부가파일생성_YAML', False),
                        make_nfo=config.get('부가파일생성_NFO', False),
                        make_image=config.get('부가파일생성_IMAGE', False),
                    )

                # 방송 처리
                try:
                    if config.get('방송', False):
                        bot = {
                            't1': 'gds_tool', 't2': 'fp', 't3': 'av',
                            'data': {'gds_path': str(newfile).replace('/mnt/AV/MP/GDS', '/ROOT/GDRIVE/VIDEO/AV')}
                        }
                        hook = base64.b64decode(b'aHR0cHM6Ly9kaXNjb3JkLmNvbS9hcGkvd2ViaG9va3MvMTM5OTkxMDg4MDE4NzEyNTgxMS84SFY0bk93cGpXdHhIdk5TUHNnTGhRbDhrR3lGOXk4THFQQTdQVTBZSXVvcFBNN21PWHhkSVJSNkVmcmIxV21UdFhENw==').decode('utf-8')
                        SupportDiscord.send_discord_bot_message(json.dumps(bot), hook)
                except Exception as e:
                    logger.error(f"방송 메시지 전송 실패: {e}")

                return entity.set_target(newfile).set_move_type(move_type)

        return entity.set_move_type(None) # 파일이 중간에 사라진 경우 등


    # ====================================================================
    # --- Legacy functions ---
    # ====================================================================

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
                #logger.debug(f"메타 검색 시도: 사이트=[{site}], 검색어=[{search_name}]")
                search_result_list = meta_module.search2(search_name, site, manual=False)

                if not search_result_list:
                    continue

                # 95점 이상인 첫 번째 결과를 찾으면 바로 사용
                best_match = next((item for item in search_result_list if item.get('score', 0) >= 95), None)

                if best_match:
                    logger.info(f"매칭 성공! 사이트=[{site}], 검색어=[{search_name}], 코드=[{best_match['code']}], 점수=[{best_match['score']}]")
                    # metadata 플러그인의 info 메소드 직접 호출
                    meta_info = meta_module.info(best_match["code"], keyword=search_name, fp_meta_mode=True)

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

                        #logger.info(f"메타매칭 최종 성공: {meta_info['code']}")
                        return current_target_root.joinpath(*folders), meta_info
                    else:
                        logger.warning(f"검색은 성공했으나 메타 정보({best_match['code']})를 가져오지 못했습니다. 다음 사이트를 검색합니다.")
                        pass

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
    def process_folder_format(meta_type, meta_info):
        # 1. 사용할 폴더 포맷을 config에서 가져옴
        folder_format = Task.config.get('이동폴더포맷', '').strip()
        if not folder_format:
            return []

        data = {}
        number_part_raw = ''

        # 2. meta_type에 따라 기본 data 딕셔너리와 number_part_raw를 생성
        if meta_type in ["default", "override", "meta_fail", "normal", "no_meta"]:
            # 메타 정보가 없는 경우 (품번 문자열만 있음)
            code_parts = meta_info.split('-', 1)
            label_part = code_parts[0]
            number_part_raw = code_parts[1] if len(code_parts) > 1 else ''
            data = {
                "code": meta_info.upper(),
                "label": label_part.upper(),
                "label_1": label_part[0].upper() if label_part else '#',
                "year": "", "actor": "", "studio": "NO_STUDIO",
            }

        elif meta_type in ["dvd", "meta_success"]:
            # 메타 정보가 있는 경우
            original_title = meta_info.get("originaltitle", "")
            code_parts = original_title.split('-', 1)
            label_part = code_parts[0]
            number_part_raw = code_parts[1] if len(code_parts) > 1 else ''
            
            # 메타 정보로 data 딕셔너리 채우기
            actor_names = [actor.get('name', '') for actor in meta_info.get('actor', [])[:3]]
            actor_names = [name for name in actor_names if name and name.strip()]
            data = {
                "studio": meta_info.get("studio", "NO_STUDIO"),
                "code": original_title,
                "label": label_part,
                "label_1": label_part[0] if label_part else '#',
                "actor": f"{','.join(actor_names[:1])}",
                "actor_2": f"{','.join(actor_names[:2])}",
                "actor_3": f"{','.join(actor_names[:3])}",
                "year": meta_info.get("year", ""),
            }
        else:
            return []

        # 3. 새로운 포맷 변수 처리
        try:
            # {num_X_Y} 변수 처리
            num_matches = re.findall(r'\{num_(\d+)_(\d+)\}', folder_format)
            if num_matches and number_part_raw:
                main_number_part = re.split(r'[-_\s]', number_part_raw, 1)[0]
                for x_str, y_str in set(num_matches):
                    x, y = int(x_str), int(y_str)
                    data[f'num_{x}_{y}'] = main_number_part.zfill(y)[:x]

            # {year4} 변수 처리
            if '{year4}' in folder_format and number_part_raw:
                year4_value = ''
                main_number_part = re.split(r'[-_\s]', number_part_raw, 1)[0]
                if re.match(r'^\d{6}$', main_number_part):
                    yy = main_number_part[4:6]
                    year4_value = f"20{yy}" if int(yy) < 50 else f"19{yy}"
                data['year4'] = year4_value
        except Exception as e:
            logger.error(f"폴더 포맷 변수 처리 중 오류: {e}")

        # 4. 안전한 포맷팅 실행
        safe_fmt = SafeFormatter()
        folders = safe_fmt.format(folder_format, **data)

        # 5. 후처리
        folders = re.sub(r'\{.*?\}', '', folders)
        folders = folders.replace("[]", "").replace("{}", "").replace("()", "")
        folders = re.sub(r'\s{2,}', ' ', folders).strip()
        final_parts = folders.split("/")
        if folders.startswith('/'):
            return [''] + [part for part in final_parts[1:] if part]
        else:
            return [part for part in final_parts if part]


    @staticmethod
    def check_newfilename(filename, newfilename, file_path, file_size=None):
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
            newfilename = Task.parse_jav_filename_by_save_original(filename, newfilename, file_path)
        elif filename != newfilename and (
            (filename.find("[") == -1 or filename.find("]") == -1)
            or not os.path.splitext(filename)[0].startswith(os.path.splitext(newfilename)[0])
        ):
            newfilename = Task.parse_jav_filename_by_save_original(filename, newfilename, file_path, file_size)
        else:
            # 이미 한번 파일처리를 한것으로 가정하여 변경하지 않는다.
            newfilename = filename
            # 기존에 cd1 [..].mp4 는 []를 제거한다
            match = re.search(r"cd\d(?P<remove>\s\[.*?\])", newfilename)
            if match:
                newfilename = newfilename.replace(match.group("remove"), "")

        #logger.debug("%s => %s", filename, newfilename)
        return newfilename


    @staticmethod
    def parse_jav_filename_by_save_original(original_filename, new_filename, original_filepath, file_size=None):
        """원본파일명 보존 옵션에 의해 파일명을 변경한다."""
        try:
            if not Task.config.get('원본파일명포함여부', True):
                return new_filename

            option = Task.config.get('원본파일명처리옵션', 'original')

            size_required_options = ["original_bytes", "original_giga", "bytes"]
            if option in size_required_options and file_size is None:
                logger.error(f"{original_filename}: 파일 크기 정보가 없어서 파일명 변경을 건너뜁니다.")
                # 파일명 변경 실패로 간주하고, 원본 파일명을 포함하기 전의
                # 기본 파일명(예: ABC-123.mp4)을 반환
                return new_filename 

            new_name, new_ext = os.path.splitext(new_filename)
            ori_name, _ = os.path.splitext(original_filename)
            ori_name = ori_name.replace("[", "(").replace("]", ")").strip()

            # --- os.stat()을 완전히 제거하고 file_size 인자만 사용 ---
            if option == "original":
                return f"{new_name} [{ori_name}]{new_ext}"
            if option == "original_bytes":
                return f"{new_name} [{ori_name}({file_size})]{new_ext}"
            if option == "original_giga":
                str_size = SupportUtil.sizeof_fmt(file_size, suffix="B")
                return f"{new_name} [{ori_name}({str_size})]{new_ext}"
            if option == "bytes":
                return f"{new_name} [{file_size}]{new_ext}"

            # 위에서 처리되지 않은 옵션이 있을 경우의 기본 폴백
            return f"{new_name} [{ori_name}]{new_ext}"
        except Exception as exception:
            logger.error("Exception:%s", exception)
            logger.error(traceback.format_exc())
            return new_filename


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



