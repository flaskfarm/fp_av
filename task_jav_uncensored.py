from .setup import *
from pathlib import Path
import base64, json
import traceback
import re
import os
import shutil
import time
from datetime import datetime

ModelSetting = P.ModelSetting
from .task_jav_censored import Task as CensoredTask
from .tool import ToolExpandFileProcess, UtilFunc
from .model_jav_uncensored import ModelJavUncensoredItem
from support import SupportYaml, SupportUtil, SupportDiscord
from .task_make_yaml import Task as TaskMakeYaml


class TaskBase:
    @F.celery.task
    def start(*args):
        logger.info(args)
        job_type = args[0]

        if job_type == 'default':
            config = {
                "이름": "default",
                "사용": True,
                # 기본 탭
                "다운로드폴더": ModelSetting.get_list("jav_uncensored_download_path"),
                "최소크기": ModelSetting.get_int("jav_uncensored_min_size"),
                "최대기간": ModelSetting.get_int("jav_uncensored_max_age"),
                "품번파싱제외키워드": ModelSetting.get_list("jav_uncensored_filename_cleanup_list", "|"),
                "파일처리하지않을파일명": ModelSetting.get_list("jav_uncensored_filename_not_allowed_list", "|"),
                "파일당딜레이": ModelSetting.get_int("jav_uncensored_delay_per_file"),
                # 파일명 탭
                "파일명변경": ModelSetting.get_bool("jav_uncensored_change_filename"),
                "분할파일처리": ModelSetting.get_bool("jav_uncensored_process_part_files"),
                "원본파일명포함여부": ModelSetting.get_bool("jav_uncensored_include_original_filename"),
                "원본파일명처리옵션": ModelSetting.get("jav_uncensored_include_original_filename_option"),
                # 폴더구조 탭
                "이동폴더포맷": ModelSetting.get("jav_uncensored_folder_format"),
                "처리실패이동폴더": ModelSetting.get("jav_uncensored_temp_path").strip(),
                "중복파일이동폴더": ModelSetting.get("jav_uncensored_remove_path").strip(),
                "사용자지정레이블폴더": ModelSetting.get("jav_uncensored_label_path_overrides"),
                "메타사용": ModelSetting.get("jav_uncensored_use_meta"),
                "메타매칭시이동폴더": ModelSetting.get("jav_uncensored_meta_path").strip(),
                "메타매칭실패시이동폴더": ModelSetting.get("jav_uncensored_meta_no_path").strip(),
                "메타매칭실패시파일명변경": ModelSetting.get_bool("jav_uncensored_meta_no_change_filename"),
                "매칭실패재시도주기": ModelSetting.get_int("jav_uncensored_meta_no_retry_every"),
                "라이브러리폴더": ModelSetting.get("jav_uncensored_target_path").strip(),

                "재시도": True,
                "방송": False,
                "부가파일생성_YAML": ModelSetting.get_bool("jav_uncensored_make_yaml"),
                "부가파일생성_NFO": ModelSetting.get_bool("jav_uncensored_make_nfo"),
                "부가파일생성_IMAGE": ModelSetting.get_bool("jav_uncensored_make_image"),
                "PLEXMATE스캔": ModelSetting.get_bool("jav_uncensored_scan_with_plex_mate"),
            }

            config['PLEXMATE_URL'] = F.SystemModelSetting.get('ddns')

            TaskBase.__task(config)

        elif job_type == 'yaml':
            try:
                yaml_filepath = ModelSetting.get('jav_uncensored_yaml_filepath') 
                if not yaml_filepath:
                    logger.error("YAML 파일 경로가 설정되지 않았습니다.")
                    return

                yaml_data = SupportYaml.read_yaml(yaml_filepath)
                for job in yaml_data.get('작업', []):
                    if not job.get('사용', True):
                        continue
                    job['재시도'] = False

                    # 공통 실행 헬퍼 호출 (각 job에 대해)
                    TaskBase.__task(job)
            except Exception as e:
                logger.error(f"YAML 파일 처리 중 오류 발생: {str(e)}")
                logger.error(traceback.format_exc())


    @staticmethod
    def __task(config):
        # 1. 공용 헬퍼 함수를 호출하여 파싱 규칙 로드
        CensoredTask._load_parsing_rules(config)
        
        # 2. Uncensored 전용 설정 로드 (메타 검색 지원 레이블)
        try:
            meta_module = Task.get_meta_module() # jav_uncensored 메타 모듈
            if meta_module and hasattr(meta_module, 'site_map'):
                supported_labels = [
                    v['keyword'][0] for v in meta_module.site_map.values() if v.get('keyword')
                ]
                config['메타검색지원레이블'] = supported_labels
            else:
                config['메타검색지원레이블'] = []
        except Exception as e:
            logger.error(f"메타데이터 모듈에서 지원 레이블 목록을 가져오는 데 실패했습니다: {e}")
            config['메타검색지원레이블'] = []

        Task.start(config)


class Task:
    config = None
    metadata_module = None

    @staticmethod
    def start(config):
        Task.config = config
        CensoredTask.config = config

        logger.debug("처리 파일 목록 생성")
        files = Task.__collect_initial_files()
        if not files:
            logger.info("처리할 파일이 없습니다.")
            return

        # logger.debug("파싱 및 처리 계획 수립")
        execution_plan = []
        no_label_files = [] # 파싱에 실패한 파일들을 담을 리스트

        parsing_rules = Task.config.get('파싱규칙')
        cleanup_list = Task.config.get('품번파싱제외키워드')

        for file in files:
            info = CensoredTask.__prepare_initial_info(file, parsing_rules, cleanup_list, mode='uncensored')
            if info:
                # 파싱 성공 -> 실행 계획에 추가
                execution_plan.append(info)
            else:
                # 파싱 실패 -> 실패 목록에 추가
                no_label_files.append(file)

        logger.debug(f"파싱 성공: {len(execution_plan)}개, 파싱 실패(NO LABEL): {len(no_label_files)}개")

        if no_label_files:
            # logger.debug("파싱 실패 파일 이동 시작")
            for file in no_label_files:
                Task.__move_to_no_label_folder(file)

        if not execution_plan:
            logger.debug("파싱에 성공한 파일이 없어 작업을 종료합니다.")
            return

        # logger.debug("'-C' 파트 처리")
        CensoredTask.__resolve_c_part_ambiguity(execution_plan)

        # logger.debug("최종 파일명 조립")
        for info in execution_plan:
            CensoredTask.__assemble_final_plan(info)

        logger.debug("메타 검색 및 파일 이동 시작")
        Task.__execute_plan(execution_plan)


    # ====================================================================
    # --- Uncensored 전용 헬퍼 ---
    # ====================================================================


    @staticmethod
    def __collect_initial_files():
        config = Task.config
        no_censored_path = Path(config['처리실패이동폴더'].strip())
        if not no_censored_path.is_dir():
            logger.warning("'처리 실패시 이동 폴더'가 유효하지 않아 작업을 중단합니다.")
            return []

        src_list = CensoredTask.get_path_list(config['다운로드폴더'])
        src_list.extend(Task.__add_meta_no_path()) # 매칭 실패 재시도 경로 추가

        all_files = []
        for src in src_list:
            ToolExpandFileProcess.preprocess_cleanup(src, config.get('최소크기', 0), config.get('최대기간', 0))
            _f = ToolExpandFileProcess.preprocess_listdir(src, no_censored_path, config)
            all_files.extend(_f or [])

        if all_files:
            all_files.sort(key=lambda p: [int(c) if c.isdigit() else c.lower() for c in re.split('([0-9]+)', p.name)])
        return all_files


    @staticmethod
    def __move_to_no_label_folder(file_path: Path):
        """품번 추출에 실패한 파일을 지정된 폴더로 이동시킵니다."""
        config = Task.config
        target_root_str = config.get('품번추출실패시이동경로', '').strip()

        if not target_root_str:
            target_root_str = config.get('처리실패이동폴더', '').strip()
            if not target_root_str: return
            target_dir = Path(target_root_str).joinpath("[NO LABEL]")
        else:
            target_dir = Path(target_root_str)

        try:
            target_dir.mkdir(parents=True, exist_ok=True)
            target_file = target_dir.joinpath(file_path.name)
            if target_file.exists():
                target_file = target_file.with_name(f"[{int(time.time())}] {file_path.name}")
            shutil.move(file_path, target_file)
            logger.info(f"품번 추출 실패 파일을 이동: {target_file}")
        except Exception as e:
            logger.error(f"{file_path.name} 이동 중 오류: {e}")


    @staticmethod
    def __execute_plan(execution_plan):
        config = Task.config
        code_groups = {}
        for info in execution_plan:
            code_groups.setdefault(info['search_name'], []).append(info)

        total_files = len(execution_plan)
        logger.info(f"처리할 품번 그룹 {len(code_groups)}개 (총 파일 {total_files}개)")

        scan_enabled = config.get("PLEXMATE스캔", False)
        if config.get('메타사용') == 'using' or scan_enabled:
            delay = config.get('파일당딜레이', 0)
        else:
            delay = 0
        processed_count = 0
        last_scan_path = None

        for idx, (pure_code, group_infos) in enumerate(code_groups.items()):
            if delay > 0 and idx > 0: time.sleep(delay)
            
            try:
                # 1. 그룹 대표 정보로 경로의 '기반'과 '타입'을 한 번만 결정
                path_result, move_type, meta_info = Task.__determine_target_path_and_meta(group_infos[0])

                if path_result is None:
                    continue

                # 2. move_type에 따라 최종 target_dir을 올바르게 계산
                target_dir = None
                if move_type == "override_final":
                    target_dir = path_result
                elif move_type in ["meta_fail", "no_rule", "no_label"]:
                    target_dir = path_result
                else:
                    format_type = "dvd" if move_type == "meta_success" else "normal"
                    folders = CensoredTask.process_folder_format(format_type, meta_info or pure_code)
                    target_dir = path_result.joinpath(*folders)

                # 3. 스캔 경로 변경 감지 및 요청
                if scan_enabled and target_dir != last_scan_path and last_scan_path is not None:
                    logger.info(f"경로 변경 감지. 이전 경로 스캔 요청: {last_scan_path}")
                    CensoredTask.__request_plex_mate_scan(last_scan_path) # CensoredTask의 함수 호출

                # 4. 그룹 내 각 파일 처리
                for info in group_infos:
                    processed_count += 1
                    logger.info(f"[{processed_count:03d}/{total_files:03d}] {info['original_file'].name}")

                    info['target_dir'] = target_dir
                    info['move_type'] = move_type
                    info['meta_info'] = meta_info
                    
                    # CensoredTask의 공유 함수를 호출
                    entity = CensoredTask.__file_move_logic(info, ModelJavUncensoredItem)

                    if entity and entity.move_type is not None:
                        entity.save()

                # 5. 이동 성공 시, 현재 그룹의 경로를 '마지막 스캔 경로'로 업데이트
                if scan_enabled and target_dir is not None:
                    last_scan_path = target_dir

            except Exception as e:
                logger.error(f"'{pure_code}' 그룹 처리 중 예외: {e}")
                logger.error(traceback.format_exc())
                processed_count += len(group_infos)

        # 6. 모든 루프가 끝난 후, 마지막으로 처리된 경로에 대해 스캔을 요청
        if scan_enabled and last_scan_path is not None:
            logger.info(f"모든 파일 처리 완료. 마지막 경로 스캔 요청: {last_scan_path}")
            CensoredTask.__request_plex_mate_scan(last_scan_path)


    @staticmethod
    def __determine_target_path_and_meta(info):
        """사용자가 정의한 최종 우선순위 규칙에 따라 루트 폴더, 이동 타입, 메타 정보를 결정합니다."""
        config = Task.config
        pure_code = info.get('pure_code')

        if not pure_code:
            logger.warning(f"처리 정보에 품번이 없습니다: {info.get('original_file')}")
            return None, None, None

        label = pure_code.split('-')[0].lower()

        # [규칙 1] 사용자 지정 레이블 경로
        overrides_map = Task.__parse_key_value_text(config.get('사용자지정레이블폴더', ''))
        if label in overrides_map:
            user_defined_format = overrides_map[label]

            temp_config = config.copy()
            temp_config['이동폴더포맷'] = user_defined_format

            CensoredTask.config = temp_config
            folders = CensoredTask.process_folder_format("override", pure_code)
            CensoredTask.config = config # config 원상 복구

            final_path = Path('/'.join(folders)) if folders and folders[0] == '' else Path(*folders)
            logger.debug(f"'{label}' -> 사용자 지정 경로: {final_path}")

            return final_path, "override_final", None

        # [규칙 2] 메타 사용
        if config.get('메타사용') == 'using':
            supported_labels = config.get('메타검색지원레이블', [])

            if label in supported_labels:
                # (YES) 메타 검색을 시도합니다.
                logger.debug(f"'{label}' -> 메타 검색 시도")
                meta_info = Task.__search_meta(pure_code)

                if meta_info:
                    # (성공) -> '메타매칭시이동폴더'
                    target_path_str = config.get('메타매칭시이동폴더')
                    if not target_path_str:
                        logger.warning(f"'매칭 성공 시 이동 폴더'가 설정되지 않아 '{pure_code}'의 경로를 결정할 수 없습니다.")
                    else:
                        return Path(target_path_str), "meta_success", meta_info
                else:
                    # (실패) -> '메타매칭실패시이동폴더'
                    target_path_str = config.get('메타매칭실패시이동폴더')
                    if not target_path_str:
                        # Fallback: 처리실패이동폴더/[NO META]
                        return Path(config['처리실패이동폴더']).joinpath("[NO META]"), "meta_fail", None
                    return Path(target_path_str), "meta_fail", None

        # [규칙 3] 위 모든 조건에 해당하지 않는 경우 -> '라이브러리폴더'
        # (사용자 지정X, 메타사용OFF 또는 메타 미지원 레이블)
        target_path_str = config.get('라이브러리폴더')
        if target_path_str:
            return Path(target_path_str), "default", None

        # 모든 경로 설정이 비어있어 최종 목적지를 결정할 수 없는 경우
        logger.warning(f"'{pure_code}'에 대한 처리 규칙(사용자지정/메타/기본)이 모두 비어있어 이동하지 않습니다.")
        return Path(config['처리실패이동폴더']).joinpath("[NO RULE MATCH]"), "no_rule", None


    @staticmethod
    def __parse_key_value_text(text_data: str) -> dict:
        """'키:값' 형식의 여러 줄 텍스트를 파싱하여 딕셔너리로 반환합니다."""
        if not text_data:
            return {}
        
        result = {}
        for line in text_data.strip().splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            
            parts = [part.strip() for part in line.split(':', 1)]
            if len(parts) == 2 and parts[0]:
                key = parts[0].lower()
                value = parts[1]
                result[key] = value
        return result


    @staticmethod
    def __search_meta(pure_code):
        """메타 검색 헬퍼 함수"""
        try:
            meta_module = Task.get_meta_module()
            if not meta_module: return None
            search_result = meta_module.search(pure_code, manual=False)
            best_match = next((item for item in search_result if item.get('score', 0) >= 95), None)
            if best_match:
                return meta_module.info(best_match["code"])
        except Exception as e:
            logger.error(f"'{pure_code}' 메타 검색 중 예외: {e}")
        return None


    @staticmethod
    def __add_meta_no_path():
        """매칭 실패 영상을 주기적으로 재시도하기 위해 스캔 목록에 추가합니다."""
        config = Task.config
        meta_no_path_str = config.get('매칭실패시이동경로', '').strip()
        if not meta_no_path_str: return []

        meta_no_path = Path(meta_no_path_str)
        if not meta_no_path.is_dir(): return []

        retry_every = config.get("매칭실패재시도주기", 0)
        if retry_every <= 0: return []
        
        last_retry_str = ModelSetting.get("jav_uncensored_meta_no_last_retry")
        if (datetime.now() - datetime.fromisoformat(last_retry_str)).days < retry_every: return []
        
        ModelSetting.set("jav_uncensored_meta_no_last_retry", datetime.now().isoformat())
        logger.info(f"매칭 실패 영상 재시도 폴더를 스캔 목록에 추가: {meta_no_path}")
        return [meta_no_path]


    # ====================================================================
    # --- 유틸리티 및 레거시 함수들 ---
    # ====================================================================


    @staticmethod
    def get_meta_module():
        """Uncensored 전용 메타데이터 모듈 로더"""
        try:
            if Task.metadata_module is None:
                Task.metadata_module = F.PluginManager.get_plugin_instance("metadata").get_module("jav_uncensored")
            return Task.metadata_module
        except Exception as e:
            logger.debug(f"Exception: {e}")
            logger.debug(traceback.format_exc())
            return None


