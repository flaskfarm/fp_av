from .setup import *
from pathlib import Path
import base64, json
import traceback
import re
import os
import shutil
import time
from datetime import datetime
import shlex

ModelSetting = P.ModelSetting
from .task_jav_censored import Task as CensoredTask
from .task_jav_censored import TaskBase as CensoredTaskBase
from .tool import ToolExpandFileProcess, UtilFunc, SafeFormatter
from .model_jav_uncensored import ModelJavUncensoredItem
from support import SupportYaml, SupportUtil, SupportDiscord
from .task_make_yaml import Task as TaskMakeYaml


class TaskBase:
    @F.celery.task
    def start(*args):
        logger.info(args)
        job_type = args[0]

        config = {
            "이름": job_type,
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
            "파일명에미디어정보포함": ModelSetting.get_bool("jav_uncensored_include_media_info_in_filename"),
            "분할파일처리": ModelSetting.get_bool("jav_uncensored_process_part_files"),
            "원본파일명포함여부": ModelSetting.get_bool("jav_uncensored_include_original_filename"),
            "원본파일명처리옵션": ModelSetting.get("jav_uncensored_include_original_filename_option"),
            # 폴더구조 탭
            "이동폴더포맷": ModelSetting.get("jav_uncensored_folder_format"),
            "처리실패이동폴더": ModelSetting.get("jav_uncensored_temp_path").strip(),
            "중복파일이동폴더": ModelSetting.get("jav_uncensored_remove_path").strip(),
            "메타사용": ModelSetting.get("jav_uncensored_use_meta"),
            "메타매칭시이동폴더": ModelSetting.get("jav_uncensored_meta_path").strip(),
            "메타매칭실패시이동폴더": ModelSetting.get("jav_uncensored_meta_no_path").strip(),
            "메타매칭실패시파일명변경": ModelSetting.get_bool("jav_uncensored_meta_no_change_filename"),
            "매칭실패재시도주기": ModelSetting.get_int("jav_uncensored_meta_no_retry_every"),
            "라이브러리폴더": ModelSetting.get("jav_uncensored_target_path").splitlines(),

            "재시도": True,
            "방송": False,
            "부가파일생성_YAML": ModelSetting.get_bool("jav_uncensored_make_yaml"),
            "부가파일생성_NFO": ModelSetting.get_bool("jav_uncensored_make_nfo"),
            "부가파일생성_IMAGE": ModelSetting.get_bool("jav_uncensored_make_image"),
            "PLEXMATE스캔": ModelSetting.get_bool("jav_uncensored_scan_with_plex_mate"),
            "드라이런": ModelSetting.get_bool("jav_uncensored_dry_run"),
            'PLEXMATE_URL': F.SystemModelSetting.get('ddns'),
        }

        config['parse_mode'] = 'uncensored'
        CensoredTask._load_extended_settings(config)

        base_config_with_advanced = config

        if job_type in ['default', 'dry_run']:
            final_config = base_config_with_advanced.copy()
            final_config["이름"] = job_type
            if final_config.get('드라이런', False):
                logger.warning(f"'{final_config['이름']}' 작업: Dry Run 모드가 활성화되었습니다.")

            TaskBase.__task(final_config)

        elif job_type == 'yaml':
            yaml_filepath = args[1]
            try:
                yaml_data = SupportYaml.read_yaml(yaml_filepath)
                for job in yaml_data.get('작업', []):
                    if not job.get('사용', True): continue

                    final_config = base_config_with_advanced.copy()
                    user_subbed_override = job.get('자막우선처리', None)
                    final_config.update(job)

                    if user_subbed_override is not None and isinstance(user_subbed_override, dict):
                        final_config['자막우선처리'] = {**base_config_with_advanced.get('자막우선처리',{}), **user_subbed_override}

                    if final_config.get('드라이런', False):
                        logger.warning(f"'{final_config.get('이름', 'YAML Job')}' 작업: Dry Run 모드가 활성화되었습니다.")

                    if '커스텀경로규칙' in job:
                        if isinstance(job['커스텀경로규칙'], list):
                            logger.debug("작업 YAML에 정의된 '커스텀경로규칙'을 직접 적용하고, 기능을 활성화합니다.")
                            final_config['커스텀경로활성화'] = True
                        else:
                            logger.warning("작업 YAML의 '커스텀경로규칙'이 리스트 형식이 아니므로 무시합니다.")

                    elif 'meta_custom_path' in job:
                        logger.debug("작업 YAML에 정의된 meta_custom_path 설정을 파싱하여 적용합니다.")
                        custom_path_section = job['meta_custom_path']
                        current_module = final_config.get('parse_mode')
                        is_enabled, rules = CensoredTask._parse_custom_path_rules(custom_path_section, current_module)
                        final_config['커스텀경로활성화'] = is_enabled
                        final_config['커스텀경로규칙'] = rules

                    TaskBase.__task(final_config)
            except Exception as e:
                logger.error(f"YAML 파일 처리 중 오류 발생: {e}")


    @staticmethod
    def __task(config):
        config['module_name'] = 'jav_uncensored'

        try:
            meta_module = CensoredTask.get_meta_module('jav_uncensored')
            if meta_module and hasattr(meta_module, 'site_map'):
                supported_labels = [v['keyword'][0].lower() for v in meta_module.site_map.values() if v.get('keyword')]
                config['메타검색지원레이블'] = set(supported_labels)
            else:
                config['메타검색지원레이블'] = set()
        except Exception as e:
            logger.error(f"메타데이터 모듈에서 지원 레이블 목록 로드 실패: {e}")
            config['메타검색지원레이블'] = set()

        Task.start(config)


class Task:
    metadata_module = None


    @staticmethod
    def start(config):

        task_context = {
            'module_name': 'jav_uncensored',
            'parse_mode': 'uncensored',
            'execute_plan': Task.__execute_plan,
            'db_model': ModelJavUncensoredItem,
            'get_target_path_function': Task.__get_target_path,
        }

        CensoredTask.__start_shared_logic(config, task_context)


    # ====================================================================
    # --- Uncensored 전용 헬퍼 ---
    # ====================================================================


    @staticmethod
    def __get_target_path(config, info):
        """
        Uncensored 전용 경로 결정 로직.
        """
        meta_info = None
        move_type = "default"

        # --- 1. 커스텀 경로 규칙을 최우선으로 확인 ---
        if config.get('커스텀경로활성화', False):
            custom_rules = config.get('커스텀경로규칙', [])
            # 커스텀 규칙은 항상 파일 원본의 레이블로 먼저 매칭 시도
            matched_rule = CensoredTask._find_and_merge_custom_path_rules(info, custom_rules)

            if matched_rule:
                # 커스텀 규칙이 매칭되었다면, 메타 검색은 선택적으로 수행
                if config.get('메타사용') == 'using' and info['label'].lower() in config.get('메타검색지원레이블', set()):
                    meta_module = CensoredTask.get_meta_module('jav_uncensored')
                    meta_info = Task.__search_meta(config, meta_module, info['pure_code'])

                # 메타 실패 시 강제 적용 옵션 확인
                if not meta_info and not (matched_rule.get('force_on_meta_fail') or matched_rule.get('메타실패시강제적용')):
                    # 강제 적용 옵션이 없고 메타에 실패하면, 커스텀 규칙을 무시하고 일반 실패 로직으로 넘어감
                    pass
                else:
                    # 커스텀 규칙 적용 확정
                    move_type = "custom_path"
                    target_root_str = (matched_rule.get('path') or matched_rule.get('경로', '')).strip()
                    folder_format_to_use = (matched_rule.get('format') or matched_rule.get('폴더포맷')) or config['이동폴더포맷']

                    # 경로가 규칙에 없으면, 상황에 맞는 기본 경로를 사용
                    if not target_root_str:
                        if meta_info:
                            target_root_str = config.get('메타매칭시이동폴더')
                        else: # 메타 실패(강제적용) 또는 메타 미사용
                            library_paths = CensoredTask.get_path_list(config.get('라이브러리폴더', []))
                            target_root_str = library_paths[0] if library_paths else None

                    if not target_root_str:
                        logger.error("커스텀 규칙에 따른 이동 경로를 결정할 수 없습니다.")
                        return Path(config['처리실패이동폴더']).joinpath("[NO CUSTOM PATH]"), "error", None

                    folders = CensoredTask.process_folder_format(config, info, folder_format_to_use, meta_info)
                    return Path(target_root_str).joinpath(*folders), move_type, meta_info

        # --- 2. (커스텀 규칙 미적용 시) 일반 경로 결정 ---

        # 메타 사용 모드일 때만 메타 검색 실행
        if config.get('메타사용') == 'using':
            if info['label'].lower() in config.get('메타검색지원레이블', set()):
                meta_module = CensoredTask.get_meta_module('jav_uncensored')
                meta_info = Task.__search_meta(config, meta_module, info['pure_code'])

            if meta_info:
                move_type = "meta_success"
                target_root_path = Path(config.get('메타매칭시이동폴더'))
                folders = CensoredTask.process_folder_format(config, info, config['이동폴더포맷'], meta_info)
                return target_root_path.joinpath(*folders), move_type, meta_info
            else:
                move_type = "meta_fail"
                target_root_path = Path(config.get('메타매칭실패시이동폴더'))
                if target_root_path.name:
                    return target_root_path, move_type, meta_info
                # 메타 실패 폴더가 없으면 아래 라이브러리 폴더 로직으로 넘어감

        # 메타 미사용 모드 또는 위에서 경로 결정이 안 된 모든 경우
        move_type = "default"
        library_paths = CensoredTask.get_path_list(config.get('라이브러리폴더', []))
        if not library_paths:
            logger.error("이동할 라이브러리 폴더가 설정되지 않았습니다.")
            return Path(config['처리실패이동폴더']).joinpath("[NO TARGET PATH]"), "error", None

        target_root_path = Path(library_paths[0])
        folders = CensoredTask.process_folder_format(config, info, config['이동폴더포맷'], meta_info) # meta_info는 None일 것
        return target_root_path.joinpath(*folders), move_type, meta_info


    @staticmethod
    def __execute_plan(config, execution_plan, db_model, task_context=None):
        """
        Uncensored 모듈의 최종 실행 함수. Censored와 로직 통일.
        """
        if task_context is None:
            task_context = {}

        sub_config = config.get('자막우선처리', {})

        code_groups = {}
        for info in execution_plan:
            code_groups.setdefault(info['pure_code'], []).append(info)

        total_files = len(execution_plan)
        logger.info(f"처리할 품번 그룹 {len(code_groups)}개 (총 파일 {total_files}개)")

        scan_enabled = config.get("PLEXMATE스캔", False)
        processed_count = 0
        last_scan_path = None
        last_move_type = None

        successful_move_types = {'subbed', 'override', 'meta_success', 'default', 'custom_path'}
        if config.get('scan_with_no_meta', True):
            successful_move_types.add('meta_fail')

        for idx, (pure_code, group_infos) in enumerate(code_groups.items()):
            try:
                representative_info = group_infos[0]
                target_dir, move_type, meta_info = None, None, None

                # --- 자막 우선 처리 로직 ---
                is_subbed_target = False
                if config.get('자막우선처리활성화', True) is not False:
                    if sub_config.get('처리활성화', False) and sub_config.get('규칙'):
                        if any(kw in representative_info['original_file'].name.lower() for kw in sub_config.get('내장자막키워드', [])) or \
                           CensoredTask._find_external_subtitle(config, representative_info, sub_config):
                            is_subbed_target = True

                if is_subbed_target:
                    logger.info(f"'{pure_code}' 그룹: 자막 파일 조건 충족, 우선 처리합니다.")
                    rule = sub_config['규칙']
                    base_path = Path(rule['경로'])
                    # 자막 우선 처리 시에도 메타 검색 수행
                    if config.get('메타사용') == 'using' and representative_info['label'].lower() in config.get('메타검색지원레이블', set()):
                        meta_module = CensoredTask.get_meta_module('jav_uncensored')
                        meta_info = Task.__search_meta(config, meta_module, pure_code)

                    folder_format = rule.get('폴더구조') or config.get('이동폴더포맷')
                    folders = CensoredTask.process_folder_format(config, representative_info, folder_format, meta_info)
                    target_dir = base_path.joinpath(*folders)
                    move_type = "subbed"
                else:
                    # --- 일반 경로 결정 로직 ---
                    target_dir, move_type, meta_info = Task.__get_target_path(config, representative_info)

                if target_dir is None:
                    logger.debug(f"'{pure_code}' 그룹: 이동할 경로가 결정되지 않아 처리를 건너뜁니다.")
                    continue

                # --- 스캔 요청 전 조건 확인 ---
                if scan_enabled and target_dir != last_scan_path and last_scan_path is not None:
                    if last_move_type in successful_move_types:
                        CensoredTask.__request_plex_mate_scan(config, last_scan_path)

                # --- 그룹 내 각 파일 처리 루프 ---
                for info in group_infos:
                    processed_count += 1
                    logger.info(f"[{processed_count:03d}/{total_files:03d}] {info['original_file'].name}")

                    current_target_dir = target_dir
                    current_move_type = move_type

                    if move_type == "subbed":
                        rule = config.get('자막우선처리', {}).get('규칙', {})
                        exclude_pattern = rule.get('이동제외패턴')
                        if exclude_pattern:
                            try:
                                if re.search(exclude_pattern, info['original_file'].name, re.IGNORECASE):
                                    # logger.info(f"  -> 파일 '{info['original_file'].name}'은(는) 제외 패턴과 일치하여 'subbed_path' 대신 일반 경로로 재지정합니다.")
                                    current_target_dir, current_move_type, _ = Task.__get_target_path(config, info)
                            except re.error as e:
                                logger.error(f"subbed 경로 처리의 '이동제외패턴' 정규식 오류: {e}")

                    new_filename = None
                    if info.get('file_type') in ['subtitle', 'etc']:
                        new_filename = info['original_file'].name
                    elif info.get('file_type') == 'video':
                        if config.get('파일명에미디어정보포함') and not info.get('final_media_info', {}).get('is_valid', True):
                            current_move_type = "failed_video"
                            current_target_dir = Path(config['처리실패이동폴더']).joinpath("[FAILED VIDEO]")
                            new_filename = info['original_file'].name
                        else:
                            new_filename = ToolExpandFileProcess.assemble_filename(config, info)
                    else:
                        new_filename = info['original_file'].name

                    if new_filename is None:
                        continue

                    info['newfilename'] = new_filename

                    info.update({
                        'target_dir': current_target_dir,
                        'move_type': current_move_type,
                        'meta_info': meta_info
                    })

                    entity = CensoredTask.__file_move_logic(config, info, db_model)
                    if entity and entity.move_type is not None:
                        entity.save()

                if scan_enabled and target_dir is not None:
                    last_scan_path = target_dir
                    last_move_type = move_type
            except Exception as e:
                logger.error(f"'{pure_code}' 그룹 처리 중 예외 발생: {e}")
                logger.error(traceback.format_exc())

        if scan_enabled and last_scan_path is not None:
            if last_move_type in successful_move_types:
                logger.info(f"모든 파일 처리 완료. 마지막 경로 스캔 요청: {last_scan_path}")
                CensoredTask.__request_plex_mate_scan(config, last_scan_path)


    @staticmethod
    def __search_meta(config, meta_module, pure_code):
        """메타 검색을 수행하고 딜레이를 적용하는 단일 책임 헬퍼."""
        if not meta_module:
            return None
        try:
            delay_seconds = config.get('파일당딜레이', 0)
            if delay_seconds > 0:
                time.sleep(delay_seconds)

            search_result = meta_module.search(pure_code, manual=False)
            if search_result:
                best_match = next((item for item in search_result if item.get('score', 0) >= 95), None)
                if best_match:
                    return meta_module.info(best_match["code"], fp_meta_mode=False)
        except Exception as e:
            logger.error(f"'{pure_code}' 메타 검색 중 예외: {e}")
        return None



