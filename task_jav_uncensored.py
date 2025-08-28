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
            "사용자지정레이블폴더": ModelSetting.get("jav_uncensored_label_path_overrides"),
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
                    final_config.update(job)

                    if final_config.get('드라이런', False):
                        logger.warning(f"'{final_config.get('이름', 'YAML Job')}' 작업: Dry Run 모드가 활성화되었습니다.")
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
        }

        CensoredTask.__start_shared_logic(config, task_context)


    # ====================================================================
    # --- Uncensored 전용 헬퍼 ---
    # ====================================================================


    @staticmethod
    def __get_target_with_meta(config, info):
        """
        Uncensored 전용 메타 검색 및 경로를 결정합니다.
        """
        label = info['label'].lower()
        meta_info = None
        move_type = "default"

        # [Priority 1] 사용자 지정 레이블 폴더 (절대 경로)
        overrides_map = Task.__parse_key_value_text(config.get('사용자지정레이블폴더', ''))
        if label in overrides_map:
            user_defined_format = overrides_map[label]
            logger.info(f"'{label}' -> 사용자 지정 절대 경로 규칙 적용.")
            folders = CensoredTask.process_folder_format(config, info, user_defined_format, meta_data=None)

            return Path(os.path.join('/', *folders)), "override", None

        # [Priority 2] 메타 검색 로직
        target_root_str = None
        if config.get('메타사용') == 'using' and label in config.get('메타검색지원레이블', set()):
            meta_module = CensoredTask.get_meta_module('jav_uncensored')
            meta_info = Task.__search_meta(config, meta_module, info['pure_code'])

            if meta_info:
                move_type = "meta_success"
                target_root_str = config.get('메타매칭시이동폴더')
                logger.info(f"메타 검색 성공: {meta_info.get('originaltitle')}")
            else:
                move_type = "meta_fail"
                target_root_str = config.get('메타매칭실패시이동폴더')
                logger.info(f"메타 검색 실패: {info['pure_code']}")
        else:
            # 메타 미사용 또는 지원되지 않는 레이블
            library_paths = CensoredTask.get_path_list(config.get('라이브러리폴더', []))
            target_root_str = library_paths[0] if library_paths else None

        if not target_root_str:
            logger.error("이동할 대상 경로가 설정되지 않았습니다. 처리 실패 폴더로 이동합니다.")
            return Path(config['처리실패이동폴더']).joinpath("[NO TARGET PATH]"), "error", None

        # [Priority 3 & 최종 경로 결정]
        current_target_root = Path(str(target_root_str))
        folder_format_to_use = config['이동폴더포맷']

        if config.get('커스텀경로활성화', False):
            custom_rules = config.get('커스텀경로규칙', [])
            effective_info = info.copy()
            if meta_info:
                effective_info['label'] = meta_info.get("originaltitle", info['pure_code']).split('-')[0]

            matched_rule = CensoredTask.__find_custom_path_rule(effective_info, custom_rules)
            if matched_rule:
                custom_path_str = matched_rule.get('path', '').strip()
                if custom_path_str:
                    current_target_root = Path(custom_path_str)
                    move_type = "custom_path"
                    if matched_rule['format']:
                        folder_format_to_use = matched_rule['format']

        # 최종적으로 결정된 경로와 포맷을 사용하여 폴더 목록 생성
        folders = CensoredTask.process_folder_format(config, info, folder_format_to_use, meta_info)

        return current_target_root.joinpath(*folders), move_type, meta_info


    @staticmethod
    def __execute_plan(config, execution_plan, db_model):
        """
        Uncensored 모듈의 최종 실행 함수. Censored와 로직 통일.
        """
        sub_config = config.get('확장_자막우선처리', {})
        
        code_groups = {}
        for info in execution_plan:
            code_groups.setdefault(info['pure_code'], []).append(info)

        total_files = len(execution_plan)
        logger.info(f"처리할 품번 그룹 {len(code_groups)}개 (총 파일 {total_files}개)")

        scan_enabled = config.get("PLEXMATE스캔", False)
        processed_count = 0
        last_scan_path = None
        last_move_type = None # 마지막 이동 타입을 추적

        successful_move_types = {'subbed', 'override', 'meta_success', 'default', 'custom_path'}
        if config.get('scan_with_no_meta', True):
            successful_move_types.add('meta_fail') # Uncensored는 no_meta가 별도로 없음
            logger.debug(f"메타 없는 파일 스캔 활성화. 스캔 대상: {successful_move_types}")

        for idx, (pure_code, group_infos) in enumerate(code_groups.items()):
            try:
                representative_info = group_infos[0]
                target_dir, move_type, meta_info = None, None, None

                # --- 자막 우선 처리 로직 ---
                is_subbed_target = False
                if sub_config.get('처리활성화') and sub_config.get('규칙'):
                    if any(kw in representative_info['original_file'].name.lower() for kw in sub_config['내장자막키워드']) or \
                       CensoredTask._find_external_subtitle(config, representative_info, sub_config):
                        is_subbed_target = True

                if is_subbed_target:
                    logger.info(f"'{pure_code}' 그룹: 자막 파일 조건 충족, 우선 처리합니다.")
                    rule = sub_config['규칙']
                    base_path = Path(rule['경로'])
                    # 자막 우선 처리 시에도 메타 검색은 선택적으로 수행
                    if config.get('메타사용') == 'using' and representative_info['label'].lower() in config.get('메타검색지원레이블', set()):
                        meta_module = CensoredTask.get_meta_module('jav_uncensored')
                        meta_info = Task.__search_meta(config, meta_module, pure_code)

                    folders = CensoredTask.process_folder_format(config, representative_info, meta_info)
                    target_dir = base_path.joinpath(*folders)
                    move_type = "subbed"
                else:
                    # --- 일반 경로 결정 및 미디어 정보 처리 ---
                    # (Censored와 동일한 로직을 여기에 직접 포함)
                    is_set = any(info.get('is_part_of_set') for info in group_infos)
                    ext_config = config.get('확장_ffprobe', {})
                    use_media_info_in_filename = config.get('파일명에미디어정보포함') and ext_config.get('enable')

                    if is_set and use_media_info_in_filename:
                        merge_result = CensoredTask._merge_and_standardize_media_info(group_infos, ext_config)
                        if merge_result.get('is_valid_set'):
                            for info in group_infos: info['final_media_info'] = merge_result['final_media']
                        else:
                            logger.warning(f"'{pure_code}' 그룹 미디어 오류로 분할 세트 처리를 취소합니다.")
                            failed_files_set = {f['original_file'].name for f in merge_result.get('failed_files', [])}
                            for info in group_infos:
                                info.update({'is_part_of_set': False, 'parsed_part_type': ''})
                                info['final_media_info'] = {'is_valid': False} if info['original_file'].name in failed_files_set else info.get('media_info')
                    else:
                        for info in group_infos:
                            info['final_media_info'] = info.get('media_info') if use_media_info_in_filename else None

                    # Uncensored 전용 경로 결정 헬퍼 호출
                    target_dir, move_type, meta_info = Task.__get_target_with_meta(config, representative_info)

                # --- 스캔 요청 전 조건 확인 ---
                successful_move_types = {'subbed', 'override', 'meta_success', 'default', 'custom_path'}

                if scan_enabled and target_dir != last_scan_path and last_scan_path is not None:
                    if last_move_type in successful_move_types:
                        CensoredTask.__request_plex_mate_scan(config, last_scan_path)

                # --- 그룹 내 각 파일 처리 루프 ---
                for info in group_infos:
                    processed_count += 1
                    logger.info(f"[{processed_count:03d}/{total_files:03d}] {info['original_file'].name}")

                    info['newfilename'] = ToolExpandFileProcess.assemble_filename(config, info)
                    info['target_dir'] = target_dir
                    info['move_type'] = move_type
                    info['meta_info'] = meta_info

                    entity = CensoredTask.__file_move_logic(config, info, db_model)
                    if entity and entity.move_type is not None:
                        entity.save()

                # --- 마지막 스캔 경로 및 타입 업데이트 ---
                if scan_enabled and target_dir is not None:
                    last_scan_path = target_dir
                    last_move_type = move_type

            except Exception as e:
                logger.error(f"'{pure_code}' 그룹 처리 중 예외 발생: {e}")
                logger.error(traceback.format_exc())

        # --- 모든 작업 완료 후 최종 스캔 요청 ---
        if scan_enabled and last_scan_path is not None:
            successful_move_types = {'subbed', 'override', 'meta_success', 'default', 'custom_path'}
            if last_move_type in successful_move_types:
                logger.info(f"모든 파일 처리 완료. 마지막 경로 스캔 요청: {last_scan_path}")
                CensoredTask.__request_plex_mate_scan(config, last_scan_path)


    @staticmethod
    def __parse_key_value_text(text_data: str) -> dict:
        """사용자 지정 레이블 폴더 설정을 파싱하는 헬퍼 함수."""
        if not text_data: return {}
        result = {}
        for line in text_data.strip().splitlines():
            line = line.strip()
            if not line or line.startswith('#'): continue
            parts = [part.strip() for part in line.split(':', 1)]
            if len(parts) == 2 and parts[0]: result[parts[0].lower()] = parts[1]
        return result


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
                    return meta_module.info(best_match["code"], fp_meta_mode=True)
        except Exception as e:
            logger.error(f"'{pure_code}' 메타 검색 중 예외: {e}")
        return None



