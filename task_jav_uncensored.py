from .setup import *
from pathlib import Path
import base64, json
import traceback
import re
import os
import shutil
import time
from datetime import datetime
from collections import defaultdict

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

        is_manual_retry = (job_type == 'manual_path')

        if job_type == 'manual_path' and len(args) > 1:
            target_paths = [args[1]]
            logger.info(f"수동 경로 재처리 모드 시작: {target_paths}")
        else:
            target_paths = ModelSetting.get(f"jav_uncensored_download_path").splitlines()

        config = {
            "이름": job_type,
            "사용": True,
            # 기본 탭
            "다운로드폴더": target_paths,
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
            "메타매칭실패시이동": ModelSetting.get_bool("jav_uncensored_meta_no_move"),
            "메타매칭시이동폴더": ModelSetting.get("jav_uncensored_meta_path").strip(),
            "메타매칭실패시이동폴더": ModelSetting.get("jav_uncensored_meta_no_path").strip(),
            "메타매칭실패시파일명변경": ModelSetting.get_bool("jav_uncensored_meta_no_change_filename"),
            "매칭실패재시도주기": ModelSetting.get_int("jav_uncensored_meta_no_retry_every"),
            "라이브러리폴더": ModelSetting.get("jav_uncensored_target_path").splitlines(),

            "재시도": True,
            "방송": False,
            "부가파일생성_YAML": ModelSetting.get_bool("jav_uncensored_make_yaml"),
            "부가파일생성_NFO": ModelSetting.get_bool("jav_uncensored_make_nfo"),
            "부가파일생성_JSON": ModelSetting.get_bool("jav_uncensored_make_json"),
            "부가파일생성_IMAGE": ModelSetting.get_bool("jav_uncensored_make_image"),
            "부가파일덮어쓰기": ModelSetting.get_bool("jav_uncensored_make_overwrite"),
            "부가파일미디어경로포함": ModelSetting.get_bool("jav_uncensored_include_media_path"),
            "PLEXMATE스캔": ModelSetting.get_bool("jav_uncensored_scan_with_plex_mate"),
            "드라이런": ModelSetting.get_bool("jav_uncensored_dry_run"),
            'PLEXMATE_URL': F.SystemModelSetting.get('ddns'),
        }

        config['parse_mode'] = 'uncensored'
        CensoredTask._load_extended_settings(config)

        if is_manual_retry:
            logger.info("수동 재처리 모드: 메타 매칭 실패 시 이동 설정을 강제로 False로 전환합니다.")
            config["메타매칭실패시이동"] = False

        if job_type in ['default', 'dry_run']:
            final_config = config.copy()
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

                    job_name = job.get('이름', '이름 없는 작업')
                    logger.info(f"=========================================")
                    logger.info(f"YAML 작업 실행 시작: [{job_name}]")
                    logger.info(f"=========================================")

                    final_config = config.copy()

                    for key, value in job.items():
                        if value is None or (isinstance(value, str) and not value.strip()):
                            # logger.warning(f"설정 무시: '{key}' 값이 비어있습니다.")
                            continue
                        
                        if key == '원본파일명처리옵션':
                            valid_options = ['original', 'original_bytes', 'original_giga', 'bytes']
                            if value not in valid_options:
                                logger.error(f"설정 오류: '{key}'의 값 '{value}'은 유효하지 않아 무시합니다. (허용값: {valid_options})")
                                continue
                        
                        final_config[key] = value

                    if '자막우선처리활성화' in job:
                        final_config.setdefault('자막우선처리', {})['처리활성화'] = job['자막우선처리활성화']
                    
                    if '동반자막처리활성화' in job:
                        final_config.setdefault('동반자막처리', {})['처리활성화'] = job['동반자막처리활성화']

                    # 커스텀 경로 규칙 예외 처리
                    if '커스텀경로규칙' in job:
                        if isinstance(job.get('커스텀경로규칙'), list):
                            final_config['커스텀경로활성화'] = True
                        else:
                            final_config['커스텀경로활성화'] = False 

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
    def _get_final_target_path(config, info, task_context, do_meta_search=False, preloaded_meta=None):
        use_meta_option = config.get('메타사용', 'using')
        is_companion_pair = bool(info.get('companion_subs_list'))
        sub_config = config.get('자막우선처리', {})
        
        final_path_str = ""
        final_format_str = config.get('이동폴더포맷', '').strip()
        final_move_type = "normal"
        is_failed_move = False # 플래그

        # --- 1. 메타 정보 획득 ---
        meta_info = None
        is_meta_success = False
        if preloaded_meta is not None:
            meta_info = preloaded_meta
        elif do_meta_search and use_meta_option == 'using':
            meta_info = Task._get_metadata(config, info)
        
        is_meta_success = (meta_info is not None) if use_meta_option == 'using' else True
        
        # --- 2. 기본 경로 결정 ---
        if is_meta_success:
            if use_meta_option == 'using':
                final_path_str = config.get('메타매칭시이동폴더')
                final_move_type = "meta_success"
            else:
                library_paths = config.get('라이브러리폴더', [])
                if library_paths:
                    final_path_str = library_paths[0]
                else:
                    logger.error("메타 미사용 모드에서 이동할 라이브러리 폴더가 설정되지 않았습니다.")
                    return None, "no_library_path", meta_info
                final_move_type = "normal"
        else:
            if config.get('메타매칭실패시이동', False):
                final_path_str = config.get('메타매칭실패시이동폴더')
                final_move_type = "meta_fail"
                is_failed_move = True
            else:
                return None, "meta_fail_skipped", meta_info

        # --- 3. 오버라이드 (Censored와 동일 로직) ---
        
        # 3-1. 동반 자막
        if is_companion_pair:
            companion_config = config.get('동반자막처리', {})
            comp_path = ""
            if is_meta_success:
                comp_path = config.get('동반자막처리경로_메타성공시') or companion_config.get('경로_메타성공시')
            else:
                comp_path = config.get('동반자막처리경로_메타실패시') or companion_config.get('경로_메타실패시')
            
            if not comp_path:
                comp_path = config.get('동반자막처리경로') or companion_config.get('경로')
                is_failed_move = False 

            if comp_path: 
                final_path_str = comp_path
            
            final_move_type = 'companion_kor'
            
            comp_format = config.get('동반자막처리폴더포맷') or companion_config.get('폴더포맷')
            if comp_format: 
                final_format_str = comp_format

        # 3-2. 커스텀 경로 규칙
        if config.get('커스텀경로활성화', False):
            # Uncensored는 CensoredTask의 헬퍼 사용
            rule = CensoredTask._find_and_merge_custom_path_rules(info, config.get('커스텀경로규칙', []), meta_info)
            if rule and (is_meta_success or rule.get('force_on_meta_fail')):
                custom_path = rule.get('path') or rule.get('경로', '')
                if custom_path: 
                    final_path_str = custom_path
                    final_move_type = 'custom_path'
                custom_format = rule.get('format') or rule.get('폴더포맷', '')
                if custom_format: 
                    final_format_str = custom_format

                is_failed_move = False

        # 3-3. 자막 우선 처리
        if not is_companion_pair and sub_config.get('처리활성화', False):
            is_applicable = False
            rule = sub_config.get('규칙', {})
            exclude_pattern = rule.get('이동제외패턴')
            if not (exclude_pattern and re.search(exclude_pattern, info['original_file'].name, re.IGNORECASE)):
                if info['file_type'] == 'video':
                    has_internal = any(kw in info['original_file'].name.lower() for kw in sub_config.get('내장자막키워드', []))
                    has_external = CensoredTask._find_external_subtitle(config, info, sub_config, task_context)
                    if has_internal or has_external: is_applicable = True
                elif info['file_type'] == 'subtitle':
                    is_applicable = True
            if is_applicable:
                sub_path = rule.get('경로')
                if sub_path:
                    final_path_str = sub_path
                    final_move_type = 'subbed'
                    is_failed_move = False

        # --- 4. 최종 경로 조립 ---
        if not final_path_str:
            return None, final_move_type, meta_info

        # 헬퍼 호출
        base_path, path_template = CensoredTask._resolve_path_template(config, info, meta_info, final_path_str)

        # [포맷 확정 로직] 플래그 사용
        if is_failed_move:
            final_format_str = ""
        
        # [결합]
        if path_template and final_format_str:
            final_format_str = f"{path_template.rstrip('/')}/{final_format_str.lstrip('/')}"
        elif path_template:
            final_format_str = path_template
        elif final_format_str:
            final_format_str = final_format_str

        # is_code_folder 판단
        if final_format_str:
            last_segment = final_format_str.split('/')[-1].lower()
            code_tags = ['{code}', '{code_lower}']

            info['is_code_folder'] = any(tag in last_segment for tag in code_tags)
        else:
            info['is_code_folder'] = False

        folders = CensoredTask.process_folder_format(config, info, final_format_str, meta_info)
        target_dir = base_path.joinpath(*folders)

        return target_dir, final_move_type, meta_info


    @staticmethod
    def _get_metadata(config, info):
        """
        Uncensored 메타데이터 검색을 수행하고, 성공 시 meta_info 객체를 반환합니다.
        """
        meta_module = CensoredTask.get_meta_module('jav_uncensored')
        if not meta_module:
            return None
        
        if info['label'].lower() not in config.get('메타검색지원레이블', set()):
            return None
            
        try:
            delay_seconds = config.get('파일당딜레이', 0)
            if delay_seconds > 0:
                time.sleep(delay_seconds)

            search_result = meta_module.search(info['pure_code'], manual=False)
            if search_result:
                best_match = next((item for item in search_result if item and item.get('score', 0) >= 95), None)
                if best_match:
                    meta_info = meta_module.info(best_match["code"], fp_meta_mode=True)
                    if meta_info:
                        match_site = best_match.get('site', 'N/A')
                        logger.info(f"'{info['pure_code']}' 메타 검색 성공: {meta_info.get('originaltitle')} (from: {match_site})")
                        for actor in (meta_info.get("actor") or []):
                            try:
                                meta_module.process_actor(actor)
                            except Exception as e:
                                logger.error(f"배우 '{actor.get('originalname')}' 정보 처리 중 오류: {e}")
                        return meta_info
        except Exception as e:
            logger.error(f"'{info['pure_code']}' 메타 검색 중 예외: {e}")
        
        return None


    @staticmethod
    def __execute_plan(config, execution_plan, db_model, task_context=None):
        if task_context is None: task_context = {}
        
        use_meta_option = config.get('메타사용', 'not_using')
        is_companion_enabled = config.get('동반자막처리활성화', False)

        total_items_in_plan = len(execution_plan)
        logger.info(f"처리할 실행 계획 {total_items_in_plan}개")
        logger.info(f"작업 모드: 메타사용 = {use_meta_option}, 동반자막처리 = {is_companion_enabled}")

        scan_enabled = config.get("PLEXMATE스캔", False)
        item_count = 0
        
        # 스캔할 경로들을 중복 없이 저장할 집합(Set)
        scan_queue = set()
        
        # 스캔 대상으로 인정할 유효한 이동 타입 정의
        valid_scan_types = {
            'dvd', 'normal', 'subbed', 'custom_path', 
            'companion_kor', 'companion_kor_sub',
            'meta_success'
        }
        if config.get('scan_with_no_meta', True):
            valid_scan_types.update(['no_meta', 'meta_fail'])
        
        # 실패 타입 명시적 정의 (안전장치)
        failed_types = {'failed_video', 'etc_file', 'meta_fail_skipped', 'no_meta_deleted_due_to_duplication'}

        from itertools import groupby
        execution_plan.sort(key=lambda x: x['pure_code'])

        for pure_code, group_infos_iter in groupby(execution_plan, key=lambda x: x['pure_code']):
            group_infos = list(group_infos_iter)
            logger.debug(f"'{pure_code}' 그룹 처리 시작 ({len(group_infos)}개 파일)")

            first_info = group_infos[0]
            _, _, meta_info_for_group = Task._get_final_target_path(config, first_info, task_context, do_meta_search=True)

            processed_dirs_for_group = set()

            for info in group_infos:
                try:
                    item_count += 1
                    log_prefix = f"[{item_count:03d}/{total_items_in_plan:03d}]"
                    logger.info(f"{log_prefix} 처리 시작: {info['original_file'].name}")
                    
                    target_dir, move_type, _ = Task._get_final_target_path(config, info, task_context, preloaded_meta=meta_info_for_group)

                    # 미디어 정보 분석 실패 여부 확인
                    is_media_info_failed = (
                        config.get('파일명에미디어정보포함') and
                        info['file_type'] == 'video' and
                        (not info.get('final_media_info') or not info['final_media_info'].get('is_valid', True))
                    )

                    if is_media_info_failed and config.get('미디어정보실패시이동', True):
                        logger.warning(f"'{info['original_file'].name}'의 미디어 정보 분석에 실패하여 실패 경로로 이동합니다.")
                        move_type = 'failed_video'
                        
                        failed_path_str = config.get('미디어정보실패시이동경로', '')
                        if failed_path_str:
                            # 사용자가 지정한 실패 경로 사용 (포맷팅 지원)
                            base_path, format_str = CensoredTask._resolve_path_template(config, info, meta_info_for_group, failed_path_str)
                            folders = CensoredTask.process_folder_format(config, info, format_str, meta_info_for_group)
                            target_dir = base_path.joinpath(*folders)
                        else:
                            # 기본 실패 경로 사용 (처리실패이동폴더/[FAILED_VIDEO])
                            base_failed_path = config.get('처리실패이동폴더', '').strip()
                            if base_failed_path:
                                target_dir = Path(base_failed_path).joinpath("[FAILED_VIDEO]")
                            else:
                                logger.error("미디어 분석 실패 파일을 이동할 '미디어정보실패시이동경로' 또는 '처리실패이동폴더'가 설정되지 않았습니다.")
                                continue # 이동 경로가 없으면 건너뛰기
                    else:
                        # 정상 처리 또는 실패 시 이동 옵션이 꺼진 경우
                        target_dir, move_type, _ = Task._get_final_target_path(config, info, task_context, preloaded_meta=meta_info_for_group)

                    if not target_dir:
                        logger.warning(f"'{info['original_file'].name}'의 이동 경로를 결정할 수 없어 건너뜁니다. (이동 타입: {move_type})")
                        continue
                    
                    info.update({'target_dir': target_dir, 'move_type': move_type, 'meta_info': meta_info_for_group})
                    
                    # 부가파일 생성 여부 플래그 결정
                    current_target_dir = str(target_dir)
                    if current_target_dir not in processed_dirs_for_group:
                        info['should_create_meta'] = True
                        processed_dirs_for_group.add(current_target_dir)
                    else:
                        info['should_create_meta'] = False
                    
                    # 스캔 대기열 추가 로직
                    current_target_dir = target_dir
                    if scan_enabled and current_target_dir is not None:
                        if move_type in valid_scan_types and move_type not in failed_types:
                            scan_queue.add(current_target_dir)
                    
                    entity = CensoredTask.__file_move_logic(config, info, db_model)
                    
                    if entity or config.get('드라이런', False):
                        if entity: 
                            if entity.target_path:
                                entity.save()
                            else:
                                continue
                    
                        if 'companion_subs_list' in info:
                            for s_info in info['companion_subs_list']:
                                sub_ext = s_info['original_file'].suffix
                                
                                logger.info(f"{log_prefix} 동반 자막: {s_info['original_file'].name}")
                                
                                new_video_stem = Path(info['newfilename']).stem
                                if entity and entity.target_path:
                                    new_video_stem = Path(entity.target_path).stem
                                
                                final_sub_name = new_video_stem
                                
                                if s_info.get('is_korean', True):
                                    if config.get('동반자막언어코드추가', True) and not re.search(r'\.(ko|kr|kor)$', new_video_stem, re.I):
                                        final_sub_name += '.ko'
                                
                                final_sub_name += sub_ext

                                s_info.update({'target_dir': target_dir, 'move_type': 'companion_kor_sub', 'newfilename': final_sub_name})
                                s_entity = CensoredTask.__file_move_logic(config, s_info, db_model)
                                if s_entity and s_entity.target_path: s_entity.save()
                                
                                if scan_enabled and s_info.get('target_dir'):
                                    scan_queue.add(s_info['target_dir'])

                except Exception as e:
                    logger.error(f"'{info.get('pure_code', '알 수 없음')}' 파일 처리 중 예외 발생: {e}")
                    logger.error(traceback.format_exc())

        # 모든 파일 처리 후 일괄 스캔 요청
        if scan_enabled and scan_queue:
            sorted_scan_paths = sorted(list(scan_queue))
            logger.info(f"모든 파일 처리 완료. 총 {len(sorted_scan_paths)}개 경로에 대해 순차적 스캔을 요청합니다.")
            
            for path in sorted_scan_paths:
                CensoredTask.__request_plex_mate_scan(config, path)
                time.sleep(2)

