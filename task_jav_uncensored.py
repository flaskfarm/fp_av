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
                    final_config.update(job)

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
    def __execute_plan(config, execution_plan, db_model, task_context=None):
        """
        Uncensored 모듈의 최종 실행 함수
        """
        if task_context is None: task_context = {}

        # --- 0. 설정 로드 ---
        sub_config = config.get('자막우선처리', {})
        is_companion_enabled = config.get('동반자막처리활성화', False)
        use_meta_option = config.get('메타사용', 'not_using')

        code_groups = defaultdict(list)
        for info in execution_plan:
            code_groups.setdefault(info['pure_code'], []).append(info)

        total_files = len(execution_plan)
        logger.info(f"처리할 품번 그룹 {len(code_groups)}개 (총 파일 {total_files}개)")
        logger.info(f"작업 모드: 메타사용 = {use_meta_option}, 동반자막처리 = {is_companion_enabled}")

        scan_enabled = config.get("PLEXMATE스캔", False)
        processed_count = 0
        last_scan_path = None
        last_move_type = None
        successful_move_types = {'meta_success', 'normal', 'subbed', 'custom_path', 'companion_kor', 'companion_kor_sub'}
        if config.get('scan_with_no_meta', True):
            successful_move_types.update(['meta_fail'])

        for idx, (pure_code, group_infos) in enumerate(code_groups.items()):
            try:
                processed_infos = set()
                korean_pairs = []
                foreign_pairs = []

                # --- 1. 페어링 단계: 영상과 자막 쌍 찾기 ---
                if is_companion_enabled:
                    videos = sorted([info for info in group_infos if info['file_type'] == 'video'], key=lambda x: len(x['original_file'].name), reverse=True)
                    subs = [info for info in group_infos if info['file_type'] == 'subtitle']

                    for v_info in videos:
                        if id(v_info) in processed_infos: continue
                        video_stem = v_info['original_file'].stem

                        for s_info in subs:
                            if id(s_info) in processed_infos: continue
                            sub_stem = s_info['original_file'].stem

                            if video_stem == sub_stem or video_stem == os.path.splitext(sub_stem)[0] or sub_stem == os.path.splitext(video_stem)[0]:
                                if config.get('동반자막한국어자막판별', False):
                                    if UtilFunc.is_korean_subtitle(s_info['original_file'], config):
                                        korean_pairs.append((v_info, s_info))
                                    else:
                                        foreign_pairs.append((v_info, s_info))
                                else:
                                    korean_pairs.append((v_info, s_info))

                                processed_infos.add(id(v_info))
                                processed_infos.add(id(s_info))
                                break

                # --- 2. 처리 단계 ---
                # 2-1. 한국어 동반 자막 쌍 처리
                for v_info, s_info in korean_pairs:
                    processed_count += 1
                    logger.info(f"[{processed_count:03d}/{total_files:03d}] 동반 영상: {v_info['original_file'].name}")

                    base_path_str = config.get('동반자막처리경로', '').strip()
                    if not base_path_str: base_path_str = config.get('라이브러리폴더', [''])[0]
                    base_path = Path(base_path_str)

                    folder_format = config.get('동반자막처리폴더포맷', '').strip()
                    if not folder_format: folder_format = config.get('이동폴더포맷')

                    meta_info_pair = None
                    if use_meta_option == 'using':
                        if v_info['label'].lower() in config.get('메타검색지원레이블', set()):
                            meta_module = CensoredTask.get_meta_module('jav_uncensored')
                            meta_info_pair = Task.__search_meta(config, meta_module, v_info['pure_code'])

                    folders = CensoredTask.process_folder_format(config, v_info, folder_format, meta_info_pair)
                    target_dir = base_path.joinpath(*folders)

                    v_info.update({'target_dir': target_dir, 'move_type': 'companion_kor', 'newfilename': ToolExpandFileProcess.assemble_filename(config, v_info), 'meta_info': meta_info_pair})

                    v_entity = CensoredTask.__file_move_logic(config, v_info, db_model)

                    # 실제 작업: v_entity가 유효하고 target_path가 있으면 성공
                    if v_entity and v_entity.target_path:
                        v_entity.save()
                        
                        processed_count += 1
                        logger.info(f"[{processed_count:03d}/{total_files:03d}] 동반 자막(한): {s_info['original_file'].name}")
                        
                        new_video_stem = Path(v_entity.target_path).stem
                        sub_stem, sub_ext = os.path.splitext(s_info['original_file'].name)
                        if config.get('동반자막언어코드추가', True) and not re.search(r'\.(ko|kr|kor)$', new_video_stem, re.I):
                            new_video_stem += '.ko'
                        
                        s_info.update({'target_dir': target_dir, 'move_type': 'companion_kor_sub', 'newfilename': new_video_stem + sub_ext})
                        s_entity = CensoredTask.__file_move_logic(config, s_info, db_model)
                        if s_entity: s_entity.save()

                    # 드라이런: v_entity가 None이지만, 드라이런 모드일 경우
                    elif config.get('드라이런', False):
                        processed_count += 1
                        logger.info(f"[{processed_count:03d}/{total_files:03d}] 동반 자막(한): {s_info['original_file'].name}")
                        
                        new_video_stem = Path(v_info['newfilename']).stem
                        sub_stem, sub_ext = os.path.splitext(s_info['original_file'].name)
                        if config.get('동반자막언어코드추가', True) and not re.search(r'\.(ko|kr|kor)$', new_video_stem, re.I):
                            new_video_stem += '.ko'
                        
                        s_info.update({'target_dir': target_dir, 'move_type': 'companion_kor_sub', 'newfilename': new_video_stem + sub_ext})
                        CensoredTask.__file_move_logic(config, s_info, db_model)

                # 2-2. 외국어 동반 자막 쌍 처리
                for v_info, s_info in foreign_pairs:
                    processed_count += 1
                    logger.info(f"[{processed_count:03d}/{total_files:03d}] 동반 자막(외): {s_info['original_file'].name} (영상은 일반 처리)")
                    processed_infos.remove(id(v_info))
                    
                    foreign_sub_path_str = config.get('외국어자막이동경로', '').strip()
                    if foreign_sub_path_str:
                        folders = CensoredTask.process_folder_format(config, s_info, foreign_sub_path_str, None)
                        target_dir = Path('/').joinpath(*folders) if foreign_sub_path_str.startswith('/') else Path().joinpath(*folders)
                    else:
                        target_dir = Path(config.get('처리실패이동폴더')).joinpath("[FOREIGN_SUBS]")
                    
                    s_info.update({'target_dir': target_dir, 'move_type': 'companion_foreign_sub', 'newfilename': s_info['original_file'].name})
                    s_entity = CensoredTask.__file_move_logic(config, s_info, db_model)
                    if s_entity: s_entity.save()

                # 2-3. 짝이 없는 나머지 파일들 처리
                unmatched_infos = [info for info in group_infos if id(info) not in processed_infos]
                if unmatched_infos:
                    rep_info = unmatched_infos[0]
                    group_target_dir, group_move_type, group_meta_info = None, None, None

                    if use_meta_option == 'using':
                        meta_info = None
                        if rep_info['label'].lower() in config.get('메타검색지원레이블', set()):
                            meta_module = CensoredTask.get_meta_module('jav_uncensored')
                            meta_info = Task.__search_meta(config, meta_module, rep_info['pure_code'])
                        
                        if meta_info:
                            group_target_dir = Path(config.get('메타매칭시이동폴더')).joinpath(*CensoredTask.process_folder_format(config, rep_info, config['이동폴더포맷'], meta_info))
                            group_move_type, group_meta_info = "meta_success", meta_info
                        else:
                            group_move_type = "meta_fail"
                            if config.get('메타매칭실패시이동', False):
                                no_meta_path_format_str = config.get('메타매칭실패시이동폴더', '').strip()
                                if no_meta_path_format_str:
                                    folders = CensoredTask.process_folder_format(config, rep_info, no_meta_path_format_str, None)
                                    if no_meta_path_format_str.startswith('/'):
                                        group_target_dir = Path('/').joinpath(*folders)
                                    else:
                                        group_target_dir = Path().joinpath(*folders)
                                else:
                                    temp_path_str = config.get('처리실패이동폴더', '').strip()
                                    if temp_path_str:
                                        group_target_dir = Path(temp_path_str).joinpath("[NO META]")
                                        logger.info(f"메타 없음: '메타매칭실패시이동폴더'가 비어있어 기본 [NO META] 폴더로 이동합니다 - {group_target_dir}")

                    else: # 'not_using'
                        library_paths = config.get('라이브러리폴더', [])
                        if library_paths and library_paths[0]:
                            group_target_dir = Path(library_paths[0]).joinpath(*CensoredTask.process_folder_format(config, rep_info, config['이동폴더포맷'], None))
                            group_move_type = "normal"
                    
                    group_has_external_subtitle = False
                    if sub_config.get('처리활성화', False):
                        if CensoredTask._find_external_subtitle(config, rep_info, sub_config, task_context):
                            group_has_external_subtitle = True

                    if group_target_dir is None and not group_has_external_subtitle:
                        logger.debug(f"'{pure_code}' 그룹의 남은 파일들은 이동할 경로가 없어 건너뜁니다.")
                        continue
                    
                    for info in unmatched_infos:
                        processed_count += 1
                        logger.info(f"[{processed_count:03d}/{total_files:03d}] {info['original_file'].name}")

                        current_target_dir, current_move_type = group_target_dir, group_move_type
                        
                        if info.get('file_type') == 'etc':
                            target_root_str = config.get('처리실패이동폴더', '').strip()
                            if target_root_str:
                                current_target_dir = Path(target_root_str).joinpath("[ETC FILES]")
                                current_move_type = "etc_file_moved"
                            else: continue
                        else:
                            is_handled_by_priority = False
                            if sub_config.get('처리활성화', False):
                                rule = sub_config.get('규칙', {})
                                exclude_pattern = rule.get('이동제외패턴')
                                if not (exclude_pattern and re.search(exclude_pattern, info['original_file'].name, re.IGNORECASE)):
                                    has_internal_keyword = any(kw in info['original_file'].name.lower() for kw in sub_config.get('내장자막키워드', []))
                                    if group_has_external_subtitle or has_internal_keyword:
                                        base_path = Path(rule['경로'])
                                        folder_format = config.get('이동폴더포맷')
                                        folders = CensoredTask.process_folder_format(config, info, folder_format, group_meta_info)
                                        current_target_dir = base_path.joinpath(*folders)
                                        current_move_type = "subbed"
                                        is_handled_by_priority = True

                            if not is_handled_by_priority and config.get('커스텀경로활성화', False):
                                custom_rules = config.get('커스텀경로규칙', [])
                                matched_rule = CensoredTask._find_and_merge_custom_path_rules(info, custom_rules, group_meta_info)
                                if matched_rule:
                                    rule_name = matched_rule.get('name') or matched_rule.get('이름', '이름 없는 규칙')
                                    force_on_meta_fail = matched_rule.get('force_on_meta_fail', False) or matched_rule.get('메타실패시강제적용', False)
                                    if group_move_type in ['meta_success', 'normal'] or force_on_meta_fail:
                                        logger.debug(f"  -> 파일에 커스텀 경로 규칙 '{rule_name}'이 적용됩니다.")
                                        custom_path_str = (matched_rule.get('path') or matched_rule.get('경로', '')).strip()
                                        base_path_for_format = Path(custom_path_str) if custom_path_str else current_target_dir
                                        if base_path_for_format:
                                            folder_format_to_use = (matched_rule.get('format') or matched_rule.get('폴더포맷')) or config['이동폴더포맷']
                                            folders = CensoredTask.process_folder_format(config, info, folder_format_to_use, group_meta_info)
                                            current_target_dir = base_path_for_format.joinpath(*folders)
                                            current_move_type = "custom_path"
                                        else:
                                            logger.warning(f"커스텀 규칙 '{rule_name}'이 매칭되었으나, 적용할 기준 경로(base path)가 없어 건너뜁니다.")
                                    else:
                                        logger.debug(f"  -> 커스텀 규칙 '{rule_name}'은(는) 메타 성공/미사용 시에만 적용되므로 건너뜁니다.")

                        new_filename = ToolExpandFileProcess.assemble_filename(config, info)
                        if info['file_type'] in ['etc', 'subtitle']: new_filename = info['original_file'].name
                        
                        media_info_to_check = info.get('final_media_info')
                        if config.get('파일명에미디어정보포함') and isinstance(media_info_to_check, dict) and not media_info_to_check.get('is_valid', True):
                            current_move_type = "failed_video"
                            current_target_dir = Path(config['처리실패이동폴더']).joinpath("[FAILED VIDEO]")
                            new_filename = info['original_file'].name

                        info.update({'newfilename': new_filename, 'target_dir': current_target_dir, 'move_type': current_move_type, 'meta_info': group_meta_info})

                        if scan_enabled and current_target_dir != last_scan_path and last_scan_path is not None:
                            if last_move_type in successful_move_types: CensoredTask.__request_plex_mate_scan(config, last_scan_path)

                        entity = CensoredTask.__file_move_logic(config, info, db_model)
                        if entity and entity.move_type is not None: entity.save()

                        if scan_enabled and current_target_dir is not None:
                            last_scan_path = current_target_dir
                            last_move_type = current_move_type

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
                    return meta_module.info(best_match["code"], fp_meta_mode=True)
        except Exception as e:
            logger.error(f"'{pure_code}' 메타 검색 중 예외: {e}")
        return None


    # ====================================================================
    # --- Legacy Functions ---
    # ====================================================================


    @staticmethod
    def __get_target_path(config, info):
        """
        Uncensored 전용 경로 결정 로직.
        """
        meta_info = None
        move_type = "default"

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



