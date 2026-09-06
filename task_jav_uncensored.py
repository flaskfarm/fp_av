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
            "파일처리하지않을파일명": ModelSetting.get("jav_uncensored_filename_not_allowed_list") or "",
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
            "매칭실패이동후스캔": ModelSetting.get_bool("jav_uncensored_meta_no_scan_include"),
            "메타매칭실패시파일명변경": ModelSetting.get_bool("jav_uncensored_meta_no_change_filename"),
            "매칭실패재시도주기": ModelSetting.get_int("jav_uncensored_meta_no_retry_every"),
            "라이브러리폴더": ModelSetting.get("jav_uncensored_target_path").splitlines(),

            "재시도": True,
            "방송": False,
            "방송경로변환": None,
            "부가파일생성_YAML": ModelSetting.get_bool("jav_uncensored_make_yaml"),
            "부가파일생성_NFO": ModelSetting.get_bool("jav_uncensored_make_nfo"),
            "부가파일생성_JSON": ModelSetting.get_bool("jav_uncensored_make_json"),
            "부가파일생성_IMAGE": ModelSetting.get_bool("jav_uncensored_make_image"),
            "부가파일생성_TRAILER": ModelSetting.get_bool("jav_uncensored_make_trailer"),
            "부가파일덮어쓰기": ModelSetting.get_bool("jav_uncensored_make_overwrite"),
            "부가파일미디어경로포함": ModelSetting.get_bool("jav_uncensored_include_media_path"),
            "PLEXMATE스캔": ModelSetting.get_bool("jav_uncensored_scan_with_plex_mate"),
            "드라이런": ModelSetting.get_bool("jav_uncensored_dry_run"),

            "동반자막처리활성화": ModelSetting.get_bool("jav_uncensored_companion_enable"),
            "동반자막언어코드추가": ModelSetting.get_bool("jav_uncensored_companion_add_ko"),
            "동반자막한국어자막판별": ModelSetting.get_bool("jav_uncensored_companion_detect_kor"),
            "동반자막경로별도처리": ModelSetting.get_bool("jav_uncensored_companion_use_separate_path"),
            "동반자막처리경로": ModelSetting.get("jav_uncensored_companion_path").strip(),
            "동반자막처리경로_메타실패시": ModelSetting.get("jav_uncensored_companion_meta_fail_path").strip(),

            "공유용라이브러리": ModelSetting.get_bool("jav_uncensored_is_shared_library") if ModelSetting.get("jav_uncensored_is_shared_library") is not None else False,
            "배우이미지우선순위": ModelSetting.get("jav_uncensored_override_actor_img_order") or "",
            "메타이미지모드": ModelSetting.get("jav_uncensored_override_image_mode") or "",
            "메타옵션_이미지제거": ModelSetting.get_bool("jav_uncensored_strip_images") if ModelSetting.get("jav_uncensored_strip_images") is not None else False,
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
        is_failed_move = False

        # --- 1. 메타 정보 획득 ---
        meta_info = None
        is_meta_success = False
        if preloaded_meta is not None:
            meta_info = preloaded_meta
        elif do_meta_search and use_meta_option == 'using':
            meta_info = Task._get_metadata(config, info)
        
        is_meta_success = (meta_info is not None) if use_meta_option == 'using' else True
        
        # --- 2. 커스텀 룰 사전 탐색 ---
        matched_custom_rule = None
        if config.get('커스텀경로활성화', False):
            matched_custom_rule = CensoredTask._find_and_merge_custom_path_rules(info, config.get('커스텀경로규칙', []), meta_info)

        # --- 3. 기본 경로 및 타입 결정 ---
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
                custom_meta_fail_path = matched_custom_rule.get('메타매칭실패시이동폴더') if matched_custom_rule else None
                if custom_meta_fail_path:
                    final_path_str = custom_meta_fail_path
                else:
                    final_path_str = config.get('메타매칭실패시이동폴더')
                
                final_move_type = "meta_fail"
                is_failed_move = True
            else:
                return None, "meta_fail_skipped", meta_info

        # --- 4. 오버라이드 룰 확인 및 병합 ---
        # 4-1. 동반 자막 처리
        if is_companion_pair:
            companion_config = config.get('동반자막처리', {})
            comp_path = ""
            comp_format = ""
            use_separate_path = config.get('동반자막경로별도처리', False)

            if matched_custom_rule:
                if not is_meta_success:
                    comp_path = matched_custom_rule.get('동반자막처리경로_메타실패시')
                if not comp_path:
                    comp_path = matched_custom_rule.get('동반자막처리경로')
                comp_format = matched_custom_rule.get('동반자막처리폴더포맷')
                if comp_path: use_separate_path = True

            if use_separate_path and not comp_path:
                if not is_meta_success:
                    comp_path = config.get('동반자막처리경로_메타실패시')
                if not comp_path:
                    comp_path = config.get('동반자막처리경로')
            
            if not comp_format:
                comp_format = config.get('동반자막처리폴더포맷', '')

            if use_separate_path and not comp_path:
                use_separate_path = False

            if use_separate_path and comp_path: 
                final_path_str = comp_path
                final_move_type = 'companion_kor'
                is_failed_move = False 
                
                if comp_format: 
                    final_format_str = comp_format
                else:
                    if '{' in comp_path:
                        final_format_str = ""

        # 4-2. 일반 커스텀 경로 (동반 자막이 아닐 때)
        elif matched_custom_rule:
            if is_meta_success or matched_custom_rule.get('force_on_meta_fail') or matched_custom_rule.get('메타실패시강제적용'):
                custom_path = matched_custom_rule.get('path') or matched_custom_rule.get('경로', '')
                custom_format = matched_custom_rule.get('format') or matched_custom_rule.get('폴더포맷', '')
                
                if custom_path: 
                    final_path_str = custom_path
                    final_move_type = 'custom_path'
                    is_failed_move = False
                    
                    if custom_format: 
                        final_format_str = custom_format
                    else:
                        if '{' in custom_path:
                            final_format_str = ""
                else:
                    if custom_format: 
                        final_format_str = custom_format

        # 4-3. 자막 우선 처리
        elif not is_companion_pair and sub_config.get('처리활성화', False):
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
        
        # --- 5. 최종 경로 조립 ---
        if not final_path_str:
            return None, final_move_type, meta_info

        base_path, path_template = CensoredTask._resolve_path_template(config, info, meta_info, final_path_str)

        if is_failed_move:
            final_format_str = ""
        
        if path_template and final_format_str:
            final_format_str = f"{path_template.rstrip('/')}/{final_format_str.lstrip('/')}"
        elif path_template:
            final_format_str = path_template
        elif final_format_str:
            final_format_str = final_format_str

        if final_format_str:
            last_segment = final_format_str.split('/')[-1].lower()
            code_tags = ['{code}', '{code_lower}', '{code_upper}']
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
        local_meta = info.get('local_meta_json')
        if local_meta:
            logger.info(f"'{info['pure_code']}' 로컬 JSON 메타데이터를 사용하여 검색을 건너뜁니다.")
            return local_meta

        meta_module = CensoredTask.get_meta_module('jav_uncensored')
        if not meta_module:
            return None

        # extra_opts 초기화 및 옵션 정제
        extra_opts = {}

        skip_trans = None
        if config.get('번역스킵') is not None:
            skip_trans = config.get('번역스킵')
        elif config.get('skip_trans') is not None:
            skip_trans = config.get('skip_trans')
        elif config.get('_skip_trans_temp') is not None:
            skip_trans = config.get('_skip_trans_temp')

        if skip_trans is not None:
            extra_opts['skip_trans'] = skip_trans

        # 한글/영문 키 호환 오버라이드 추출
        is_shared = config.get('공유용라이브러리') or config.get('is_shared_library') or False
        override_actor_img_order = config.get('배우이미지우선순위') or config.get('actor_img_order') or ""
        override_image_mode = config.get('메타이미지모드') or config.get('image_mode') or ""
        override_strip_images = config.get('메타옵션_이미지제거') or config.get('strip_images') or False

        if is_shared:
            # 공유용 라이브러리 모드: 개인 서버 주소 노출 방지 옵션 주입
            extra_opts['actor_img_order'] = override_actor_img_order or 'google_fileid, site_img_url'
            extra_opts['image_mode'] = override_image_mode or 'ff_proxy'
            if override_strip_images:
                extra_opts['strip_images'] = True
        else:
            # 개인용 라이브러리 모드: 사용자가 명시적으로 오버라이드한 값만 주입
            if override_actor_img_order:
                extra_opts['actor_img_order'] = override_actor_img_order
            if override_image_mode:
                extra_opts['image_mode'] = override_image_mode
            if override_strip_images:
                extra_opts['strip_images'] = True

        best_match = None
        match_site = "N/A"
        meta_info = None
        manual_url = info.get('manual_url')

        try:
            if manual_url:
                logger.info(f"'{info['pure_code']}' JAV 수동 URL 매칭 시도: {manual_url}")
                search_result = meta_module.search(manual_url, manual=True)
                if search_result:
                    best_match = next((item for item in search_result if item and item.get('score', 0) >= 95), None)

            if not best_match:
                if info['label'].lower() not in config.get('메타검색지원레이블', set()):
                    return None
                
                search_result = meta_module.search(info['pure_code'], manual=False)
                if search_result:
                    best_match = next((item for item in search_result if item and item.get('score', 0) >= 95), None)

            if best_match:
                meta_info = meta_module.info(best_match["code"], extra_opts=extra_opts)
                if meta_info:
                    match_site = best_match.get('site', 'N/A')
                    logger.info(f"'{info['pure_code']}' 메타 검색 성공: {meta_info.get('originaltitle')} (from: {match_site}, 번역스킵:{extra_opts.get('skip_trans', '기본값')}, 공유모드:{is_shared})")
                    return meta_info

        except Exception as e:
            logger.error(f"'{info['pure_code']}' 메타 검색 중 예외: {e}")
            logger.error(traceback.format_exc())
        
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
        
        valid_scan_types = {
            'dvd', 'normal', 'subbed', 'custom_path', 
            'companion_kor', 'companion_kor_sub',
            'meta_success', 'vr', 'featurette_video'
        }
        if config.get('매칭실패이동후스캔', False):
            valid_scan_types.update(['no_meta', 'meta_fail'])
        
        failed_types = {'failed_video', 'etc_file', 'meta_fail_skipped', 'no_meta_deleted_due_to_duplication'}

        any_meta_option_on = any([
            config.get('부가파일생성_YAML', False),
            config.get('부가파일생성_NFO', False),
            config.get('부가파일생성_JSON', False),
            config.get('부가파일생성_IMAGE', False),
            config.get('부가파일생성_TRAILER', False)
        ])
        make_overwrite = config.get('부가파일덮어쓰기', False)

        from itertools import groupby
        
        file_type_order = {'subtitle': 0, 'etc': 1, 'video': 2}
        execution_plan.sort(key=lambda x: (
            [int(c) if c.isdigit() else c.lower() for c in re.split(r'([0-9]+)', x['pure_code'])],
            file_type_order.get(x.get('file_type', 'etc'), 9),
            [int(c) if c.isdigit() else c.lower() for c in re.split(r'([0-9]+)', x['original_file'].name)]
        ))

        for pure_code, group_infos_iter in groupby(execution_plan, key=lambda x: x['pure_code']):
            group_infos = list(group_infos_iter)
            logger.debug(f"'{pure_code}' 그룹 처리 시작 ({len(group_infos)}개 파일)")

            first_info = next((i for i in group_infos if i.get('file_type') == 'video'), group_infos[0])
            has_local_meta = bool(first_info.get('local_meta_json'))
            
            config['_skip_trans_temp'] = (not any_meta_option_on) or (not make_overwrite)
            target_dir_for_group, _, meta_info_for_group = Task._get_final_target_path(config, first_info, task_context, do_meta_search=True)

            if meta_info_for_group and any_meta_option_on and not make_overwrite and not has_local_meta:
                needs_files = False
                if target_dir_for_group:
                    if not target_dir_for_group.exists():
                        needs_files = True
                    else:
                        is_code_folder = first_info.get('is_code_folder', False)
                        identifier = (meta_info_for_group.get('originaltitle') or meta_info_for_group.get('sorttitle') or meta_info_for_group.get('code', 'movie')).lower()
                        
                        video_stem = Path(first_info.get('newfilename', first_info['original_file'].name)).stem
                        if first_info.get('is_part_of_set') and first_info.get('parsed_part_type'):
                            part_type = first_info.get('parsed_part_type')
                            if video_stem.endswith(part_type):
                                video_stem = video_stem[:-len(part_type)]

                        prefix = 'movie' if is_code_folder else video_stem
                        img_prefix = '' if is_code_folder else f'{video_stem}-'
                        trailer_name = 'movie-trailer.mp4' if is_code_folder else f'{video_stem}-trailer.mp4'

                        if config.get('부가파일생성_YAML', False) and not target_dir_for_group.joinpath(f'{prefix}.yaml').exists(): needs_files = True
                        elif config.get('부가파일생성_NFO', False) and not target_dir_for_group.joinpath(f'{prefix}.nfo').exists(): needs_files = True
                        elif config.get('부가파일생성_JSON', False) and not target_dir_for_group.joinpath(f'{identifier}.json').exists(): needs_files = True
                        elif config.get('부가파일생성_IMAGE', False):
                            if meta_info_for_group.get('thumb') and not target_dir_for_group.joinpath(f'{img_prefix}poster.jpg').exists(): needs_files = True
                            if (meta_info_for_group.get('fanart') or meta_info_for_group.get('thumb')) and not target_dir_for_group.joinpath(f'{img_prefix}fanart.jpg').exists(): needs_files = True
                        elif config.get('부가파일생성_TRAILER', False) and meta_info_for_group.get('extras') and not target_dir_for_group.joinpath(trailer_name).exists(): needs_files = True

                if needs_files:
                    logger.debug(f"'{pure_code}' 부가 파일 생성/갱신이 필요하여 번역이 포함된 메타데이터를 다시 요청합니다.")
                    config['_skip_trans_temp'] = False 
                    meta_info_for_group = Task._get_metadata(config, first_info)

            processed_dirs_for_group = set()

            for info in group_infos:
                try:
                    item_count += 1
                    log_prefix = f"[{item_count:03d}/{total_items_in_plan:03d}]"
                    logger.info(f"{log_prefix} 처리 시작: {info['original_file'].name}")
                    
                    target_dir, move_type, meta_info = None, None, meta_info_for_group
                    
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
                            base_path, format_str = CensoredTask._resolve_path_template(config, info, meta_info_for_group, failed_path_str)
                            folders = CensoredTask.process_folder_format(config, info, format_str, meta_info_for_group)
                            target_dir = base_path.joinpath(*folders)
                        else:
                            base_failed_path = config.get('처리실패이동폴더', '').strip()
                            if base_failed_path:
                                target_dir = Path(base_failed_path).joinpath("[FAILED_VIDEO]")
                            else:
                                continue 
                    else:
                        target_dir, move_type, _ = Task._get_final_target_path(config, info, task_context, preloaded_meta=meta_info_for_group)

                    if not target_dir:
                        logger.warning(f"'{info['original_file'].name}'의 이동 경로를 결정할 수 없어 건너뜁니다. (이동 타입: {move_type})")
                        continue
                    
                    current_target_dir_str = str(target_dir)
                    info['should_create_meta'] = False
                    
                    if not config.get('드라이런', False) and \
                       (current_target_dir_str not in processed_dirs_for_group) and \
                       (move_type not in failed_types) and \
                       any_meta_option_on and \
                       info.get('file_type') == 'video':
                        info['should_create_meta'] = True
                        processed_dirs_for_group.add(current_target_dir_str)

                    info.update({'target_dir': target_dir, 'move_type': move_type, 'meta_info': meta_info_for_group})
                    
                    # 1. 사전 부가파일 생성
                    local_json_moved = False
                    if not config.get('드라이런', False):
                        target_dir.mkdir(parents=True, exist_ok=True)
                        
                        if info.get('file_type') == 'video':
                            json_file = info['original_file'].with_suffix('.json')
                            if json_file.exists() and info.get('local_meta_json'):
                                try:
                                    target_json_name = (meta_info_for_group.get('originaltitle') or meta_info_for_group.get('sorttitle') or meta_info_for_group.get('code', 'movie')).lower()
                                    target_json_path = target_dir.joinpath(f"{target_json_name}.json")
                                    
                                    if json_file.resolve() != target_json_path.resolve():
                                        if target_json_path.exists():
                                            json_file.unlink()
                                        else:
                                            shutil.move(str(json_file), str(target_json_path))
                                            logger.debug(f"로컬 JSON 메타 파일 사전 이동 완료: {target_json_path.name}")
                                    local_json_moved = True
                                except Exception as e:
                                    pass
                        
                        if info['should_create_meta'] and meta_info_for_group:
                            logger.info(f"{log_prefix} 부가 파일 사전 생성 중...: {target_dir}")
                            printable_meta_info = meta_info_for_group.copy()
                            printable_meta_info.pop('original', None)
                            
                            video_stem_for_meta = Path(info.get('newfilename', info['original_file'].name)).stem
                            if info.get('is_part_of_set') and info.get('parsed_part_type'):
                                part_type = info.get('parsed_part_type')
                                if video_stem_for_meta.endswith(part_type):
                                    video_stem_for_meta = video_stem_for_meta[:-len(part_type)]

                            try:
                                TaskMakeYaml.make_files(
                                    printable_meta_info,
                                    current_target_dir_str,
                                    make_yaml=config.get('부가파일생성_YAML', False),
                                    make_nfo=config.get('부가파일생성_NFO', False),
                                    make_json=config.get('부가파일생성_JSON', False) and not local_json_moved,
                                    make_image=config.get('부가파일생성_IMAGE', False),
                                    make_trailer=config.get('부가파일생성_TRAILER', False),
                                    make_overwrite=config.get('부가파일덮어쓰기', False),
                                    include_media_path=config.get('부가파일미디어경로포함', False),
                                    is_code_folder=info.get('is_code_folder', False),
                                    module_name='jav',
                                    original_filename=video_stem_for_meta
                                )
                            except Exception as meta_e:
                                logger.error(f"부가 파일 생성 중 오류: {meta_e}")

                    # 2. 동반 자막 선이동
                    if 'companion_subs_list' in info:
                        for s_info in info['companion_subs_list']:
                            sub_ext = s_info['original_file'].suffix
                            logger.info(f"{log_prefix} 동반 자막 선이동: {s_info['original_file'].name}")
                            
                            new_video_stem = Path(info.get('newfilename', info['original_file'].name)).stem
                            final_sub_name = new_video_stem
                            
                            if s_info.get('is_korean', True) and config.get('동반자막언어코드추가', True) and not re.search(r'\.(ko|kr|kor)$', new_video_stem, re.I):
                                final_sub_name += '.ko'
                            final_sub_name += sub_ext

                            s_info.update({'target_dir': target_dir, 'move_type': 'companion_kor_sub', 'newfilename': final_sub_name})
                            s_entity = CensoredTask.__file_move_logic(config, s_info, db_model)
                            if s_entity and s_entity.target_path: 
                                s_entity.save()

                    # 3. 본 영상 이동
                    entity = CensoredTask.__file_move_logic(config, info, db_model)
                    
                    if entity or config.get('드라이런', False):
                        if entity: 
                            if entity.target_path:
                                entity.save()
                                if info.get('file_type') == 'video':
                                    txt_file = info['original_file'].with_suffix('.txt')
                                    if txt_file.exists() and info.get('manual_url') and not config.get('드라이런', False):
                                        try:
                                            txt_file.unlink()
                                            logger.debug(f"수동 매칭 트리거 파일 삭제 완료: {txt_file.name}")
                                        except Exception as e:
                                            logger.error(f"수동 매칭 트리거 파일 삭제 실패: {txt_file.name}, 오류: {e}")
                            else:
                                logger.warning(f"'{info['original_file'].name}' 이동 실패: 대상 경로가 없습니다.")
                                continue

                        # 4. 스캔 요청
                        if scan_enabled and entity and entity.target_path and entity.move_type in valid_scan_types:
                            if info.get('file_type') == 'video':
                                CensoredTask.__request_plex_mate_scan(config, Path(entity.target_path), entity)
                
                except Exception as e:
                    logger.error(f"'{info.get('pure_code', '알 수 없음')}' 파일 처리 중 예외 발생: {e}")
                    logger.error(traceback.format_exc())

        logger.info("fp_av_jav_uncensored: 모든 작업이 완료되었습니다.")
