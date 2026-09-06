from .setup import *
from pathlib import Path
import base64, json
import traceback
import re
import os
import shutil
import time
import requests
import json

from datetime import datetime
from collections import defaultdict

ModelSetting = P.ModelSetting
from .tool import ToolExpandFileProcess, UtilFunc, SafeFormatter, ToolExpandFileProcess
from .model_jav_censored import ModelJavCensoredItem
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
            target_paths = ModelSetting.get(f"jav_censored_download_path").splitlines()

        config = {
            "이름": job_type,
            "사용": True,
            "처리실패이동폴더": ModelSetting.get("jav_censored_temp_path").strip(),
            "중복파일이동폴더": ModelSetting.get("jav_censored_remove_path").strip(),
            "다운로드폴더": target_paths,
            "라이브러리폴더": ModelSetting.get("jav_censored_target_path").splitlines(),

            "최소크기": ModelSetting.get_int("jav_censored_min_size"),
            "최대기간": ModelSetting.get_int("jav_censored_max_age"),
            "품번파싱제외키워드": ModelSetting.get_list("jav_censored_filename_cleanup_list", "|"),
            "파일처리하지않을파일명": ModelSetting.get("jav_censored_filename_not_allowed_list") or "",

            "이동폴더포맷": ModelSetting.get("jav_censored_folder_format"),
            "메타사용": ModelSetting.get("jav_censored_use_meta"),
            "파일명변경": ModelSetting.get_bool("jav_censored_change_filename"),
            "파일명에미디어정보포함": ModelSetting.get_bool("jav_censored_include_media_info_in_filename"),
            "분할파일처리": ModelSetting.get_bool("jav_censored_process_part_files"),
            "원본파일명포함여부": ModelSetting.get_bool("jav_censored_include_original_filename"),
            "원본파일명처리옵션": ModelSetting.get("jav_censored_include_original_filename_option"),

            "메타검색에공식사이트만사용": ModelSetting.get_bool("jav_censored_meta_dvd_use_dmm_only"),
            "메타매칭시이동폴더": ModelSetting.get("jav_censored_meta_dvd_path").strip(),
            "VR영상이동폴더": ModelSetting.get("jav_censored_meta_dvd_vr_path").strip(),

            "메타매칭제외레이블": ModelSetting.get_list("jav_censored_meta_dvd_labels_exclude", ","),
            "메타매칭포함레이블": ModelSetting.get_list("jav_censored_meta_dvd_labels_include", ','),
            "배우조건매칭시이동폴더포맷": ModelSetting.get("jav_censored_folder_format_actor").strip(),
            "메타매칭실패시이동": ModelSetting.get_bool("jav_censored_meta_no_move"),
            "메타매칭실패시파일명변경": ModelSetting.get_bool("jav_censored_meta_no_change_filename"),
            "메타매칭실패시이동폴더": ModelSetting.get("jav_censored_meta_no_path").strip(),
            "매칭실패이동후스캔": ModelSetting.get_bool("jav_censored_meta_no_scan_include"),

            # "재시도": True, # 죽은 설정?
            "방송": False,
            "방송경로변환": None, # {'/mnt/local': '/cloud/path'} 형태

            # 부가파일 생성 옵션 추가
            "부가파일생성_YAML": ModelSetting.get_bool("jav_censored_make_yaml"),
            "부가파일생성_NFO": ModelSetting.get_bool("jav_censored_make_nfo"),
            "부가파일생성_JSON": ModelSetting.get_bool("jav_censored_make_json"),
            "부가파일생성_IMAGE": ModelSetting.get_bool("jav_censored_make_image"),
            "부가파일생성_TRAILER": ModelSetting.get_bool("jav_censored_make_trailer"),
            "부가파일덮어쓰기": ModelSetting.get_bool("jav_censored_make_overwrite"),
            "부가파일미디어경로포함": ModelSetting.get_bool("jav_censored_include_media_path"),

            # etc
            "파일당딜레이": ModelSetting.get_int("jav_censored_delay_per_file"),
            "PLEXMATE스캔": ModelSetting.get_bool("jav_censored_scan_with_plex_mate"),
            "드라이런": ModelSetting.get_bool("jav_censored_dry_run"),

            # UI/DB 동반자막 설정 로드
            "동반자막처리활성화": ModelSetting.get_bool("jav_censored_companion_enable"),
            "동반자막언어코드추가": ModelSetting.get_bool("jav_censored_companion_add_ko"),
            "동반자막한국어자막판별": ModelSetting.get_bool("jav_censored_companion_detect_kor"),
            "동반자막경로별도처리": ModelSetting.get_bool("jav_censored_companion_use_separate_path"),
            "동반자막처리경로": ModelSetting.get("jav_censored_companion_path").strip(),
            "동반자막처리경로_메타실패시": ModelSetting.get("jav_censored_companion_meta_fail_path").strip(),

            # 메타 취득 시 임시 오버라이드 옵션 (개인용 vs 공유용 분기 지원)
            "공유용라이브러리": ModelSetting.get_bool("jav_censored_is_shared_library") if ModelSetting.get("jav_censored_is_shared_library") is not None else False,
            "배우이미지우선순위": ModelSetting.get("jav_censored_override_actor_img_order") or "",
            "메타이미지모드": ModelSetting.get("jav_censored_override_image_mode") or "",
            "메타옵션_이미지제거": ModelSetting.get_bool("jav_censored_strip_images") if ModelSetting.get("jav_censored_strip_images") is not None else False,
        }

        config['parse_mode'] = 'censored'
        Task._load_extended_settings(config)

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
                    
                    # 안전한 덮어쓰기 로직 적용
                    for key, value in job.items():
                        # 1. 값이 없거나 빈 문자열이면 무시 (기본값 유지)
                        if value is None or (isinstance(value, str) and not value.strip()):
                            # logger.warning(f"설정 무시: '{key}' 값이 비어있습니다.")
                            continue
                        
                        # 2. 특정 중요 옵션 유효성 검사 (오타 방지)
                        if key == '원본파일명처리옵션':
                            valid_options = ['original', 'original_bytes', 'original_giga', 'bytes']
                            if value not in valid_options:
                                logger.error(f"설정 오류: '{key}'의 값 '{value}'은 유효하지 않아 무시합니다. (허용값: {valid_options})")
                                continue
                        
                        # 3. 검증 통과 시 덮어쓰기
                        final_config[key] = value

                    # 아래 로직들은 job에 해당 키가 '있을 때만' 동작하므로 안전함
                    if '자막우선처리활성화' in job:
                        final_config.setdefault('자막우선처리', {})['처리활성화'] = job['자막우선처리활성화']
                    
                    if '동반자막처리활성화' in job:
                        final_config.setdefault('동반자막처리', {})['처리활성화'] = job['동반자막처리활성화']

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
        config['module_name'] = 'jav_censored'

        Task.start(config)


class Task:
    metadata_modules = {}


    @staticmethod
    def start(config):
        task_context = {
            'module_name': 'jav_censored',
            'parse_mode': 'censored',
            'execute_plan': Task.__execute_plan,
            'db_model': ModelJavCensoredItem,
        }

        Task.__start_shared_logic(config, task_context)


    @staticmethod
    def __start_shared_logic(config, task_context):
        """모든 JAV 파일 처리 작업의 공통 실행 흐름을 담당합니다."""
        task_context['subtitle_cache'] = {}
        task_context['scanned_directories_cache'] = set()

        # 1. 파일 목록 수집
        logger.debug(f"처리 파일 목록 생성")
        all_files = Task.__collect_initial_files(config, config['module_name'])
        if not all_files:
            logger.info("처리할 파일이 없습니다.")
            return

        # 2. 파싱 및 기본 정보 추출
        logger.info(f"파싱 및 기본 정보 추출")
        parsed_infos = []
        unparsed_infos = []
        for file in all_files:
            info = Task.__prepare_initial_info(config, file)
            if info.get('is_parsed'):
                parsed_infos.append(info)
            else:
                unparsed_infos.append(info)

        # 2-1. 파싱 실패 파일 처리
        if unparsed_infos:
            logger.info(f"파싱 실패 파일 {len(unparsed_infos)}개 이동 시작")
            for info in unparsed_infos:
                Task.__move_to_no_label_folder(config, info['original_file'])

        if not parsed_infos:
            logger.info("파싱에 성공한 파일이 없어 작업을 종료합니다.")
            return

        # 3. 실행 계획 수립 (페어링)
        execution_plan = []
        is_companion_enabled = config.get('동반자막처리활성화', False)

        videos = [info for info in parsed_infos if info['file_type'] == 'video']
        subtitles = [info for info in parsed_infos if info['file_type'] == 'subtitle']
        etc_files = [info for info in parsed_infos if info['file_type'] == 'etc']
        
        # 기타 파일 처리
        if etc_files:
            failed_path_str = config.get('처리실패이동폴더', '').strip()
            if failed_path_str:
                target_dir = Path(failed_path_str).joinpath("[ETC_FILES]")
                
                if not config.get('드라이런', False):
                    target_dir.mkdir(parents=True, exist_ok=True)
                
                for info in etc_files:
                    file = info['original_file']
                    newfile = target_dir.joinpath(file.name)
                    
                    if config.get('드라이런', False):
                        logger.warning(f"[Dry Run] 기타 파일 이동 예정: '{file.name}' -> '{newfile}'")
                        continue
                    
                    try:
                        if newfile.exists():
                            newfile.unlink()
                            
                        shutil.move(str(file), str(newfile))
                        logger.debug(f"기타 파일 이동 (중복 무시): {file.name} -> {target_dir}")
                        
                        entity = task_context['db_model'](config.get('이름'), str(file.parent), file.name)
                        entity.set_target(newfile).set_move_type('etc_file')
                        entity.save()
                        
                    except Exception as e:
                        logger.error(f"기타 파일 이동 중 오류 ({file.name}): {e}")
            else:
                logger.warning(f"기타 파일({len(etc_files)}개)을 이동할 '처리실패이동폴더'가 설정되지 않아 건너뜁니다.")
        
        if is_companion_enabled:
            videos, unmatched_subs = ToolExpandFileProcess.pair_companion_subtitles(videos, subtitles, config)
            
            # 최종 실행 계획 = 영상(자막 포함) + 짝없는 자막
            execution_plan.extend(videos)
            execution_plan.extend(unmatched_subs)
        else:
            # 동반자막 기능이 꺼져있으면, 모든 파일을 그대로 실행 계획에 포함
            execution_plan.extend(videos)
            execution_plan.extend(subtitles)

        # 4. 최종 정렬
        if execution_plan:
            logger.debug(f"실행 계획을 품번 및 파일 타입 기준으로 재정렬합니다.")
            file_type_order = {'video': 0, 'subtitle': 1, 'etc': 2}
            execution_plan.sort(key=lambda info: (
                [int(c) if c.isdigit() else c.lower() for c in re.split('([0-9]+)', info['pure_code'])], 
                file_type_order.get(info['file_type'], 9),
                [int(c) if c.isdigit() else c.lower() for c in re.split('([0-9]+)', info['original_file'].name)]
            ))

        # 5. 분할 파일 처리
        if config.get('분할파일처리', True):
            logger.debug(f"분할 파일 세트 식별 및 처리 시작")
            Task.__process_part_sets(execution_plan)

        # 6. 파일명 조립을 위한 최종 데이터(final_media_info) 준비
        ext_config = config.get('미디어정보설정', {})
        use_media_info = config.get('파일명에미디어정보포함')

        if not use_media_info:
            for info in execution_plan:
                info['final_media_info'] = None
        else:
            from collections import defaultdict
            
            # 분할 파일 세트끼리만 묶기 위한 딕셔너리
            # Key: (품번, prefix, suffix) -> 유니크한 세트 식별자
            set_groups = defaultdict(list)

            for info in execution_plan:
                if info.get('is_part_of_set'):
                    # 분할 파일인 경우: 고유 세트 키로 그룹핑
                    set_key = (info['pure_code'], info.get('part_set_prefix'), info.get('part_set_suffix'))
                    set_groups[set_key].append(info)
                else:
                    # 단일 파일인 경우: 개별 미디어 정보 즉시 할당
                    info['final_media_info'] = info.get('media_info')

            # 식별된 세트들에 대해서만 병합(Merge) 로직 수행
            for key, group_infos in set_groups.items():
                pure_code = key[0]
                merge_result = Task._merge_and_standardize_media_info(group_infos, ext_config)
                
                if merge_result.get('is_valid_set'):
                    # 세트가 유효(해상도/코덱 일치)하면 병합된 정보 사용
                    for info in group_infos:
                        info['final_media_info'] = merge_result['final_media']
                else:
                    # 세트 내 스펙 불일치 시 그룹 해제 및 개별 처리
                    logger.warning(f"'{pure_code}' 분할 세트의 미디어 정보 불일치로 세트 처리를 취소합니다.")
                    failed_files_set = {f['original_file'].name for f in merge_result.get('failed_files', [])}
                    
                    for info in group_infos:
                        info.update({'is_part_of_set': False, 'parsed_part_type': ''})
                        # 정보 분석에 실패했던 파일만 invalid 처리, 나머지는 본인 정보 사용
                        if info['original_file'].name in failed_files_set:
                            info['final_media_info'] = {'is_valid': False}
                        else:
                            info['final_media_info'] = info.get('media_info')

        # 7. 최종 파일명 조립 (tool.py 호출)
        for info in execution_plan:
            info['newfilename'] = ToolExpandFileProcess.assemble_filename(config, info)

        # 8. 실제 파일 이동 (각 모듈의 __execute_plan 호출)
        task_context['execute_plan'](config, execution_plan, task_context['db_model'], task_context)


    # ====================================================================
    # --- 헬퍼 함수들 (Helper Functions) ---
    # ====================================================================

    @staticmethod
    def _parse_custom_path_rules(custom_path_section, current_module):
        """커스텀 경로 규칙 섹션을 파싱하여 활성화 여부와 규칙 리스트를 반환합니다."""
        if not custom_path_section or not isinstance(custom_path_section, dict):
            return False, []

        is_enabled = custom_path_section.get('enable', False)
        custom_rules_for_module = []

        if is_enabled:
            custom_path_rules_yaml = custom_path_section.get('규칙', [])
            if custom_path_rules_yaml:
                logger.debug(f"커스텀 경로 규칙 {len(custom_path_rules_yaml)}개를 로드합니다.")
                for rule_idx, rule in enumerate(custom_path_rules_yaml):
                    target_module = rule.get('모듈', 'all').lower()
                    if target_module != 'all' and target_module != current_module:
                        continue

                    label_pattern = rule.get('레이블', '').strip()
                    filename_pattern = rule.get('파일명패턴', '').strip()

                    if not label_pattern and not filename_pattern:
                        continue

                    if label_pattern:
                        label_pattern = re.sub(r'\s', '', label_pattern)

                    processed_rule = rule.copy() 
                    processed_rule.update({
                        'name': rule.get('이름', f'규칙 #{rule_idx+1}'),
                        'path': rule.get('경로', '').strip(),
                        'format': rule.get('폴더포맷', '').strip(),
                        'label_pattern': label_pattern,
                        'filename_pattern': filename_pattern,
                        'force_on_meta_fail': rule.get('메타실패시강제적용', False)
                    })
                    
                    custom_rules_for_module.append(processed_rule)

        return is_enabled, custom_rules_for_module


    @staticmethod
    def _load_extended_settings(config):
        """jav_censored 메타 모듈에서 파싱 규칙 및 개별 확장 기능 설정을 로드합니다."""

        try:
            meta_module = Task.get_meta_module('jav_censored')
            if meta_module and hasattr(meta_module, 'get_jav_settings'):
                jav_settings = meta_module.get_jav_settings()

                config['파싱규칙'] = jav_settings.get('jav_parsing_rules', {})

                # 기타 고급 설정(misc_settings)
                misc_settings = jav_settings.get('misc_settings', {})

                config['중복체크방식'] = misc_settings.get('duplicate_check_method', 'flexible')
                config['메타검색에사용할사이트'] = misc_settings.get('메타검색에사용할사이트', None)
                config['이미처리된파일명패턴'] = misc_settings.get('already_processed_pattern', r'^[a-zA-Z0-9]+-[a-zA-Z0-9-_]+(\s\[.*\](?:cd\d+)?)$')
                config['허용된숫자레이블'] = misc_settings.get('allowed_numeric_labels', r'^(741|1pon|10mu).*?')
                config['scan_with_no_meta'] = misc_settings.get('scan_with_no_meta', True)
                # 확장자 설정 로드
                default_video_exts = {".mp4", ".mkv", ".avi", ".ts", ".wmv", ".m2ts", ".mts", ".m4v", ".flv", ".asf", ".mpg", ".ogm", ".rmvb", ".mov", ".webm"}
                default_subtitle_exts = {".srt", ".smi", ".ass", ".ssa", "idx", "sub", ".sup", ".ttml", ".vtt"}

                video_ext_str = misc_settings.get('video_extensions', '').strip()
                if video_ext_str:
                    config['video_exts'] = {f".{ext.strip().lstrip('.')}" for ext in video_ext_str.split()}
                else:
                    config['video_exts'] = default_video_exts

                subtitle_ext_str = misc_settings.get('subtitle_extensions', '').strip()
                if subtitle_ext_str:
                    config['subtitle_exts'] = {f".{ext.strip().lstrip('.')}" for ext in subtitle_ext_str.split()}
                else:
                    config['subtitle_exts'] = default_subtitle_exts

                # --- 커스텀 경로 규칙 ---
                custom_path_section = jav_settings.get('meta_custom_path', {})
                current_module = config.get('parse_mode')
                is_enabled, rules = Task._parse_custom_path_rules(custom_path_section, current_module)
                config['커스텀경로활성화'] = is_enabled
                config['커스텀경로규칙'] = rules

                # --- ffprobe 관련 설정 로드 ---
                default_media_info_config = {
                    'ffprobe_path': '/usr/bin/ffprobe',
                    'tolerance': { 'audio_bitrate': 5, 'fps': 0.01 },
                    'standard_fps_values': [23.976, 24, 25, 29.97, 30, 59.94, 60],
                    'resolution_tiers': [
                        {'min_height': 4000, 'max_height': 6000, 'tag': '8K'},
                        {'min_height': 1600, 'max_height': 4000, 'tag': '4K'},
                        {'min_height': 900, 'max_height': 1600, 'tag': 'FHD'},
                        {'min_height': 600, 'max_height': 900, 'tag': 'HD'},
                        {'min_height': 0, 'max_height': 600, 'tag': 'SD'}
                    ],
                    'media_info_template': "[[{res_tag}]].[[{v_codec}]].[[{fps}fps]].[[{a_codec}]][[-{a_bitrate}kbps]] [[{tag_title}]]",
                    'reprocess_skip_pattern': r'\[(FHD|HD|SD|4K|8K|H264|H265|HEVC|AAC|AC3|OPUS)',
                    'reprocess_insert_pattern': r'^([a-zA-Z0-9-]+)(\s\[)(.*\])$'
                }
                media_info_config = jav_settings.get('filename_with_media_info', {})
                # nested dict update
                default_media_info_config.update({k: v for k, v in media_info_config.items() if not isinstance(v, dict)})
                default_media_info_config.get('tolerance', {}).update(media_info_config.get('tolerance', {}))
                config['미디어정보설정'] = default_media_info_config
                # 미디어 분석 실패 처리 설정
                config['미디어정보실패시이동'] = media_info_config.get('실패시별도경로로이동', True)
                config['미디어정보실패시이동경로'] = media_info_config.get('실패시이동경로', '')

                # --- 자막 우선 처리(subbed_path) 설정 로드 ---
                default_subbed_path_config = {
                    '처리활성화': False,
                    '자막파일확장자': {'ass', 'ssa', 'idx', 'sub', 'sup', 'smi', 'srt', 'ttml', 'vtt'},
                    '내장자막키워드': [],
                    '규칙': {}
                }
                subbed_path_config_yaml = jav_settings.get('subbed_path', {})
                if subbed_path_config_yaml and subbed_path_config_yaml.get('처리활성화'):
                    default_subbed_path_config['처리활성화'] = True

                    ext_str = subbed_path_config_yaml.get('자막파일확장자', 'ass ssa idx sub sup smi srt ttml vtt')
                    default_subbed_path_config['자막파일확장자'] = {f".{ext.strip()}" for ext in ext_str.split()}

                    keyword_str = subbed_path_config_yaml.get('내장자막키워드', '')
                    if keyword_str:
                        default_subbed_path_config['내장자막키워드'] = [kw.strip().lower() for kw in keyword_str.split()]

                    # 현재 모듈에 맞는 규칙만 저장
                    current_module = config.get('parse_mode')
                    for rule in subbed_path_config_yaml.get('규칙', []):
                        if rule.get('모듈', '').lower() == current_module:
                            default_subbed_path_config['규칙'] = rule
                            default_subbed_path_config['규칙']['이동제외패턴'] = rule.get('이동제외패턴', '').strip()
                            # logger.debug(f"자막 우선 처리 규칙 로드: 모듈={current_module}, 경로={rule.get('경로')}")
                            break

                config['자막우선처리'] = default_subbed_path_config

            else:
                logger.warning("메타데이터 플러그인을 찾을 수 없어 파싱 규칙 및 확장 설정을 로드하지 못했습니다.")
                config['파싱규칙'] = {}
                config['자막우선처리'] = {'처리활성화': False}
                config['동반자막처리활성화'] = False
        except Exception as e:
            logger.error(f"확장 설정 로드 중 오류: {e}")
            config['파싱규칙'] = {}
            config['자막우선처리'] = {'처리활성화': False}
            config['동반자막처리활성화'] = False


    @staticmethod
    def _find_external_subtitle(config, info, sub_config, task_context):
        """지정된 경로에서 품번에 해당하는 외부 자막 파일이 있는지 확인합니다."""
        subtitle_cache = task_context.get('subtitle_cache', {})
        scanned_directories_cache = task_context.get('scanned_directories_cache', set())
        pure_code = info['pure_code']

        # 1. 최종 결과 캐시(subtitle_cache)에 있는지 먼저 확인
        if pure_code in subtitle_cache:
            return subtitle_cache[pure_code]

        # 2. 예상되는 자막 폴더 경로 계산
        rule = sub_config.get('규칙', {})
        base_path_str = rule.get('경로')
        if not base_path_str: return False

        base_path = Path(base_path_str)
        if not base_path.is_dir(): return False

        folder_format = config.get('이동폴더포맷')
        relative_folders = Task.process_folder_format(config, info, folder_format, meta_data=None)
        target_sub_dir = base_path.joinpath(*relative_folders)

        # 3. 해당 폴더를 이미 스캔했는지 확인
        if target_sub_dir in scanned_directories_cache:
            # 스캔은 했는데 캐시에 없다면, 이 품번의 자막은 그 폴더에 없다는 의미.
            return False

        # 4. (Cache Miss) 디렉터리 스캔 수행
        if target_sub_dir.is_dir():
            logger.debug(f"자막 경로 스캔: {target_sub_dir}")
            for file in target_sub_dir.iterdir():
                if file.suffix.lower() in sub_config['자막파일확장자']:
                    parsed = ToolExpandFileProcess.parse_jav_filename(
                        file.stem, config.get('파싱규칙'), config.get('품번파싱제외키워드'), mode=config.get('parse_mode')
                    )
                    if parsed and parsed.get('code'):
                        # 이 폴더에서 찾은 모든 자막의 품번을 캐시에 True로 저장
                        subtitle_cache[parsed['code']] = True

        # 5. 스캔이 끝난 디렉터리를 캐시에 기록 (파일이 있든 없든)
        scanned_directories_cache.add(target_sub_dir)

        # 6. 스캔 후, 원래 찾으려던 품번이 캐시에 있는지 최종 확인하여 반환
        return subtitle_cache.get(pure_code, False)


    @staticmethod
    def __move_to_no_label_folder(config, file_path: Path):
        """품번 추출에 실패한 파일을 '처리실패이동폴더/[NO LABEL]'로 이동시킵니다."""

        target_root_str = config.get('처리실패이동폴더', '').strip()
        if not target_root_str:
            logger.warning(f"'{file_path.name}'을 이동할 '처리실패이동폴더'가 설정되지 않았습니다.")
            return

        target_dir = Path(target_root_str).joinpath("[NO LABEL]")
        target_file = target_dir.joinpath(file_path.name)

        is_dry_run = config.get('드라이런', False)
        if is_dry_run:
            log_msg = f"[Dry Run] 품번 추출 실패: '{file_path.name}' -> '{target_file}' (이동 예정)"
            logger.warning(log_msg)
            return

        try:
            target_dir.mkdir(parents=True, exist_ok=True)

            if target_file.exists():
                logger.warning(f"NO LABEL 폴더에 동일 파일명이 존재하여 원본을 삭제합니다: {file_path.name}")
                file_path.unlink()
                return

            shutil.move(file_path, target_file)
            logger.info(f"품번 추출 실패 파일을 이동: {target_file}")
        except Exception as e:
            logger.error(f"{file_path.name} 이동 중 오류: {e}")


    @staticmethod
    def __add_meta_no_path(module_name):
        """메타 매칭 실패 파일을 주기적으로 재시도하기 위해 스캔 목록에 추가합니다."""
        meta_no_path_str = ModelSetting.get(f'{module_name}_meta_no_path').strip()
        if not meta_no_path_str: return []

        if '{' in meta_no_path_str:
            meta_no_path_str = meta_no_path_str.split('{')[0].rstrip('/\\')

        meta_no_path = Path(meta_no_path_str)
        if not meta_no_path.is_dir(): return []

        retry_every = ModelSetting.get_int(f"{module_name}_meta_no_retry_every")
        if retry_every <= 0: return []

        last_retry_key = f"{module_name}_meta_no_last_retry"
        last_retry_str = ModelSetting.get(last_retry_key)
        if (datetime.now() - datetime.fromisoformat(last_retry_str)).days < retry_every:
            return []

        ModelSetting.set(last_retry_key, datetime.now().isoformat())
        logger.info(f"매칭 실패 파일 재시도 폴더를 스캔 목록에 추가: {meta_no_path}")
        return [meta_no_path]


    @staticmethod
    def __collect_initial_files(config, module_name):
        """파일 시스템에서 처리할 초기 파일 목록을 수집하고, 통계를 반환합니다."""

        temp_path_str = config.get('처리실패이동폴더', '').strip()
        if not temp_path_str:
            logger.warning("'처리실패이동폴더'가 설정되지 않았습니다. 전처리 중 발생하는 실패 파일은 이동되지 않습니다.")
            temp_path = None
        else:
            temp_path = Path(temp_path_str)

        src_list = Task.get_path_list(config['다운로드폴더'])

        # 주기적 재시도
        src_list.extend(Task.__add_meta_no_path(module_name))

        all_files = []
        for src in src_list:
            ToolExpandFileProcess.preprocess_cleanup(
                src, 
                config.get('최소크기', 0), 
                config.get('최대기간', 0),
                config['subtitle_exts']
            )
            _f = ToolExpandFileProcess.preprocess_listdir(src, temp_path, config)
            all_files.extend(_f or [])

        from collections import Counter
        stats = Counter()

        video_exts = config.get('video_exts', set())
        subtitle_exts = config.get('subtitle_exts', set())
        for file in all_files:
            ext = file.suffix.lower()
            if ext in video_exts:
                stats['video'] += 1
            elif ext in subtitle_exts:
                stats['subtitle'] += 1
            else:
                stats['etc'] += 1

        total_count = sum(stats.values())
        video_count = stats.get('video', 0)
        subtitle_count = stats.get('subtitle', 0)
        etc_count = stats.get('etc', 0)

        log_msg = f"파일 수집 완료: 총 {total_count}개 "
        log_msg += f"(동영상: {video_count}개, 자막: {subtitle_count}개"
        # etc 파일이 있을 때만 로그에 포함
        if etc_count > 0:
            log_msg += f", 기타: {etc_count}개"
        log_msg += ")"
        logger.info(log_msg)

        # --- 최종 파일 리스트 반환 ---
        # all_files.sort(key=lambda p: [int(c) if c.isdigit() else c.lower() for c in re.split('([0-9]+)', p.name)])

        return all_files


    @staticmethod
    def __prepare_initial_info(config, file):
        """파일을 파싱하여 메타 검색 이전에 가능한 모든 정보를 추출합니다."""
        parsing_rules = config.get('파싱규칙')
        cleanup_list = config.get('품번파싱제외키워드')
        mode = config.get('parse_mode')

        parsed = ToolExpandFileProcess.parse_jav_filename(file.name, parsing_rules, cleanup_list, mode=mode)
        if not parsed:
            return {'is_parsed': False, 'original_file': file, 'file_type': 'unparsed'}

        ext = file.suffix.lower()
        if ext in config.get('video_exts', set()):
            file_type = 'video'
        elif ext in config.get('subtitle_exts', set()):
            file_type = 'subtitle'
        elif ext in {'.json', '.txt', '.yaml', '.nfo'}:
            file_type = 'meta'
        else:
            file_type = 'etc'

        txt_file = file.with_suffix('.txt')
        json_file = file.with_suffix('.json')
        manual_url = None
        local_meta_json = None
        
        if file_type == 'video':
            if txt_file.exists() and txt_file.is_file():
                try:
                    with open(txt_file, 'r', encoding='utf-8') as f:
                        content = f.read().strip()
                        if content.startswith('http://') or content.startswith('https://'):
                            manual_url = content
                            logger.info(f"수동 매칭 JAV TXT 감지: '{file.name}' -> {manual_url}")
                except Exception as e:
                    logger.error(f"수동 JAV TXT 읽기 에러: {e}")

            # 로컬 JSON 메타데이터 파일 확인
            if json_file.exists() and json_file.is_file():
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        local_meta_json = json.load(f)
                        logger.info(f"로컬 메타데이터 JSON 감지: '{json_file.name}'")
                except Exception as e:
                    logger.error(f"로컬 메타데이터 JSON 읽기 에러: {e}")

        info = {
            'original_file': file,
            'file_size': file.stat().st_size,
            'file_type': file_type,
            'is_parsed': True,
            'pure_code': parsed['code'],
            'search_keyword': parsed.get('search_keyword'),
            'label': parsed['label'],
            'number': parsed['number'],
            'raw_number': parsed['raw_number'],
            'ext': parsed['ext'],
            'meta_info': None,
            'media_info': None,
            'manual_url': manual_url,
            'local_meta_json': local_meta_json
        }
        ext_config = config.get('미디어정보설정', {})
        if config.get('파일명에미디어정보포함') and info.get('file_type') == 'video':
            info['media_info'] = ToolExpandFileProcess._get_media_info(file, ext_config)

        return info


    @staticmethod
    def __process_part_sets(execution_plan):
        """
        분할 파일 처리 로직
        """

        video_infos = [info for info in execution_plan if info.get('file_type') == 'video' and not info.get('is_part_of_set')]
        code_groups = defaultdict(list)
        for info in video_infos:
            code_groups[info['pure_code']].append(info)

        for pure_code, group_infos in code_groups.items():
            all_infos_in_group = list(group_infos)

            while len(all_infos_in_group) >= 2:
                all_infos_in_group.sort(key=lambda i: [int(c) if c.isdigit() else c.lower() for c in re.split('([0-9]+)', i['original_file'].stem)])

                info1, info2 = all_infos_in_group[0], all_infos_in_group[1]
                stem1, stem2 = info1['original_file'].stem, info2['original_file'].stem

                prefix = os.path.commonprefix([stem1, stem2])
                suffix = os.path.commonprefix([stem1[::-1], stem2[::-1]])[::-1]

                part1_raw = stem1[len(prefix):len(stem1)-len(suffix)]
                part2_raw = stem2[len(prefix):len(stem2)-len(suffix)]

                # Prefix 정리
                if prefix.endswith('0'):
                    part1_clean = part1_raw.strip(' _.-()')
                    if part1_clean in ['1', '01']:
                        if not prefix.upper().endswith(info1['label'].upper() + '0'):
                            prefix = prefix[:-1]
                            part1_raw = stem1[len(prefix):len(stem1)-len(suffix)]
                            part2_raw = stem2[len(prefix):len(stem2)-len(suffix)]

                def is_valid_part(p): return p and len(p) < 6 and p.strip(' _.-()').isalnum()

                if not (prefix and is_valid_part(part1_raw) and is_valid_part(part2_raw)):
                    all_infos_in_group.pop(0)
                    continue

                release_set_infos = []
                next_remaining_infos = []
                for info in all_infos_in_group:
                    stem = info['original_file'].stem
                    if stem.startswith(prefix) and stem.endswith(suffix):
                        part_str = stem[len(prefix):len(stem)-len(suffix)]
                        if is_valid_part(part_str):
                            release_set_infos.append(info)
                        else:
                            next_remaining_infos.append(info)
                    else:
                        next_remaining_infos.append(info)

                all_infos_in_group = next_remaining_infos
                if len(release_set_infos) < 2: continue

                part_items = []
                for info in release_set_infos:
                    part_str = info['original_file'].stem[len(prefix):len(info['original_file'].stem)-len(suffix)]
                    part_items.append({'info': info, 'part_str': part_str})

                # --- 2C & 2D: 검증 및 할당 ---
                items_to_process = []
                valid_set = True
                part_type = None
                for item in part_items:
                    part_str = item['part_str'].strip(' _.-()')
                    c_type, key = None, None
                    if part_str.lower().startswith('cd') and part_str[2:].isdigit(): c_type, key = 'cd', (int(part_str[2:]),)
                    elif part_str.isdigit(): c_type, key = 'digit', (int(part_str),)
                    elif part_str.isalpha() and len(part_str) == 1: c_type, key = 'alpha', (ord(part_str.lower()) - ord('a') + 1,)
                    elif re.match(r'^[a-zA-Z](\d+)$', part_str, re.I):
                        match = re.match(r'^([a-zA-Z])(\d+)$', part_str, re.I)
                        alpha_part, digit_part = match.groups()
                        c_type, key = 'alpha_digit', (ord(alpha_part.lower()), int(digit_part))
                    else: valid_set = False; break
                    if part_type is None: part_type = c_type
                    elif part_type != c_type: valid_set = False; break
                    items_to_process.append({'info': item['info'], 'sort_key': key})

                if not valid_set: continue
                items_to_process.sort(key=lambda x: x['sort_key'])

                first_key = items_to_process[0]['sort_key']
                if part_type in ['cd', 'digit'] and first_key[0] not in [0, 1]: continue
                if part_type == 'alpha' and first_key[0] > 1: continue
                if part_type == 'alpha_digit' and not (first_key[0] == 0 and first_key[1] == 1): continue

                is_continuous = True
                if len(items_to_process) > 1:
                    for i in range(1, len(items_to_process)):
                        p, c = items_to_process[i-1]['sort_key'], items_to_process[i]['sort_key']
                        if len(p) != len(c): is_continuous = False; break
                        if len(p) == 2 and p[0] == c[0] and p[1] + 1 == c[1]: continue
                        elif len(p) == 2 and p[0] + 1 == c[0] and (c[1] == 1 or c[1] == 0): continue
                        elif len(p) == 1 and p[0] + 1 == c[0]: continue
                        else: is_continuous = False; break

                if not is_continuous: continue

                logger.info(f"'{pure_code}' 그룹에서 유효한 분할 파일 세트(prefix='{prefix}', suffix='{suffix}')를 발견했습니다.")
                total_size = sum(item['info']['file_size'] for item in items_to_process)
                for i, item in enumerate(items_to_process):
                    info = item['info']
                    info.update({'is_part_of_set': True, 'parsed_part_type': f"cd{i + 1}",
                                'part_set_total_size': total_size, 'part_set_prefix': prefix,
                                'part_set_number': '', 'part_set_suffix': suffix})

        # --- 3. 최종 정리 ---
        part_groups = defaultdict(list)
        for info in execution_plan:
            if info.get('is_part_of_set'):
                part_groups[info['pure_code']].append(info)
        for pure_code, group_infos in part_groups.items():
            for info in group_infos:
                info['part_set_code'] = pure_code


    @staticmethod
    def _resolve_path_template(config: dict, info: dict, meta_info: dict, path_or_format: str) -> (Path, str):
        """
        경로 문자열을 해석하여 (기준 경로, 포맷 문자열)을 반환합니다.
        """
        if not path_or_format:
            return None, None
        
        if '{' in path_or_format and '}' in path_or_format:
            # 경로 자체가 템플릿인 경우
            if path_or_format.startswith('/'):
                return Path('/'), path_or_format.lstrip('/')
            else:
                return Path(), path_or_format
        else:
            # 고정 경로인 경우, 포맷은 비워둠
            return Path(path_or_format), ""


    @staticmethod
    def _get_final_target_path(config, info, task_context, do_meta_search=False, preloaded_meta=None):
        use_meta_option = config.get('메타사용', 'not_using')
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
            matched_custom_rule = Task._find_and_merge_custom_path_rules(info, config.get('커스텀경로규칙', []), meta_info)
            if matched_custom_rule and not (is_meta_success or matched_custom_rule.get('force_on_meta_fail') or matched_custom_rule.get('메타실패시강제적용')):
                matched_custom_rule = None

        # --- 3. 기본 경로 및 타입 결정 ---
        if is_meta_success:
            if use_meta_option == 'using':
                if config.get('메타매칭시이동폴더'):
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
            
            # VR 처리
            if meta_info:
                vr_path_setting = config.get('VR영상이동폴더', '').strip()
                vr_genres = ["VR専用", "ハイクオリティVR", "VR", "Virtual Reality"]
                original_data = meta_info.get('original')
                genres_to_check = ((original_data.get('genre') if isinstance(original_data, dict) else meta_info.get('genre')) or [])
                if vr_path_setting and any(x in genres_to_check for x in vr_genres):
                    base, format_ = Task._resolve_path_template(config, info, meta_info, vr_path_setting)
                    if base is not None:
                        final_path_str = str(base)
                        if format_: final_format_str = format_
                        final_move_type = "vr"
        else: 
            # 메타 실패 처리
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
                    has_external = Task._find_external_subtitle(config, info, sub_config, task_context)
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

        base_path, path_template = Task._resolve_path_template(config, info, meta_info, final_path_str)

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

        folders = Task.process_folder_format(config, info, final_format_str, meta_info)
        target_dir = base_path.joinpath(*folders)

        return target_dir, final_move_type, meta_info


    @staticmethod
    def _get_metadata(config, info):
        """
        메타데이터 검색을 수행하고, 성공 시 meta_info 객체를 반환합니다.
        """
        # 로컬 JSON 메타데이터 최우선 처리
        local_meta = info.get('local_meta_json')
        if local_meta:
            logger.info(f"'{info['pure_code']}' 로컬 JSON 메타데이터를 사용하여 검색을 건너뜁니다.")
            return local_meta

        meta_module = Task.get_meta_module('jav_censored')
        if not meta_module:
            logger.error("메타데이터 플러그인을 찾을 수 없습니다. 메타 검색을 건너뜁니다.")
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

        # 기본 검색어 설정 (파싱된 품번)
        search_name = info.get('search_keyword') or info['pure_code']
        
        # 변수 사전 정의 (예외 방지)
        manual_url = info.get('manual_url')
        best_match = None
        match_site = "N/A"
        meta_info = None

        # 수동 URL 매칭 우선 처리
        if manual_url:
            logger.info(f"'{info['pure_code']}' JAV 수동 URL 매칭 시도: {manual_url}")
            try:
                res = meta_module.search(manual_url, manual=True)
                if res and isinstance(res, list) and len(res) > 0:
                    if res[0].get('score', 0) >= 95:
                        best_match = res[0]
            except Exception as e:
                logger.error(f"JAV 수동 URL 검색 중 예외 발생: {e}")
                logger.error(traceback.format_exc())

        # 수동 매칭이 없거나 실패한 경우, 일반 자동 검색 진행
        if not best_match:
            label = ""
            try:
                if '-' in search_name:
                    label = search_name.split("-")[0].lower()
                else:
                    label = search_name.lower()
            except Exception as e:
                logger.error(f"검색어에서 레이블 추출 중 오류 ('{search_name}'): {e}")

            if label and label in config.get('메타매칭제외레이블', []):
                logger.info(f"'{label}'은(는) '정상 매칭 제외 레이블'에 포함되어 메타 검색을 건너뜁니다.")
                return None

            custom_search_settings = config.get('메타검색에사용할사이트')

            if custom_search_settings:
                site_list = [s for s in custom_search_settings if not config.get('메타검색에공식사이트만사용', False) or s.get('사이트') in ['dmm', 'mgstage']]
                for rule in site_list:
                    site_name, min_score = rule.get('사이트'), rule.get('점수', 95)
                    if not site_name: continue
                    try:
                        res = meta_module.search2(search_name, site_name, manual=False)
                        if res and isinstance(res, list):
                            match = next((item for item in res if item.get('score', 0) >= min_score), None)
                            if match:
                                logger.debug(f"'{site_name}' 사이트에서 '{search_name}'에 대한 유효한 매치(점수: {match.get('score')})를 찾았습니다. 검색을 중단합니다.")
                                best_match = match
                                break 
                    except Exception as e:
                        logger.error(f"'{site_name}' 사이트 검색 중 예외 발생: {e}")
            else:
                try:
                    res = meta_module.search(search_name, manual=False)
                    if res and isinstance(res, list) and len(res) > 0:
                        if res[0].get('score', 0) >= 95:
                            best_match = res[0]
                except Exception as e:
                    logger.error(f"메타데이터 통합 검색(search) 중 예외 발생: {e}")

        # 매칭된 결과가 있으면 상세 정보 조회
        if best_match:
            try:
                keyword_param = info.get('search_keyword') if manual_url else search_name
                meta_info = meta_module.info(best_match["code"], keyword=keyword_param, extra_opts=extra_opts)
                if meta_info:
                    match_site = best_match.get('site', 'N/A')
            except Exception as e:
                logger.error(f"'{best_match['code']}'의 상세 정보 획득 중 예외 발생: {e}")
                logger.error(traceback.format_exc())
                
        if meta_info:
            logger.info(f"'{info['pure_code']}' 메타 검색 성공: {meta_info.get('originaltitle')} (from: {match_site}, 번역스킵:{extra_opts.get('skip_trans', '기본값')}, 공유모드:{is_shared})")
            return meta_info
        else:
            logger.info(f"'{info['pure_code']}'에 대한 유효한 메타 정보를 찾지 못했습니다.")
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
        
        # 스캔 대상으로 인정할 유효한 이동 타입 정의
        valid_scan_types = {
            'dvd', 'normal', 'subbed', 'custom_path', 
            'companion_kor', 'companion_kor_sub',
            'meta_success', 'vr', 'featurette_video'
        }
        if config.get('매칭실패이동후스캔', False):
            valid_scan_types.update(['no_meta', 'meta_fail'])
        
        failed_types = {'failed_video', 'etc_file', 'meta_fail_skipped', 'no_meta_deleted_due_to_duplication'}

        # 전역 옵션 캐싱
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
            
            # 경로 계산 1회 수행 및 재사용
            config['_skip_trans_temp'] = (not any_meta_option_on) or (not make_overwrite)
            target_dir_for_group, _, meta_info_for_group = Task._get_final_target_path(config, first_info, task_context, do_meta_search=True)

            # 로컬 메타가 없을 때만 누락 파일 검사 및 재요청
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

            # 메타데이터 품번 동기화
            if meta_info_for_group and meta_info_for_group.get('originaltitle'):
                meta_code_raw = meta_info_for_group['originaltitle']
                meta_code_lower = meta_code_raw.lower()
                
                if pure_code != meta_code_lower:
                    if '-' in meta_code_lower:
                        meta_label, meta_number = meta_code_lower.rsplit('-', 1)
                    else:
                        meta_label, meta_number = meta_code_lower, ""

                    for info in group_infos:
                        info['pure_code'] = meta_code_lower
                        info['label'] = meta_label
                        info['number'] = meta_number 
                        
                        from .tool import ToolExpandFileProcess
                        new_assembled_name = ToolExpandFileProcess.assemble_filename(config, info)
                        info['newfilename'] = new_assembled_name
                        
                    logger.info(f"메타 품번 동기화: '{pure_code}' -> '{meta_code_lower}'")

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
                            base_path, format_str = Task._resolve_path_template(config, info, meta_info_for_group, failed_path_str)
                            folders = Task.process_folder_format(config, info, format_str, meta_info_for_group)
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

                    # 1. 사전 부가 파일 생성
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
                                    make_json=config.get('부가파일생성_JSON', False) and not local_json_moved, # 이미 옮겼으면 생략!
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

                    # 2. 동반 자막 이동
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
                            s_entity = Task.__file_move_logic(config, s_info, db_model)
                            if s_entity and s_entity.target_path: 
                                s_entity.save()

                    # 3. 본 영상 이동
                    entity = Task.__file_move_logic(config, info, db_model)
                    
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

                        # 4. 스캔 요청: 모든 파일 이동이 끝난 후, 대상이 '영상(video)'일 때만 1회 즉시 호출
                        if scan_enabled and entity and entity.target_path and entity.move_type in valid_scan_types:
                            if info.get('file_type') == 'video':
                                Task.__request_plex_mate_scan(config, Path(entity.target_path), entity)
                
                except Exception as e:
                    logger.error(f"'{info.get('pure_code', '알 수 없음')}' 파일 처리 중 예외 발생: {e}")
                    logger.error(traceback.format_exc())

        logger.info("fp_av_jav_censored: 모든 작업이 완료되었습니다.")


    @staticmethod
    def __file_move_logic(config, info, model_class):
        """실제 파일 이동, 중복 처리, DB 기록, 방송 등을 담당합니다."""
        is_dry_run = config.get('드라이런', False)
        file = info['original_file']
        newfilename = info.get('newfilename', file.name)
        target_dir = info.get('target_dir')
        move_type = info.get('move_type')
        meta_info = info.get('meta_info')

        entity = model_class(config.get('이름'), str(file.parent), file.name)

        if move_type is None or target_dir is None:
            logger.warning(f"'{file.name}'의 최종 이동 경로를 결정할 수 없어 건너뜁니다.")
            return entity.set_move_type(None)

        newfile = target_dir.joinpath(newfilename)

        # Dry Run 모드일 경우, 로그만 남기고 조기 리턴
        if is_dry_run:
            log_msg = f"[Dry Run] 원본: {file}\n"
            log_msg += f"         이동: {newfile} (타입: {move_type})\n"

            if move_type == "no_meta" and not config.get('메타매칭실패시이동폴더', ''):
                log_msg += f"         결과: '메타매칭실패시이동폴더'가 비어있어 건너뛸 예정"
            elif move_type == "failed_video" and not config.get('처리실패이동폴더', ''):
                log_msg += f"         결과: '처리실패이동폴더'가 비어있어 건너뛸 예정"
            elif UtilFunc.is_duplicate(file, newfile, config):
                remove_path_str = config.get('중복파일이동폴더', '')
                if not remove_path_str:
                    log_msg += f"         결과: 중복 파일, '중복파일이동폴더'가 비어있어 건너뛸 예정"
                else:
                    log_msg += f"         결과: 중복 파일, {remove_path_str}로 이동될 예정"
            else:
                log_msg += f"         결과: 정상적으로 이동될 예정"

            logger.warning(log_msg)
            return None

        # 미디어 분석 실패(failed_video) 처리
        if move_type == "failed_video":
            failed_video_path_str = config.get('처리실패이동폴더', '').strip()
            if not failed_video_path_str:
                logger.info(f"미디어 분석 실패: '처리실패이동폴더'가 비어있어 이동을 건너뜁니다: {file.name}")
                return entity.set_move_type("failed_video_skipped")

            target_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(file, newfile)
            logger.info(f"미디어 분석 실패 파일을 이동했습니다: {newfile}")

            return entity.set_target(newfile).set_move_type(move_type)

        if move_type in ["no_meta", "meta_fail"]:
            target_dir.mkdir(parents=True, exist_ok=True)
            if newfile.exists():
                file.unlink()
                return entity.set_move_type("no_meta_deleted_due_to_duplication")
            else:
                shutil.move(file, newfile)
                logger.info(f"메타 매칭 실패 파일을 이동했습니다: {newfile}")
                return entity.set_target(newfile).set_move_type(move_type)

        # 타겟 경로 내 중복 처리
        if UtilFunc.is_duplicate(file, newfile, config):
            logger.info(f"타겟 경로에 동일 파일이 존재합니다: {newfile}")
            remove_path_str = config.get('중복파일이동폴더', '').strip()

            if not file.exists():
                return entity.set_move_type("already_processed_by_other")

            if not remove_path_str:
                logger.info(f"중복 파일: '중복파일이동폴더'가 비어있어 이동/삭제를 건너뜁니다: {file.name}")
                return entity.set_move_type("duplicate_skipped")
            else:
                remove_path = Path(remove_path_str)
                remove_path.mkdir(parents=True, exist_ok=True)

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
            try:
                # 파일 이동
                target_dir.mkdir(parents=True, exist_ok=True) 
                shutil.move(file, newfile)
                logger.info(f"파일 이동 성공: -> {newfile}")

                # 방송 처리 (이동 성공 후)
                if config.get('방송', False) and info.get('file_type') == 'video':
                    gds_path_str = str(newfile)
                    replace_rules = config.get('방송경로변환')

                    if replace_rules and isinstance(replace_rules, dict):
                        for src, tgt in replace_rules.items():
                            if src in gds_path_str:
                                gds_path_str = gds_path_str.replace(src, tgt)
                                break

                    bot = {
                        't1': 'gds_tool', 
                        't2': 'fp', 
                        't3': config.get('module_name', 'av'),
                        'data': {'gds_path': gds_path_str}
                    }
                    logger.debug(f"방송(Discord) 전송 데이터: {bot}")
                    
                    hook = base64.b64decode(b'aHR0cHM6Ly9kaXNjb3JkLmNvbS9hcGkvd2ViaG9va3MvMTM5OTkxMDg4MDE4NzEyNTgxMS84SFY0bk93cGpXdHhIdk5TUHNnTGhRbDhrR3lGOXk4THFQQTdQVTBZSXVvcFBNN21PWHhkSVJSNkVmcmIxV21UdFhENw==').decode('utf-8')
                    SupportDiscord.send_discord_bot_message(json.dumps(bot), hook)

                return entity.set_target(newfile).set_move_type(move_type)

            except Exception as e:
                logger.error(f"최종 파일 이동 또는 후속 작업 중 오류: {file} -> {newfile}, 오류: {e}")
                logger.error(traceback.format_exc())
                return entity.set_move_type("move_fail")

        return entity.set_move_type(None)


    @staticmethod
    def _merge_and_standardize_media_info(set_infos, config):
        """
        분할 파일 세트의 미디어 정보를 '필드별'로 비교하여,
        '일치하는 정보만'을 포함하는 대표 미디어 딕셔너리를 생성합니다.
        """
        ext_config = config.get('미디어정보설정', {})

        # --- 1. 치명적 오류 검사 (비디오/오디오 스트림 부재) ---
        failed_files = []
        valid_media_infos = []
        for info in set_infos:
            media_info = info.get('media_info')
            # media_info 자체가 없거나, is_valid가 False인 경우 치명적 오류로 판단
            if not media_info or not media_info.get('is_valid', False):
                failed_files.append(info)
            else:
                valid_media_infos.append(media_info)

        # 치명적 오류 파일이 하나라도 있으면, 그룹 해제를 위해 실패를 반환
        if failed_files:
            return {
                'is_valid_set': False,
                'reason': 'Critical media info failure in set',
                'failed_files': failed_files
            }

        # --- 2. 모든 파일이 유효할 경우, 일치하는 정보만 추출하여 대표 미디어 정보 구성 ---
        if not valid_media_infos: # 모든 파일이 유효하지 않은 극단적 케이스
            return {'is_valid_set': False, 'reason': 'No valid media info found', 'failed_files': set_infos}

        base_media = valid_media_infos[0]
        final_media = {'is_valid': True} # 이 딕셔너리는 항상 유효함
        tolerance = ext_config.get('tolerance', {})

        # 각 필드별로 일관성 검사 후, 일치할 때만 final_media에 추가
        # 해상도 태그 (res_tag)
        if len({mi.get('res_tag') for mi in valid_media_infos}) == 1:
            final_media['res_tag'] = base_media.get('res_tag')
            final_media['width'] = base_media.get('width')
            final_media['height'] = base_media.get('height')

        # 비디오 코덱 (v_codec)
        if len({mi.get('v_codec') for mi in valid_media_infos}) == 1:
            final_media['v_codec'] = base_media.get('v_codec')

        # 오디오 코덱 (a_codec)
        if len({mi.get('a_codec') for mi in valid_media_infos}) == 1:
            final_media['a_codec'] = base_media.get('a_codec')

        # FPS (허용 오차 내)
        fps_list = [mi.get('fps_float', 0.0) for mi in valid_media_infos]
        if max(fps_list) - min(fps_list) <= tolerance.get('fps', 0.01):
            final_media['fps_float'] = base_media.get('fps_float')
            final_media['fps'] = base_media.get('fps')

        # 오디오 비트레이트 (허용 오차 내)
        bitrate_list = [mi.get('a_bitrate', 0) for mi in valid_media_infos]
        if max(bitrate_list) - min(bitrate_list) <= tolerance.get('audio_bitrate', 5):
            final_media['a_bitrate'] = base_media.get('a_bitrate')

        # Tag Title (하나 이하의 고유한 non-empty 타이틀이 있을 때만 일치로 간주)
        tag_titles = {mi.get('tag_title') for mi in valid_media_infos if mi.get('tag_title')}
        if len(tag_titles) <= 1:
            final_media['tag_title'] = base_media.get('tag_title')

        # is_valid_set: True와 함께, 일치하는 정보만 담긴 final_media 반환
        return {'is_valid_set': True, 'final_media': final_media}


    @staticmethod
    def process_folder_format(config, info, format_str, meta_data=None):
        """
        파싱 정보(info)를 기반으로 폴더 경로를 생성합니다.
        메타 정보(meta_data)가 있으면 해당 정보를 우선적으로 사용합니다.
        """
        folder_format = format_str.strip()
        if not folder_format:
            return []

        is_western = config.get('parse_mode') == 'western'
        safe_fn = ToolExpandFileProcess.get_safe_filename

        data = {}
        data['filename'] = info['original_file'].stem

        base_label = ""
        number_part_raw = ""

        if meta_data and isinstance(meta_data, dict):
            if is_western:
                title_str = safe_fn(meta_data.get("originaltitle", ""))
                studio_str = safe_fn(meta_data.get("studio") or info.get('studio', 'NO_STUDIO'))
                code_str = safe_fn(info['original_file'].stem)
                base_label = studio_str 
            else:
                original_title = safe_fn(meta_data.get("originaltitle", ""))
                code_parts = original_title.split('-', 1)
                base_label = code_parts[0]

                if len(code_parts) > 1:
                    number_part_raw = code_parts[1]
                else:
                    num_match = re.search(r'\d+', original_title)
                    number_part_raw = num_match.group(0) if num_match else ''

                studio_str = safe_fn(meta_data.get("studio") or "NO_STUDIO")
                title_str = safe_fn(meta_data.get("title", original_title))
                code_str = original_title
            
            actor_list = meta_data.get('actor') or []
            actor_names = [safe_fn(actor.get('name_ko') or actor.get('name_org', '')) for actor in actor_list[:3] if actor.get('name_ko') or actor.get('name_org')]
            year_str = str(meta_data.get("year")) if meta_data.get("year") is not None else ""
            
            data.update({
                "studio": studio_str,
                "code": code_str, 
                "title": title_str, 
                "actor": f"{','.join(actor_names[:1])}",
                "actor_2": f"{','.join(actor_names[:2])}",
                "actor_3": f"{','.join(actor_names[:3])}",
                "year": year_str,
            })
        else:
            base_label = safe_fn(info.get('label', ''))
            
            number_part_raw = info.get('number', '')
            if not number_part_raw:
                num_match = re.search(r'\d+', info.get('pure_code', ''))
                number_part_raw = num_match.group(0) if num_match else ''

            title_str = safe_fn(info['original_file'].stem)
            
            if is_western:
                data.update({
                    "code": title_str, 
                    "title": title_str,
                    "year": "", "actor": "", 
                    "studio": safe_fn(info.get('studio', base_label))
                })
            else:
                data.update({
                    "code": safe_fn(info.get('pure_code', '').upper()),
                    "title": title_str,
                    "year": "", "actor": "", "studio": base_label.upper()
                })

        label_1 = '#'
        studio_1 = '#'
        title_1 = '#'
        filename_1 = '#'
        actor_1 = '#'
        
        processed_label = base_label
        processed_studio = data.get('studio', '')
        processed_title = data.get('title', '')
        processed_filename = data.get('filename', '')
        processed_actor = data.get('actor', '').split(',')[0].strip()

        # label_1 처리
        if base_label:
            if is_western:
                processed_label = base_label
                if base_label[0].isdigit():
                    label_1 = "09"
                elif base_label[0].isalpha():
                    label_1 = base_label[0].upper()
                else:
                    match = re.search(r'[a-zA-Z0-9]', base_label)
                    label_1 = "09" if match and match.group(0).isdigit() else (match.group(0).upper() if match else '#')
            else:
                # JAV 특화 로직 (숫자 레이블 등)
                if base_label[0].isdigit():
                    allowed_regex = config.get('허용된숫자레이블')
                    if allowed_regex and re.match(allowed_regex, base_label, re.IGNORECASE):
                        label_1 = "09"
                        processed_label = base_label
                    else:
                        first_alpha_match = re.search(r'[a-zA-Z]', base_label)
                        if first_alpha_match:
                            label_1 = first_alpha_match.group(0).upper()
                            processed_label = base_label[first_alpha_match.start():]
                        else:
                            processed_label = base_label
                else:
                    label_1 = base_label[0].upper()
                    processed_label = base_label

        # studio_1 처리
        if processed_studio:
            if is_western:
                match = re.search(r'[a-zA-Z0-9]', processed_studio)
                studio_1 = "09" if match and match.group(0).isdigit() else (match.group(0).upper() if match else '#')
            else:
                # 순수 알파벳 첫 글자 추출 (JAV)
                match = re.search(r'[a-zA-Z]', processed_studio)
                studio_1 = match.group(0).upper() if match else '#'

        # title_1 처리
        if processed_title:
            if is_western:
                match = re.search(r'[a-zA-Z0-9]', processed_title)
                title_1 = "09" if match and match.group(0).isdigit() else (match.group(0).upper() if match else '#')
            else:
                match = re.search(r'[a-zA-Z]', processed_title)
                title_1 = match.group(0).upper() if match else '#'

        # filename_1 처리
        if processed_filename:
            if is_western:
                match = re.search(r'[a-zA-Z0-9]', processed_filename)
                filename_1 = "09" if match and match.group(0).isdigit() else (match.group(0).upper() if match else '#')
            else:
                match = re.search(r'[a-zA-Z]', processed_filename)
                filename_1 = match.group(0).upper() if match else '#'

        # actor_1 처리 (첫 번째 배우의 첫 알파벳)
        if processed_actor:
            match = re.search(r'[a-zA-Z가-힣]', processed_actor) 
            actor_1 = match.group(0).upper() if match else '#'

        # 딕셔너리에 업데이트
        data.update({
            'label_1': label_1,
            'studio_1': studio_1,
            'title_1': title_1,
            'filename_1': filename_1,
            'actor_1': actor_1
        })

        if is_western:
            data['label'] = processed_label
            data['label_lower'] = processed_label.lower()
            data['code_lower'] = data['code'].lower()
            if "studio" not in (meta_data or {}):
                data['studio'] = processed_label
                data['studio_lower'] = processed_label.lower()
        else:
            data['label'] = processed_label.upper()
            data['label_lower'] = processed_label.lower()
            data['code_lower'] = data['code'].lower()
            if "studio" not in (meta_data or {}):
                data['studio'] = processed_label.upper()
                data['studio_lower'] = processed_label.lower()

        # {num_X_Y}, {year4} 등 추가 변수 처리
        try:
            if '{' in folder_format and number_part_raw:
                main_number_part = re.split(r'[-_\s]', number_part_raw, 1)[0]
                for match in re.finditer(r'\{num_(\d+)_(\d+)\}', folder_format):
                    x, y = map(int, match.groups())
                    data[f'num_{x}_{y}'] = main_number_part.zfill(y)[:x]
                # 6자리 숫자 코드에 대한 특수 처리
                if re.match(r'^\d{6}$', main_number_part):
                    # {year4} 처리
                    if '{year4}' in folder_format:
                        yy = main_number_part[4:6]
                        data['year4'] = f"20{yy}" if int(yy) < 50 else f"19{yy}"
                    # {yymm} 처리
                    # MMDDYY 형식에서 YYMM 형식으로 변환 (예: 083025 -> 2508)
                    if '{yymm}' in folder_format:
                        mm = main_number_part[:2]
                        yy = main_number_part[4:6]
                        data['yymm'] = f"{yy}{mm}"
        except Exception as e:
            logger.error(f"폴더 포맷 변수 처리 중 오류: {e}")

        # 안전한 포맷팅 및 후처리
        safe_fmt = SafeFormatter()
        folders = safe_fmt.format(folder_format, **data)
        # 포맷팅 후에도 남아있는 {태그} 제거
        folders = re.sub(r'\{[a-zA-Z0-9_.-]+\}', '', folders)
        # 비어있는 모든 종류의 괄호 제거: (), [], {}
        folders = folders.replace("()", "").replace("[]", "").replace("{}", "")
        # 연속된 공백을 하나로 합치기
        folders = re.sub(r'\s{2,}', ' ', folders)
        # 폴더 구분자(/) 주변이나 문자열 끝의 불필요한 공백/특수문자 제거
        # 예: "A / B / C " -> "A/B/C"
        # 예: "A/ - B" -> "A/B"
        final_parts = [part.strip(' ._-') for part in folders.split('/')]
        # 비어있는 경로 세그먼트 제거
        final_parts = [part for part in final_parts if part]

        return final_parts


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


    @staticmethod
    def get_meta_module(module_name):
        """[공용] 지정된 이름의 메타데이터 모듈을 로드하고 캐싱합니다."""
        try:
            if module_name not in Task.metadata_modules:
                instance = F.PluginManager.get_plugin_instance("metadata")
                if instance:
                    Task.metadata_modules[module_name] = instance.get_module(module_name)
                else:
                    Task.metadata_modules[module_name] = None

            return Task.metadata_modules[module_name]
        except Exception as e:
            logger.error(f"메타데이터 모듈 '{module_name}' 로딩 중 오류: {e}")
            logger.debug(traceback.format_exc())
            Task.metadata_modules[module_name] = None
            return None


    @staticmethod
    def __request_plex_mate_scan(config, scan_path: Path, db_item=None):
        """Plex Mate에 웹 API를 통해 스캔을 요청합니다. (Localhost 방식 적용)"""
        is_dry_run = config.get('드라이런', False)

        if is_dry_run:
            logger.warning(f"[Dry Run] Plex Mate 스캔 요청 시뮬레이션: {scan_path}")
            return

        try:
            port = F.SystemModelSetting.get('port')
            url = f"http://127.0.0.1:{port}/plex_mate/api/scan/do_scan"

            callback_id = ''
            if db_item and db_item.id:
                callback_id = f"{P.package_name}_item_{db_item.id}"

            data = {
                'target': str(scan_path),
                'apikey': F.SystemModelSetting.get('apikey'),
                'mode': 'ADD',
                # 'scanner': 'web',
                'callback_id': callback_id
            }

            log_data = {'target': data['target']}
            logger.debug(f"Plex Mate 스캔 API 호출: URL={url}, Data={log_data}")
            
            res = requests.post(url, data=data, timeout=10)

            if res.status_code == 200:
                logger.info(f"Plex Mate 스캔 요청 성공: {res.json()}")
            else:
                logger.warning(f"Plex Mate 스캔 요청 실패: {res.status_code} - {res.text}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Plex Mate API 호출 중 네트워크 오류 (Localhost): {e}")
        except Exception as e:
            logger.error(f"Plex Mate 스캔 요청 중 알 수 없는 오류: {e}")
            logger.error(traceback.format_exc())


    @staticmethod
    def _find_and_merge_custom_path_rules(info, rules_list, meta_info=None):
        """
        규칙 리스트를 순회하며 파일 정보와 일치하는 모든 규칙을 찾아 병합합니다.
        값이 비어있지 않은 속성만 덮어쓰며, 리스트의 아래 규칙이 우선순위를 가집니다.
        """
        if not rules_list:
            return None

        merged_rule = {}
        has_any_match = False

        original_filename = info['original_file'].name
        actors_to_check = []
        studio_to_check = ""
        
        is_western = 'studio' in info and info['studio'] == info['label']
        label_to_check = info['label'].lower()

        if meta_info:
            if is_western:
                # 서양은 메타데이터의 'studio' 값을 무조건 커스텀 룰 매칭용 레이블로 씁니다.
                if meta_info.get("studio"):
                    label_to_check = meta_info.get("studio").lower()
            else:
                if meta_info.get("originaltitle"):
                    label_to_check = meta_info.get("originaltitle").split('-')[0].lower()
            
            if meta_info.get("actor"):
                actors_to_check = [(a.get('name_ko') or a.get('name_org', '')).strip() for a in (meta_info.get('actor') or []) if (a.get('name_ko') or a.get('name_org', '')).strip()]
            if meta_info.get("studio"):
                studio_to_check = meta_info.get("studio").strip()

        for rule in rules_list:
            rule_name = rule.get('name') or rule.get('이름', '이름 없는 규칙')
            is_and_condition = rule.get('다중조건매칭', False)
            filename_pattern = rule.get('filename_pattern') or rule.get('파일명패턴')
            label_pattern = rule.get('label_pattern') or rule.get('레이블')
            actor_pattern = rule.get('actor_pattern') or rule.get('배우')
            studio_pattern = rule.get('studio_pattern') or rule.get('스튜디오')

            if filename_pattern: filename_pattern = re.sub(r'[\r\n]+', '', filename_pattern)
            if label_pattern: label_pattern = re.sub(r'\s+', '', label_pattern)
            if actor_pattern: actor_pattern = re.sub(r'[\r\n]+', '', actor_pattern)
            if studio_pattern: studio_pattern = re.sub(r'\s+', '', studio_pattern)

            # 각 패턴의 매칭 여부를 저장할 딕셔너리 (패턴이 없으면 True로 간주하여 AND 조건 우회)
            match_results = {
                'filename': True if not filename_pattern else False,
                'label': True if not label_pattern else False,
                'actor': True if not actor_pattern else False,
                'studio': True if not studio_pattern else False
            }

            try:
                if filename_pattern and re.search(filename_pattern, original_filename, re.IGNORECASE):
                    match_results['filename'] = True
                    logger.debug(f"[{rule_name}] 파일명 매칭 성공")

                if label_pattern and label_to_check and re.fullmatch(label_pattern, label_to_check, re.IGNORECASE):
                    match_results['label'] = True
                    logger.debug(f"[{rule_name}] 레이블 매칭 성공")

                if actor_pattern and actors_to_check and any(re.search(actor_pattern, a, re.IGNORECASE) for a in actors_to_check):
                    match_results['actor'] = True
                    logger.debug(f"[{rule_name}] 배우 매칭 성공")

                if studio_pattern and studio_to_check and re.search(studio_pattern, studio_to_check, re.IGNORECASE):
                    match_results['studio'] = True
                    logger.debug(f"[{rule_name}] 스튜디오 매칭 성공")
                    
            except re.error as e:
                logger.error(f"[{rule_name}] 정규식 오류: {e}")
                continue

            # --- 최종 적용 여부 판단 ---
            is_match = False
            
            has_any_pattern = bool(filename_pattern or label_pattern or actor_pattern or studio_pattern)
            if not has_any_pattern:
                continue

            if is_and_condition:
                # [AND] 정의된 모든 패턴이 True여야 함
                is_match = all(match_results.values())
            else:
                # [OR] (기존 방식) 정의된 패턴 중 하나라도 매칭되면,
                is_match = any([
                    (filename_pattern and match_results['filename']),
                    (label_pattern and match_results['label']),
                    (actor_pattern and match_results['actor']),
                    (studio_pattern and match_results['studio'])
                ])

            # --- 규칙 병합 ---
            if is_match:
                has_any_match = True
                for key, value in rule.items():
                    if isinstance(value, bool):
                        merged_rule[key] = value
                    elif value: 
                        merged_rule[key] = value

        return merged_rule if has_any_match else None


    @staticmethod
    def _process_subtitle_fast_lane(config, subtitle_infos, db_model):
        """subbed_path가 활성화됐을 때 자막 파일을 우선적으로 이동시킵니다."""
        if not subtitle_infos:
            return

        logger.info(f"자막 우선 처리 (subbed_path): {len(subtitle_infos)}개의 자막 파일을 먼저 이동합니다.")
        sub_config = config.get('자막우선처리', {})
        rule = sub_config.get('규칙', {})
        base_path_str = rule.get('경로')
        exclude_pattern = rule.get('이동제외패턴')

        if not base_path_str:
            logger.warning("자막 우선 처리 경로(subbed_path)가 설정되지 않아 건너뜁니다.")
            return

        base_path = Path(base_path_str)
        folder_format = config.get('이동폴더포맷')

        for info in subtitle_infos:
            if exclude_pattern:
                try:
                    if re.search(exclude_pattern, info['original_file'].name, re.IGNORECASE):
                        logger.debug(f"자막 우선 처리 건너뛰기 (제외 패턴 일치): {info['original_file'].name}")
                        continue # 이 자막 파일은 건너뛰고 다음 파일로
                except re.error as e:
                    logger.error(f"자막 우선 처리의 '이동제외패턴' 정규식 오류: {e}")
            # 자막 파일은 메타 검색을 하지 않으므로 meta_data=None으로 경로 생성
            folders = Task.process_folder_format(config, info, folder_format, meta_data=None)
            target_dir = base_path.joinpath(*folders)

            # 자막 파일은 파일명을 변경하지 않음
            info['newfilename'] = info['original_file'].name
            info['target_dir'] = target_dir
            info['move_type'] = 'subbed_fast_lane'
            info['meta_info'] = None

            # 기존 파일 이동 로직 재사용
            entity = Task.__file_move_logic(config, info, db_model)
            if entity and entity.move_type is not None:
                entity.save()


    # ====================================================================
    # --- Legacy Functions ---
    # ====================================================================


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
            newfilename = Task.parse_jav_filename_by_save_original(filename, newfilename, file_path, file_size)
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
