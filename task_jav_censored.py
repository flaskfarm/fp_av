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
from .tool import ToolExpandFileProcess, UtilFunc, SafeFormatter
from .model_jav_censored import ModelJavCensoredItem
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

            "재시도": True,
            "방송": False,
            # 부가파일 생성 옵션 추가
            "부가파일생성_YAML": ModelSetting.get_bool("jav_censored_make_yaml"),
            "부가파일생성_NFO": ModelSetting.get_bool("jav_censored_make_nfo"),
            "부가파일생성_IMAGE": ModelSetting.get_bool("jav_censored_make_image"),

            # etc
            "파일당딜레이": ModelSetting.get_int("jav_censored_delay_per_file"),
            "PLEXMATE스캔": ModelSetting.get_bool("jav_censored_scan_with_plex_mate"),
            "드라이런": ModelSetting.get_bool("jav_censored_dry_run"),
            'PLEXMATE_URL': F.SystemModelSetting.get('ddns'),
        }

        config['parse_mode'] = 'censored'
        Task._load_extended_settings(config)

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
        logger.debug(f"파싱 및 기본 정보 추출")
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
            logger.debug(f"파싱 실패 파일 {len(unparsed_infos)}개 이동 시작")
            for info in unparsed_infos:
                Task.__move_to_no_label_folder(config, info['original_file'])

        if not parsed_infos:
            logger.debug("파싱에 성공한 파일이 없어 작업을 종료합니다.")
            return

        # 3. 실행 계획 수립 (페어링)
        execution_plan = []
        is_companion_enabled = config.get('동반자막처리활성화', False)

        videos = [info for info in parsed_infos if info['file_type'] == 'video']
        subtitles = [info for info in parsed_infos if info['file_type'] == 'subtitle']
        etc_files = [info for info in parsed_infos if info['file_type'] == 'etc']
        
        # --- etc 파일 우선 처리 ---
        if etc_files:
            failed_path_str = config.get('처리실패이동폴더', '').strip()
            if failed_path_str:
                target_dir = Path(failed_path_str).joinpath("[ETC_FILES]")
                logger.info(f"{len(etc_files)}개의 기타 파일을 '{target_dir}'로 이동합니다.")
                for info in etc_files:
                    # 파일명은 변경하지 않고 그대로 이동
                    info.update({'target_dir': target_dir, 'move_type': 'etc_file', 'newfilename': info['original_file'].name})
                    entity = Task.__file_move_logic(config, info, task_context['db_model'])
                    if entity and entity.target_path:
                        entity.save()
            else:
                logger.warning(f"기타 파일({len(etc_files)}개)을 이동할 '처리실패이동폴더'가 설정되지 않아 건너뜁니다.")
        
        if is_companion_enabled:
            unmatched_subs = []
            for s_info in subtitles:
                found_pair = False
                # 영상 파일명을 기준으로 짝을 찾음 (긴 이름 우선)
                for v_info in sorted(videos, key=lambda x: len(x['original_file'].name), reverse=True):
                    if v_info.get('companion_subtitle'): continue # 이미 짝이 있는 영상은 건너뜀
                    
                    video_stem = v_info['original_file'].stem
                    sub_stem = s_info['original_file'].stem
                    if video_stem == sub_stem or video_stem == os.path.splitext(sub_stem)[0]:
                        # 영상 정보에 자막 정보를 '자식'으로 포함시킴
                        if config.get('동반자막한국어자막판별', False):
                            if UtilFunc.is_korean_subtitle(s_info['original_file'], config):
                                v_info['companion_korean_sub'] = s_info
                            else:
                                v_info['companion_foreign_sub'] = s_info
                        else:
                            v_info['companion_korean_sub'] = s_info
                        found_pair = True
                        break
                
                if not found_pair:
                    unmatched_subs.append(s_info)
            # 최종 실행 계획 = 영상(자막 포함) + 짝없는 자막 + 기타 파일
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
            code_groups = defaultdict(list)
            for info in execution_plan:
                code_groups[info['pure_code']].append(info)

            for pure_code, group_infos in code_groups.items():
                is_set = any(info.get('is_part_of_set') for info in group_infos)

                if is_set:
                    merge_result = Task._merge_and_standardize_media_info(group_infos, ext_config)
                    if merge_result.get('is_valid_set'):
                        # 세트가 유효하면 병합된 정보 사용
                        for info in group_infos:
                            info['final_media_info'] = merge_result['final_media']
                    else:
                        # 세트가 유효하지 않으면 그룹 해제하고 개별 정보 사용
                        logger.warning(f"'{pure_code}' 그룹 미디어 정보 오류로 분할 세트 처리를 취소합니다.")
                        failed_files_set = {f['original_file'].name for f in merge_result.get('failed_files', [])}
                        for info in group_infos:
                            info.update({'is_part_of_set': False, 'parsed_part_type': ''})
                            info['final_media_info'] = {'is_valid': False} if info['original_file'].name in failed_files_set else info.get('media_info')
                else:
                    # 세트가 아닌 경우, 각자의 미디어 정보 사용
                    for info in group_infos:
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

                    path_str = rule.get('경로', '').strip()
                    label_pattern = rule.get('레이블', '').strip()
                    filename_pattern = rule.get('파일명패턴', '').strip()

                    if not label_pattern and not filename_pattern:
                        continue

                    if label_pattern:
                        label_pattern = re.sub(r'\s', '', label_pattern)

                    processed_rule = {
                        'name': rule.get('이름', f'규칙 #{rule_idx+1}'),
                        'path': path_str,
                        'format': rule.get('폴더포맷', '').strip(),
                        'label_pattern': label_pattern,
                        'filename_pattern': filename_pattern,
                        'force_on_meta_fail': rule.get('메타실패시강제적용', False)
                    }
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

                # --- 동반 자막 처리 설정 로드 ---
                companion_config_yaml = jav_settings.get('동반자막처리', {})
                if companion_config_yaml and isinstance(companion_config_yaml, dict):
                    config['동반자막처리'] = companion_config_yaml
                    config['동반자막처리활성화'] = companion_config_yaml.get('처리활성화', False)
                    config['동반자막처리경로'] = companion_config_yaml.get('경로', '')
                    config['동반자막처리폴더포맷'] = companion_config_yaml.get('폴더포맷', '')
                    config['동반자막한국어자막판별'] = companion_config_yaml.get('한국어자막판별', False)
                    config['동반자막언어코드추가'] = companion_config_yaml.get('언어코드추가', True)
                    config['외국어자막이동경로'] = companion_config_yaml.get('외국어자막이동경로', '')

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
        else:
            file_type = 'etc'

        info = {
            'original_file': file,
            'file_size': file.stat().st_size,
            'file_type': file_type,
            'is_parsed': True,
            'pure_code': parsed['code'],
            'label': parsed['label'],
            'number': parsed['number'],
            'raw_number': parsed['raw_number'],
            'ext': parsed['ext'],
            'meta_info': None,
            'media_info': None
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

                if not (is_valid_part(part1_raw) and is_valid_part(part2_raw)):
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
                    elif re.match(r'^[a-zA-Z]\d{1,2}$', part_str, re.I): c_type, key = 'alpha_digit', (ord(part_str[0].lower()) - ord('a'), int(part_str[1:]))
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
        주어진 경로/포맷 문자열을 해석하여 (기준 경로, 하위 폴더 포맷)으로 분리합니다.
        - 경로/포맷에 '{' 변수가 없으면: (Path(경로/포맷), config['이동폴더포맷'])
        - 경로/포맷에 '{' 변수가 있으면: (Path('/'), 경로/포맷) - 절대경로 템플릿
                                         (Path(), 경로/포맷) - 상대경로 템플릿
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
            # 경로가 고정된 경우, 기본 폴더 포맷을 사용
            return Path(path_or_format), config.get('이동폴더포맷')


    @staticmethod
    def _get_final_target_path(config, info, task_context, do_meta_search=False, preloaded_meta=None):
        """
        설정 우선순위에 따라 최종 이동 경로와 방식을 결정하는 유일한 함수.
        반환: (Path객체 | None, 이동타입문자열, 메타정보)
        """
        use_meta_option = config.get('메타사용', 'not_using')
        is_companion_pair = 'companion_korean_sub' in info
        sub_config = config.get('자막우선처리', {})

        # --- 1. 메타 정보 획득 ---
        meta_info = None
        is_meta_success = False
        if preloaded_meta is not None:
            # 미리 로드된 정보가 있으면 사용
            meta_info = preloaded_meta
        elif do_meta_search and use_meta_option == 'using':
            # 메타 검색이 필요하고 옵션이 켜져 있을 때만 API 호출
            meta_info = Task._get_metadata(config, info)

        # 메타 사용 시: meta_info가 있으면 성공.
        # 메타 미사용 시: 항상 성공으로 간주하여 라이브러리 폴더로 이동.
        is_meta_success = (meta_info is not None) if use_meta_option == 'using' else True

        # --- 2. 기본 경로 및 포맷 결정 (메타 성공/실패 기반) ---
        final_path_str = ""
        final_format_str = config.get('이동폴더포맷')
        final_move_type = "normal"

        if is_meta_success:
            library_paths = config.get('라이브러리폴더', [])
            if library_paths:
                final_path_str = library_paths[0]
            else:
                if use_meta_option == 'not_using':
                    logger.error("메타 미사용 모드에서 이동할 라이브러리 폴더가 설정되지 않았습니다.")
                    return None, "no_library_path", meta_info
                final_path_str = ""

            if use_meta_option == 'using':
                if config.get('메타매칭시이동폴더'):
                    final_path_str = config.get('메타매칭시이동폴더')
                final_move_type = "dvd"
            
            if meta_info:
                vr_path_setting = config.get('VR영상이동폴더', '').strip()
                vr_genres = ["VR専用", "ハイクオリティVR", "VR", "Virtual Reality"]
                original_data = meta_info.get('original')
                genres_to_check = ((original_data.get('genre') if isinstance(original_data, dict) else meta_info.get('genre')) or [])
                if vr_path_setting and any(x in genres_to_check for x in vr_genres):
                    base, format_ = Task._resolve_path_template(config, info, meta_info, vr_path_setting)
                    if base is not None:
                        final_path_str = str(base)
                        final_format_str = format_
                        final_move_type = "vr"
        else: # 메타 검색 실패
            if config.get('메타매칭실패시이동', False):
                final_path_str = config.get('메타매칭실패시이동폴더')
                if '{' in final_path_str and '}' in final_path_str:
                    final_format_str = final_path_str
                else:
                    final_format_str = ""
                final_move_type = "meta_fail"
            else:
                return None, "meta_fail_skipped", meta_info

        # --- 3. 우선순위에 따른 경로 및 포맷 재정의 (Override) ---
        if is_companion_pair:
            companion_config = config.get('동반자막처리', {})
            comp_path = ""

            # 메타 성공/실패에 따른 경로 결정 (작업 YAML > 전역 설정 > 기본값 순)
            if is_meta_success:
                comp_path = config.get('동반자막처리경로_메타성공시') or companion_config.get('경로_메타성공시')
            else:
                comp_path = config.get('동반자막처리경로_메타실패시') or companion_config.get('경로_메타실패시')
            
            # 위 값이 없을 경우 하위 호환 경로 사용
            if not comp_path:
                comp_path = config.get('동반자막처리경로') or companion_config.get('경로')

            if comp_path: 
                final_path_str = comp_path
            
            final_move_type = 'companion_kor'
            
            # 폴더 포맷도 작업 YAML > 전역 설정 순으로 확인
            comp_format = config.get('동반자막처리폴더포맷') or companion_config.get('폴더포맷')
            if comp_format: 
                final_format_str = comp_format
                is_custom_format_set = True

        is_custom_format_set = False
        if config.get('커스텀경로활성화', False):
            rule = Task._find_and_merge_custom_path_rules(info, config.get('커스텀경로규칙', []), meta_info)
            if rule and (is_meta_success or rule.get('force_on_meta_fail')):
                custom_path = rule.get('path') or rule.get('경로', '')
                if custom_path: 
                    final_path_str = custom_path
                    final_move_type = 'custom_path'
                custom_format = rule.get('format') or rule.get('폴더포맷', '')
                if custom_format: 
                    final_format_str = custom_format
                    is_custom_format_set = True

        if not is_companion_pair and sub_config.get('처리활성화', False):
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

        # --- 최종 경로 조립 ---
        if not final_path_str:
            return None, final_move_type, meta_info

        base_path, format_from_template = Task._resolve_path_template(config, info, meta_info, final_path_str)

        if not is_custom_format_set and format_from_template and final_move_type != 'meta_fail':
            final_format_str = format_from_template

        folders = Task.process_folder_format(config, info, final_format_str, meta_info)
        target_dir = base_path.joinpath(*folders)

        return target_dir, final_move_type, meta_info


    @staticmethod
    def _get_metadata(config, info):
        """
        메타데이터 검색을 수행하고, 성공 시 meta_info 객체를 반환합니다.
        경로 결정 로직은 포함하지 않습니다.
        """
        meta_module = Task.get_meta_module('jav_censored')
        if not meta_module:
            logger.error("메타데이터 플러그인을 찾을 수 없습니다. 메타 검색을 건너뜁니다.")
            return None

        search_name = info['pure_code']
        label = search_name.split("-")[0].lower()

        # --- 1. 제외 레이블 확인 ---
        if label in config.get('메타매칭제외레이블', []):
            logger.info(f"'{label}'은(는) '정상 매칭 제외 레이블'에 포함되어 메타 검색을 건너뜁니다.")
            return None

        # --- 2. 메타 정보 획득 ---
        meta_info = None
        match_site = "N/A"
        best_match = None
        custom_search_settings = config.get('메타검색에사용할사이트')

        if custom_search_settings:
            # 시나리오 A: 사용자가 사이트를 직접 지정한 경우
            site_list = [s for s in custom_search_settings if not config.get('메타검색에공식사이트만사용', False) or s.get('사이트') in ['dmm', 'mgstage']]
            for rule in site_list:
                site_name, min_score = rule.get('사이트'), rule.get('점수', 95)
                if not site_name: continue
                try:
                    res = meta_module.search2(search_name, site_name, manual=False)
                    match = next((item for item in res if item.get('score', 0) >= min_score), None)
                    if match:
                        logger.debug(f"'{site_name}' 사이트에서 '{search_name}'에 대한 유효한 매치(점수: {match.get('score')})를 찾았습니다. 검색을 중단합니다.")
                        best_match = match
                        break 
                except Exception as e:
                    logger.error(f"'{site_name}' 사이트 검색 중 예외 발생: {e}")
        else:
            # 시나리오 B: 통합 search 사용 (기본 검색)
            try:
                res = meta_module.search(search_name, manual=False)
                # search 결과가 있고, 그 첫번째 항목의 점수가 95점 이상이면 유효한 매치로 간주
                if res and res[0].get('score', 0) >= 95:
                    best_match = res[0]
            except Exception as e:
                logger.error(f"메타데이터 통합 검색(search) 중 예외 발생: {e}")

        if best_match:
            try:
                meta_info = meta_module.info(best_match["code"], keyword=search_name, fp_meta_mode=True)
                if meta_info:
                    match_site = best_match.get('site', 'N/A')
            except Exception as e:
                logger.error(f"'{best_match['code']}'의 상세 정보 획득 중 예외 발생: {e}")
                
        # --- 3. 메타 정보 후처리 및 반환 ---
        if meta_info:
            logger.info(f"'{search_name}' 메타 검색 성공: {meta_info.get('originaltitle')} (from: {match_site})")
            for actor in (meta_info.get("actor") or []):
                try:
                    meta_module.process_actor(actor)
                except Exception as e:
                    logger.error(f"배우 '{actor.get('originalname')}' 이름 번역 중 오류: {e}")
            return meta_info
        else:
            logger.info(f"'{search_name}'에 대한 유효한 메타 정보를 찾지 못했습니다.")
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
        last_scan_path = None
        last_move_type = None
        successful_move_types = {'dvd', 'normal', 'subbed', 'custom_path', 'companion_kor', 'companion_kor_sub', 'meta_success', 'vr'}
        if config.get('scan_with_no_meta', True):
            successful_move_types.update(['no_meta', 'meta_fail'])

        from itertools import groupby
        execution_plan.sort(key=lambda x: x['pure_code'])
        
        for pure_code, group_infos_iter in groupby(execution_plan, key=lambda x: x['pure_code']):
            group_infos = list(group_infos_iter)
            logger.debug(f"'{pure_code}' 그룹 처리 시작 ({len(group_infos)}개 파일)")

            first_info = group_infos[0]
            _, _, meta_info_for_group = Task._get_final_target_path(config, first_info, task_context, do_meta_search=True)

            for info in group_infos:
                try:
                    item_count += 1
                    log_prefix = f"[{item_count:03d}/{total_items_in_plan:03d}]"
                    logger.info(f"{log_prefix} 처리 시작: {info['original_file'].name}")
                    
                    target_dir, move_type, _ = Task._get_final_target_path(config, info, task_context, preloaded_meta=meta_info_for_group)
                    
                    if not target_dir:
                        logger.warning(f"'{info['original_file'].name}'의 이동 경로를 결정할 수 없어 건너뜁니다. (이동 타입: {move_type})")
                        continue
                    
                    info.update({'target_dir': target_dir, 'move_type': move_type, 'meta_info': meta_info_for_group})
                    
                    current_target_dir = target_dir
                    if scan_enabled and current_target_dir != last_scan_path and last_scan_path is not None:
                        if last_move_type in successful_move_types:
                            Task.__request_plex_mate_scan(config, last_scan_path)
                    
                    entity = Task.__file_move_logic(config, info, db_model)
                    
                    if entity or config.get('드라이런', False):
                        if entity: 
                            if entity.target_path:
                                entity.save()
                            else:
                                continue

                        if 'companion_korean_sub' in info:
                            s_info = info['companion_korean_sub']
                            logger.info(f"{log_prefix} 동반 자막(한): {s_info['original_file'].name}")
                            new_video_stem = Path(info['newfilename']).stem
                            if entity and entity.target_path:
                                new_video_stem = Path(entity.target_path).stem
                            sub_ext = s_info['original_file'].suffix
                            if config.get('동반자막언어코드추가', True) and not re.search(r'\.(ko|kr|kor)$', new_video_stem, re.I):
                                new_video_stem += '.ko'
                            s_info.update({'target_dir': target_dir, 'move_type': 'companion_kor_sub', 'newfilename': new_video_stem + sub_ext})
                            s_entity = Task.__file_move_logic(config, s_info, db_model)
                            if s_entity and s_entity.target_path: s_entity.save()

                        elif 'companion_foreign_sub' in info:
                            s_info = info['companion_foreign_sub']
                            logger.info(f"{log_prefix} 동반 자막(외): {s_info['original_file'].name} (별도 경로로 이동)")
                            foreign_path_str = config.get('외국어자막이동경로', '').strip()
                            sub_target_dir = Path(config.get('처리실패이동폴더')).joinpath("[FOREIGN_SUBS]")
                            if foreign_path_str:
                                base_path, format_str = Task._resolve_path_template(config, s_info, None, foreign_path_str)
                                folders = Task.process_folder_format(config, s_info, format_str, None)
                                sub_target_dir = base_path.joinpath(*folders)
                            s_info.update({'target_dir': sub_target_dir, 'move_type': 'companion_foreign_sub', 'newfilename': s_info['original_file'].name})
                            s_entity = Task.__file_move_logic(config, s_info, db_model)
                            if s_entity: s_entity.save()
                    
                    if scan_enabled and current_target_dir is not None:
                        last_scan_path = current_target_dir
                        last_move_type = move_type
                
                except Exception as e:
                    logger.error(f"'{info.get('pure_code', '알 수 없음')}' 파일 처리 중 예외 발생: {e}")
                    logger.error(traceback.format_exc())

        if scan_enabled and last_scan_path is not None:
            if last_move_type in successful_move_types:
                logger.info(f"모든 파일 처리 완료. 마지막 경로 스캔 요청: {last_scan_path}")
                Task.__request_plex_mate_scan(config, last_scan_path)


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
                # 3a. 파일 이동
                target_dir.mkdir(parents=True, exist_ok=True)
                shutil.move(file, newfile)
                logger.info(f"파일 이동 성공: -> {newfile}")

                # 3b. 부가 파일 생성 (이동 성공 후)
                if meta_info:
                    printable_meta_info = meta_info.copy()
                    printable_meta_info.pop('original', None)

                    TaskMakeYaml.make_files(
                        printable_meta_info, # 'original'이 제거된 깨끗한 데이터 전달
                        str(newfile.parent),
                        make_yaml=config.get('부가파일생성_YAML', False),
                        make_nfo=config.get('부가파일생성_NFO', False),
                        make_image=config.get('부가파일생성_IMAGE', False),
                    )

                # 3c. 방송 처리 (이동 성공 후)
                if config.get('방송', False):
                    bot = {
                        't1': 'gds_tool', 't2': 'fp', 't3': 'av',
                        'data': {'gds_path': str(newfile).replace('/mnt/AV/MP/GDS', '/ROOT/GDRIVE/VIDEO/AV')}
                    }
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

        # 1. 사용할 기본 정보(base_label, number_part_raw) 추출
        base_label = ""
        number_part_raw = ""
        data = {}

        if meta_data and isinstance(meta_data, dict):
            original_title = meta_data.get("originaltitle", "")
            code_parts = original_title.split('-', 1)
            base_label = code_parts[0]
            number_part_raw = code_parts[1] if len(code_parts) > 1 else ''
            actor_list = meta_data.get('actor') or []
            actor_names = [actor.get('name', '') for actor in actor_list[:3] if actor.get('name')]
            year_str = str(meta_data.get("year")) if meta_data.get("year") is not None else ""
            studio_str = meta_data.get("studio") or "NO_STUDIO"
            data.update({
                "studio": studio_str,
                "code": original_title,
                "actor": f"{','.join(actor_names[:1])}",
                "actor_2": f"{','.join(actor_names[:2])}",
                "actor_3": f"{','.join(actor_names[:3])}",
                "year": year_str,
            })
        else:
            base_label = info['label']
            number_part_raw = info.get('number', '')
            data.update({
                "code": info['pure_code'].upper(),
                "year": "", "actor": "", "studio": base_label.upper()
            })

        label_1 = '#'
        processed_label = base_label

        if base_label:
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

        data['label_1'] = label_1
        data['label'] = processed_label.upper()
        data['label_lower'] = processed_label.lower()
        data['code_lower'] = data['code'].lower()
        if "studio" not in (meta_data or {}):
            data['studio'] = processed_label.upper()
            data['studio_lower'] = processed_label.lower()

        # 3. {num_X_Y}, {year4} 등 추가 변수 처리
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

        # 4. 안전한 포맷팅 및 후처리
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
        """Plex Mate에 웹 API를 통해 스캔을 요청합니다."""
        is_dry_run = config.get('드라이런', False)

        if is_dry_run:
            logger.warning(f"[Dry Run] Plex Mate 스캔 요청 시뮬레이션: {scan_path}")
            return

        try:
            base_url = config.get('PLEXMATE_URL')
            if not base_url:
                return

            url = f"{base_url.rstrip('/')}/plex_mate/api/scan/do_scan"

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
            logger.error(f"Plex Mate API 호출 중 네트워크 오류: {e}")
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
        label_to_check = info['label'].lower()
        actors_to_check = []
        studio_to_check = ""

        if meta_info:
            if meta_info.get("originaltitle"):
                label_to_check = meta_info.get("originaltitle").split('-')[0].lower()
            if meta_info.get("actor"):
                actors_to_check = [a.get('name', '').strip() for a in (meta_info.get('actor') or []) if a.get('name', '').strip()]
            if meta_info.get("studio"):
                studio_to_check = meta_info.get("studio").strip()

        for rule in rules_list:
            is_match = False
            rule_name = rule.get('name') or rule.get('이름', '이름 없는 규칙')

            filename_pattern = rule.get('filename_pattern') or rule.get('파일명패턴')
            if filename_pattern:
                try:
                    if re.search(filename_pattern, original_filename, re.IGNORECASE):
                        is_match = True
                        logger.debug(f"규칙 매칭: '{rule_name}' (파일명패턴)")
                except re.error as e:
                    logger.error(f"커스텀 경로 규칙 '{rule_name}'의 파일명 정규식 오류: {e}")
                    continue

            label_pattern = rule.get('label_pattern') or rule.get('레이블')
            if not is_match and label_pattern and label_to_check:
                try:
                    if re.fullmatch(label_pattern, label_to_check, re.IGNORECASE):
                        is_match = True
                        logger.debug(f"규칙 매칭: '{rule_name}' (레이블: {label_to_check})")
                except re.error as e:
                    logger.error(f"커스텀 경로 규칙 '{rule_name}'의 레이블 정규식 오류: {e}")
                    continue

            actor_pattern = rule.get('actor_pattern') or rule.get('배우')
            if not is_match and actor_pattern and actors_to_check:
                try:
                    if any(re.search(actor_pattern, actor_name, re.IGNORECASE) for actor_name in actors_to_check):
                        is_match = True
                        matched_actors = [name for name in actors_to_check if re.search(actor_pattern, name, re.IGNORECASE)]
                        logger.debug(f"규칙 매칭: '{rule_name}' (배우: {matched_actors})")
                except re.error as e:
                    logger.error(f"규칙 '{rule_name}'의 배우 정규식 오류: {e}")

            studio_pattern = rule.get('studio_pattern') or rule.get('스튜디오')
            if not is_match and studio_pattern and studio_to_check:
                try:
                    if re.search(studio_pattern, studio_to_check, re.IGNORECASE):
                        is_match = True
                        logger.debug(f"규칙 매칭: '{rule_name}' (스튜디오: {studio_to_check})")
                except re.error as e:
                    logger.error(f"규칙 '{rule_name}'의 스튜디오 정규식 오류: {e}")

            if is_match:
                has_any_match = True
                boolean_keys = ['force_on_meta_fail', '메타실패시강제적용']
                for key, value in rule.items():
                    if key in boolean_keys:
                        if value is not None:
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
