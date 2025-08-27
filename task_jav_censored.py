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
import shlex

from datetime import datetime

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

        base_config = {
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
            "PLEXMATE스캔": ModelSetting.get_bool("jav_censored_scan_with_plex_mate"),
            "드라이런": ModelSetting.get_bool("jav_censored_dry_run"),
            'PLEXMATE_URL': F.SystemModelSetting.get('ddns'),
        }

        if job_type in ['default', 'dry_run']:
            config = base_config.copy()
            config["이름"] = job_type
            if config.get('드라이런', False):
                logger.warning(f"'{config['이름']}' 작업: Dry Run 모드가 활성화되었습니다.")

            TaskBase.__task(config)

        elif job_type == 'yaml':
            yaml_filepath = args[1]
            try:
                yaml_data = SupportYaml.read_yaml(yaml_filepath)
                for job in yaml_data.get('작업', []):
                    if not job.get('사용', True): continue

                    config = base_config.copy()
                    config.update(job)

                    if config.get('드라이런', False):
                        logger.warning(f"'{config.get('이름', 'YAML Job')}' 작업: Dry Run 모드가 활성화되었습니다.")

                    TaskBase.__task(config)
            except Exception as e:
                logger.error(f"YAML 파일 처리 중 오류 발생: {e}")


    @staticmethod
    def __task(config):
        config['parse_mode'] = 'censored'
        Task._load_extended_settings(config)

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

        config['module_name'] = task_context.get('module_name')
        config['parse_mode'] = task_context.get('parse_mode')

        # 0. 파싱 규칙 및 확장 기능 설정 로드
        Task._load_extended_settings(config)

        # 1. 파일 목록 수집
        logger.debug(f"처리 파일 목록 생성")
        files = Task.__collect_initial_files(config, task_context['module_name'])
        if not files:
            logger.info("처리할 파일이 없습니다.")
            return

        # 2. 파싱 및 기본 정보 추출, 목록 분리
        logger.debug(f"파싱 및 기본 정보 추출")
        execution_plan = []
        unparsed_infos = []

        for file in files:
            info = Task.__prepare_initial_info(config, file)

            if info.get('is_parsed'):
                execution_plan.append(info)
            else:
                unparsed_infos.append(info)

        # 2-1. 파싱 실패 파일 처리
        if unparsed_infos:
            logger.debug(f"파싱 실패 파일 {len(unparsed_infos)}개 이동 시작")
            for info in unparsed_infos:
                Task.__move_to_no_label_folder(config, info['original_file'])

        if not execution_plan:
            logger.debug("파싱에 성공한 파일이 없어 작업을 종료합니다.")
            return

        # 3. 분할 파일 처리
        if config.get('분할파일처리', True):
            logger.debug(f"분할 파일 세트 식별 및 처리 시작")
            Task.__process_part_sets(execution_plan)

        # 4. 파일명 조립을 위한 최종 데이터(final_media_info) 준비
        from collections import defaultdict # <<< defaultdict import

        ext_config = config.get('확장_ffprobe', {})
        code_groups = defaultdict(list)
        for info in execution_plan:
            code_groups.setdefault(info['pure_code'], []).append(info)

        for pure_code, group_infos in code_groups.items():
            is_set = any(info.get('is_part_of_set') for info in group_infos)
            if is_set:
                if config.get('파일명에미디어정보포함'):
                    merge_result = Task._merge_and_standardize_media_info(group_infos, config)
                    if merge_result.get('is_valid_set'):
                        for info in group_infos:
                            info['final_media_info'] = merge_result['final_media']
                    else:
                        logger.warning(f"'{pure_code}' 그룹에 미디어 정보가 없거나 유효하지 않은 파일이 있어, 세트 처리가 취소됩니다.")
                        for info in group_infos:
                            info['is_part_of_set'] = False
                            info['final_media_info'] = info.get('media_info')
                else:
                    for info in group_infos:
                        info['final_media_info'] = None
            else:
                for info in group_infos:
                    info['final_media_info'] = info.get('media_info')

        # 5. 최종 파일명 조립 (tool.py 호출)
        for info in execution_plan:
            info['newfilename'] = ToolExpandFileProcess.assemble_filename(config, info)

        # 6. 실제 파일 이동 (각 모듈의 __execute_plan 호출)
        task_context['execute_plan'](config, execution_plan, task_context['db_model'])


    # ====================================================================
    # --- 헬퍼 함수들 (Helper Functions) ---
    # ====================================================================


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

                duplicate_check_method = misc_settings.get('duplicate_check_method', 'flexible')
                config['중복체크방식'] = duplicate_check_method
                config['이미처리된파일명패턴'] = misc_settings.get('already_processed_pattern', r'^[a-zA-Z0-9]+-[a-zA-Z0-9-_]+(\s\[.*\](?:cd\d+)?)$')
                config['허용된숫자레이블'] = misc_settings.get('allowed_numeric_labels', r'^(741|1pon|10mu).*?')
                config['scan_with_no_meta'] = misc_settings.get('scan_with_no_meta', True)
                config['메타검색에사용할사이트'] = misc_settings.get('메타검색에사용할사이트', None)

                # --- 커스텀 경로 규칙 ---
                custom_path_rules_yaml = jav_settings.get('meta_custom_path', [])
                custom_rules_for_module = []
                current_module = config.get('parse_mode')

                if custom_path_rules_yaml:
                    logger.debug(f"커스텀 경로 규칙 {len(custom_path_rules_yaml)}개를 로드합니다.")
                    for rule_idx, rule in enumerate(custom_path_rules_yaml):
                        target_module = rule.get('모듈', 'all').lower()
                        if target_module != 'all' and target_module != current_module:
                            continue

                        path_str = rule.get('경로', '').strip()
                        label_pattern = rule.get('레이블', '').strip()
                        filename_pattern = rule.get('파일명패턴', '').strip()
                        
                        if not path_str or (not label_pattern and not filename_pattern):
                            continue
                        
                        # 여러 줄로 작성된 정규식에서 공백과 개행문자 제거
                        if label_pattern:
                            label_pattern = re.sub(r'\s', '', label_pattern)

                        processed_rule = {
                            'name': rule.get('이름', f'규칙 #{rule_idx+1}'),
                            'path': path_str,
                            'format': rule.get('폴더포맷', '').strip(),
                            'label_pattern': label_pattern,
                            'filename_pattern': filename_pattern
                        }
                        custom_rules_for_module.append(processed_rule)

                config['커스텀경로규칙'] = custom_rules_for_module

                # --- ffprobe 관련 설정 로드 ---
                default_ffprobe_config = {
                    'enable': False,
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
                ffprobe_config_yaml = jav_settings.get('fp_filename_with_ffprobe', {})
                # nested dict update
                default_ffprobe_config.update({k: v for k, v in ffprobe_config_yaml.items() if not isinstance(v, dict)})
                default_ffprobe_config.get('tolerance', {}).update(ffprobe_config_yaml.get('tolerance', {}))
                config['확장_ffprobe'] = default_ffprobe_config

                logger.debug(f"ffprobe 확장 활성화: {config['확장_ffprobe'].get('enable')}")

                # --- 자막 우선 처리(subbed_path) 설정 로드 ---
                default_subbed_config = {
                    '처리활성화': False,
                    '자막파일확장자': {'ass', 'ssa', 'idx', 'sub', 'sup', 'smi', 'srt', 'ttml', 'vtt'},
                    '내장자막키워드': [],
                    '규칙': {}
                }
                subbed_config_yaml = jav_settings.get('subbed_path', {})
                if subbed_config_yaml and subbed_config_yaml.get('처리활성화'):
                    default_subbed_config['처리활성화'] = True

                    ext_str = subbed_config_yaml.get('자막파일확장자', 'ass ssa idx sub sup smi srt ttml vtt')
                    default_subbed_config['자막파일확장자'] = {f".{ext.strip()}" for ext in ext_str.split()}

                    keyword_str = subbed_config_yaml.get('내장자막키워드', '')
                    if keyword_str:
                        default_subbed_config['내장자막키워드'] = [kw.strip().lower() for kw in keyword_str.split()]

                    # 현재 모듈에 맞는 규칙만 저장
                    current_module = config.get('parse_mode')
                    for rule in subbed_config_yaml.get('규칙', []):
                        if rule.get('모듈', '').lower() == current_module:
                            default_subbed_config['규칙'] = rule
                            logger.debug(f"자막 우선 처리 규칙 로드: 모듈={current_module}, 경로={rule.get('경로')}")
                            break

                config['확장_자막우선처리'] = default_subbed_config

            else:
                logger.warning("메타데이터 플러그인을 찾을 수 없어 파싱 규칙 및 확장 설정을 로드하지 못했습니다.")
                config['파싱규칙'] = {}
                config['확장_ffprobe'] = {'enable': False}
                config['확장_자막우선처리'] = {'처리활성화': False}
        except Exception as e:
            logger.error(f"확장 설정 로드 중 오류: {e}")
            config['파싱규칙'] = {}
            config['확장_ffprobe'] = {'enable': False}


    @staticmethod
    def __move_to_no_label_folder(config, file_path: Path):
        """품번 추출에 실패한 파일을 '처리실패이동폴더/[NO LABEL]'로 이동시킵니다."""

        target_root_str = config.get('처리실패이동폴더', '').strip()
        if not target_root_str:
            logger.warning(f"'{file_path.name}'을 이동할 '처리실패이동폴더'가 설정되지 않았습니다.")
            return

        target_dir = Path(target_root_str).joinpath("[NO LABEL]")

        try:
            target_dir.mkdir(parents=True, exist_ok=True)
            target_file = target_dir.joinpath(file_path.name)

            if target_file.exists():
                # is_duplicate 대신 간단한 존재 여부만 체크
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

        no_censored_path = Path(config['처리실패이동폴더'].strip())
        if not no_censored_path.is_dir():
            logger.warning("'처리 실패시 이동 폴더'가 유효하지 않아 작업을 중단합니다.")
            return [] # 파일이 없으므로 빈 리스트 반환

        src_list = Task.get_path_list(config['다운로드폴더'])
        src_list.extend(Task.__add_meta_no_path(module_name))

        all_files = []
        for src in src_list:
            ToolExpandFileProcess.preprocess_cleanup(
                src, 
                config.get('최소크기', 0), 
                config.get('최대기간', 0)
            )
            _f = ToolExpandFileProcess.preprocess_listdir(src, no_censored_path, config)
            all_files.extend(_f or [])

        from collections import Counter
        stats = Counter()
        for file in all_files:
            ext = file.suffix.lower()
            if ext in ToolExpandFileProcess.VIDEO_EXTS:
                stats['video'] += 1
            elif ext in ToolExpandFileProcess.SUBTITLE_EXTS:
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
        all_files.sort(key=lambda p: [int(c) if c.isdigit() else c.lower() for c in re.split('([0-9]+)', p.name)])

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
        if ext in ToolExpandFileProcess.VIDEO_EXTS:
            file_type = 'video'
        elif ext in ToolExpandFileProcess.SUBTITLE_EXTS:
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
            'raw_part': parsed['part'],
            'ext': parsed['ext'],
            'meta_info': None,
        }
        ext_config = config.get('확장_ffprobe', {})
        if config.get('파일명에미디어정보포함') and ext_config.get('enable') and info.get('file_type') == 'video':
            info['media_info'] = ToolExpandFileProcess._get_media_info(file, ext_config)

        return info


    @staticmethod
    def __process_part_sets(execution_plan):
        """
        분할 파일 처리 로직
        """
        from collections import defaultdict

        # 0. 동영상 파일 중, 이미 처리된 'cdN' 형식의 파일은 제외
        unprocessed_infos = []
        video_infos = [info for info in execution_plan if info.get('file_type') == 'video']
        for info in video_infos:
            if re.search(r'cd\d+$', info['original_file'].stem, re.IGNORECASE):
                logger.debug(f"분할 파일 세트 처리에서 제외: 이미 처리된 파일명 형식입니다 - {info['original_file'].name}")
            else:
                unprocessed_infos.append(info)

        potential_sets = defaultdict(list)

        for info in unprocessed_infos:
            stem = info['original_file'].stem
            raw_number_part = info.get('raw_number')
            if not raw_number_part:
                continue

            # 1. 원본 stem에서 파싱된 number가 나타나는 모든 위치를 찾음
            indices = [m.start() for m in re.finditer(re.escape(raw_number_part), stem, re.IGNORECASE)]
            if not indices:
                continue

            for idx in indices:
                # 2. number의 각 위치를 기준으로 뒷부분(tail)을 자름
                tail_part = stem[idx + len(raw_number_part):]

                # 3. tail에서 숫자 파트 또는 알파벳 파트 패턴을 찾음
                pattern = r'^(?:(?P<part_num>[-_.\s]\d{1,2})(?!\d)|(?P<part_alpha>[-_.\s]?[a-zA-Z])(?![a-zA-Z]))(?P<suffix>.*)$'
                match = re.match(pattern, tail_part, re.IGNORECASE)

                if match:
                    parts = match.groupdict()
                    part_str = ''

                    if parts['part_num']:
                        part_str = re.search(r'\d{1,2}', parts['part_num']).group(0)
                    elif parts['part_alpha']:
                        part_str = re.search(r'[a-zA-Z]', parts['part_alpha']).group(0)

                    if not (part_str.isdigit() or (part_str.isalpha() and len(part_str) == 1)):
                        continue

                    # 4. 유효한 파트를 찾았으면, 정보를 수집하고 루프 탈출
                    prefix_part = stem[:idx]
                    suffix_part = parts['suffix']

                    part_type = 'digit' if part_str.isdigit() else 'alpha'
                    group_key = (prefix_part.lower(), info['pure_code'], suffix_part.lower(), part_type)

                    potential_sets[group_key].append({
                        'info': info, 'part_str': part_str,
                        'original_prefix': prefix_part,
                        'original_number': raw_number_part,
                        'original_suffix': suffix_part
                    })
                    break # 가장 먼저 발견된 유효한 조합을 사용

        # 5. 세트 검증 및 정보 주입
        for group_key, items in potential_sets.items():
            if len(items) < 2:
                continue

            part_type = group_key[3]

            # 5a. 파트 넘버를 수치로 변환
            numeric_parts = []
            for item in items:
                part_str = item['part_str']
                if part_type == 'digit':
                    numeric_parts.append(int(part_str))
                else: # alpha
                    numeric_parts.append(ord(part_str.lower()))

            # 5b. 시퀀스 시작점 및 연속성 검사
            sorted_numeric = sorted(numeric_parts)

            starts_correctly = (part_type == 'digit' and sorted_numeric[0] == 1) or \
                               (part_type == 'alpha' and sorted_numeric[0] == ord('a'))

            is_continuous = all(sorted_numeric[i] == sorted_numeric[0] + i for i in range(len(sorted_numeric)))

            if not (starts_correctly and is_continuous):
                logger.debug(f"'{items[0]['info']['pure_code']}' 그룹은 유효한 시퀀스(시작점/연속성)가 아님: {[item['part_str'] for item in items]}")
                continue

            # 5c. 세트 확정 및 정보 주입
            representative_code = items[0]['info']['pure_code']
            logger.info(f"'{representative_code}' 그룹에서 유효한 분할 파일 세트를 발견했습니다.")
            total_size = sum(item['info']['file_size'] for item in items)

            sorted_items = sorted(zip(numeric_parts, items), key=lambda x: x[0])

            for i, (_, item) in enumerate(sorted_items):
                info = item['info']
                info['is_part_of_set'] = True
                info['parsed_part_type'] = f"cd{i + 1}"
                info['part_set_code'] = representative_code
                info['part_set_total_size'] = total_size
                info['part_set_prefix'] = item['original_prefix']
                info['part_set_number'] = item['original_number']
                info['part_set_suffix'] = item['original_suffix']


    @staticmethod
    def __execute_plan(config, execution_plan, db_model):
        """
        최종 실행 함수.
        """
        ext_config = config.get('확장_ffprobe', {})
        sub_config = config.get('확장_자막우선처리', {})

        # 품번 그룹화
        code_groups = {}
        for info in execution_plan:
            code_groups.setdefault(info['pure_code'], []).append(info)

        total_files = len(execution_plan)
        logger.info(f"처리할 품번 그룹 {len(code_groups)}개 (총 파일 {total_files}개)")

        scan_enabled = config.get("PLEXMATE스캔", False)
        delay_seconds = config.get('파일당딜레이', 0)
        processed_count = 0
        last_scan_path = None
        last_move_type = None

        successful_move_types = {'dvd', 'normal', 'subbed'}
        if config.get('scan_with_no_meta', True):
            successful_move_types.update(['no_meta', 'meta_fail'])
            # logger.debug(f"메타 없는 파일 스캔 활성화. 스캔 대상: {successful_move_types}")

        for idx, (pure_code, group_infos) in enumerate(code_groups.items()):
            try:
                representative_info = group_infos[0]
                target_dir, move_type, meta_info = None, None, None

                # --- 자막 우선 처리 로직 ---
                is_subbed_target = False
                if sub_config.get('처리활성화') and sub_config.get('규칙'):
                    # 1. 내장 자막 키워드 확인
                    if any(kw in representative_info['original_file'].name.lower() for kw in sub_config['내장자막키워드']):
                        is_subbed_target = True
                    # 2. 외부 자막 파일 확인
                    elif Task._find_external_subtitle(config, representative_info, sub_config):
                        is_subbed_target = True

                if is_subbed_target:
                    logger.info(f"'{pure_code}' 그룹: 자막 파일 조건 충족, 우선 처리합니다.")
                    rule = sub_config['규칙']
                    base_path = Path(rule['경로'])
                    folder_format = rule.get('폴더구조') or config.get('이동폴더포맷')
                    folders = Task.process_folder_format(config, representative_info, folder_format) # 그룹 대표 정보로 폴더 생성
                    target_dir = base_path.joinpath(*folders)
                    move_type = "subbed"
                    meta_info = None # 자막 우선 처리는 메타 검색 안 함
                else:
                    # --- 일반 경로 결정 및 미디어 정보 처리 ---
                    # 1. 미디어 정보 사전 처리 및 그룹 유효성 검사
                    is_set = any(info.get('is_part_of_set') for info in group_infos)
                    use_media_info_in_filename = config.get('파일명에미디어정보포함') and ext_config.get('enable')

                    if is_set and use_media_info_in_filename:
                        merge_result = Task._merge_and_standardize_media_info(group_infos, ext_config)
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

                    # 2. 경로 결정 (그룹 단위로 한 번만)
                    if config.get('메타사용') == 'using':
                        target_dir, move_type, meta_info = Task.__get_target_with_meta(config, representative_info)
                        if delay_seconds > 0: time.sleep(delay_seconds)
                    else:
                        move_type = "normal"
                        target_paths = Task.get_path_list(config['라이브러리폴더'])
                        folders = Task.process_folder_format(config, representative_info, config['이동폴더포맷'])
                        target_dir = Path(target_paths[0]).joinpath(*folders) if target_paths else None

                # --- 스캔 요청 전 조건 확인 ---
                successful_move_types = {'dvd', 'normal', 'subbed'}

                if scan_enabled and target_dir != last_scan_path and last_scan_path is not None:
                    # 이전 경로가 성공적인 이동 경로였을 때만 스캔
                    if last_move_type in successful_move_types:
                        Task.__request_plex_mate_scan(config, last_scan_path)

                # --- 그룹 내 각 파일 처리 루프 ---
                for info in group_infos:
                    processed_count += 1
                    logger.info(f"[{processed_count:03d}/{total_files:03d}] {info['original_file'].name}")

                    current_move_type = move_type
                    current_target_dir = target_dir

                    # 자막 우선 처리가 아닌 경우에만 미디어 유효성 재검사
                    if not is_subbed_target:
                        use_media_info_in_filename = config.get('파일명에미디어정보포함') and ext_config.get('enable')
                        if use_media_info_in_filename and not info.get('final_media_info', {}).get('is_valid', True):
                            current_move_type = "failed_video"
                            current_target_dir = Path(config['처리실패이동폴더']).joinpath("[FAILED VIDEO]")
                            info['newfilename'] = info['original_file'].name
                        else:
                            info['newfilename'] = ToolExpandFileProcess.assemble_filename(config, info)
                    else: # 자막 우선 처리 시에는 파일명만 조립
                        info['newfilename'] = ToolExpandFileProcess.assemble_filename(config, info)

                    info.update({
                        'target_dir': current_target_dir,
                        'move_type': current_move_type,
                        'meta_info': meta_info
                    })

                    entity = Task.__file_move_logic(config, info, db_model)
                    if entity and entity.move_type is not None:
                        entity.save()

                # --- 마지막 스캔 경로 및 타입 업데이트 ---
                if scan_enabled and target_dir is not None:
                    last_scan_path = target_dir
                    last_move_type = move_type # 현재 이동 타입 저장

            except Exception as e:
                logger.error(f"'{pure_code}' 그룹 처리 중 예외 발생: {e}")
                logger.error(traceback.format_exc())

        # --- 모든 작업 완료 후 최종 스캔 요청 ---
        if scan_enabled and last_scan_path is not None:
            # 마지막으로 처리된 경로가 성공적인 이동 경로였을 때만 최종 스캔
            successful_move_types = {'dvd', 'normal', 'subbed'}
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

        target_dir.mkdir(parents=True, exist_ok=True)
        newfile = target_dir.joinpath(newfilename)

        # Dry Run 모드일 경우, 로그만 남기고 조기 리턴
        if is_dry_run:
            log_msg = f"[Dry Run] 원본: {file}\n"
            log_msg += f"         이동: {newfile} (타입: {move_type})\n"

            # 중복 검사도 시뮬레이션
            if UtilFunc.is_duplicate(file, newfile, config):
                remove_path_str = config.get('중복파일이동폴더', '').strip()
                if not remove_path_str:
                    log_msg += f"         결과: 중복 파일로 감지되어 삭제될 예정"
                else:
                    log_msg += f"         결과: 중복 파일로 감지되어 {remove_path_str}로 이동될 예정"
            else:
                log_msg += f"         결과: 정상적으로 이동될 예정"

            logger.warning(log_msg)
            return None # DB 저장을 막기 위해 None 반환

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
        if UtilFunc.is_duplicate(file, newfile, config):
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
            try:
                # 3a. 파일 이동
                shutil.move(file, newfile)
                logger.info(f"파일 이동 성공: -> {newfile}")

                # 3b. 부가 파일 생성 (이동 성공 후)
                if meta_info:
                    TaskMakeYaml.make_files(
                        meta_info,
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
        ext_config = config.get('확장_ffprobe', {})

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
    def __get_target_with_meta_dvd(config, info):

        if config is None:
            logger.error("Task.config가 초기화되지 않았습니다. 처리를 중단합니다.")
            return None, None

        meta_module = Task.get_meta_module('jav_censored')
        if meta_module is None:
            logger.error("메타데이터 플러그인을 찾을 수 없습니다. 메타 검색을 건너뜁니다.")
            return None, None

        search_name = info['pure_code']
        label = search_name.split("-")[0]

        meta_dvd_labels_exclude = map(str.strip, config.get('메타매칭제외레이블', []))
        if label in map(str.lower, meta_dvd_labels_exclude):
            logger.info("'정식발매 영상 제외 레이블'에 포함: %s", label)
            return None, None

        target_root_str = config.get('메타매칭시이동폴더', '').strip()
        if not target_root_str:
            raise ValueError("'정상 매칭시 이동 경로'가 지정되지 않았습니다.")

        target_root_path = Path(target_root_str)
        if not target_root_path.is_dir():
            raise NotADirectoryError(f"'정상 매칭시 이동 경로'가 존재하지 않음: {target_root_str}")

        # 사용할 사이트 목록 및 점수 결정
        site_search_list = []
        custom_search_settings = config.get('메타검색에사용할사이트')

        if custom_search_settings: # 고급 설정이 있을 경우
            logger.debug("고급 메타 검색 설정을 사용하여 검색합니다.")
            site_search_list = custom_search_settings
            if config.get('메타검색에공식사이트만사용', False):
                site_search_list = [s for s in site_search_list if s['사이트'] in ['dmm', 'mgstage']]
                logger.debug(f"공식 사이트만 사용: {site_search_list}")
        else: # 고급 설정이 없을 경우 (기본 동작)
            try:
                site_order = meta_module.P.ModelSetting.get_list('jav_censored_order', ',')
                site_search_list = [{'사이트': site, '점수': 95} for site in site_order]
            except Exception as e:
                logger.warning(f"메타데이터 플러그인에서 사이트 순서 로드 실패: {e}")
                site_search_list = [{'사이트': 'dmm', '점수': 95}, {'사이트': 'mgstage', '점수': 95}]

        if not site_search_list:
            logger.warning("검색할 사이트 목록이 비어있습니다. 메타 검색을 건너뜁니다.")
            return None, None

        # 결정된 목록으로 순차 검색
        search_name = info['pure_code']
        for search_rule in site_search_list:
            site_name = search_rule['사이트']
            min_score = search_rule['점수']
            try:
                search_result_list = meta_module.search2(search_name, site_name, manual=False)
                if not search_result_list:
                    continue

                best_match = next((item for item in search_result_list if item.get('score', 0) >= min_score), None)
                if best_match:
                    logger.info(f"매칭 성공! 사이트=[{site_name}], 검색어=[{search_name}], 코드=[{best_match['code']}], 점수=[{best_match['score']}] (기준: {min_score})")
                    # 메타검색시 파일처리용으로, 프로세스 단순화 목적으로 fp_meta_mode를 추가했지만,
                    # NFO/YAML 생성 등을 고려하지 않았음...필요하면 추후 로직 업데이트 고려. 지금은 False로 비활성화
                    meta_info = meta_module.info(best_match["code"], keyword=search_name, fp_meta_mode=False)

                    if meta_info:
                        current_target_root = target_root_path
                        original_format = config.get('이동폴더포맷')
                        use_custom_format = False

                        custom_rules = config.get('커스텀경로규칙', [])
                        # 메타 정보가 있을 때는 메타의 레이블을 기준으로, 없으면 파싱된 레이블 기준
                        effective_info = info.copy()
                        effective_info['label'] = meta_info.get("originaltitle", info['pure_code']).split('-')[0]

                        matched_rule = Task.__find_custom_path_rule(effective_info, custom_rules)
                        if matched_rule:
                            custom_path = Path(matched_rule['path'])
                            if custom_path.is_dir():
                                current_target_root = custom_path
                                if matched_rule['format']:
                                    config['이동폴더포맷'] = matched_rule['format']
                                    use_custom_format = True

                        folders = Task.process_folder_format(config, info, config['이동폴더포맷'], meta_info)

                        if use_custom_format:
                            config['이동폴더포맷'] = original_format

                        vr_genres = ["고품질VR", "VR전용", "VR専用", "ハイクオリティVR"]
                        if any(x in (meta_info.get("genre") or []) for x in vr_genres):
                            vr_path_str = config.get('VR영상이동폴더', '').strip()
                            if vr_path_str:
                                current_target_root = Path(vr_path_str)

                        return current_target_root.joinpath(*folders), meta_info

            except Exception as e:
                logger.error(f"'{site_name}' 사이트 검색 중 예외 발생: {e}")
                logger.debug(traceback.format_exc())

        logger.info(f"'{search_name}'에 대한 유효한 메타 정보를 찾지 못했습니다.")

        meta_dvd_labels_include = map(str.strip, config.get('메타매칭포함레이블', []))
        if label in map(str.lower, meta_dvd_labels_include):
            folders = Task.process_folder_format(config, info, config['이동폴더포맷'])
            logger.info("메타 매칭에 실패했지만 '정식발매 영상 포함 레이블'에 해당되어 처리: %s", label)
            return target_root_path.joinpath(*folders), None

        return None, None


    @staticmethod
    def __get_target_with_meta(config, info):
        target_dir, meta_info = Task.__get_target_with_meta_dvd(config, info)
        if target_dir is not None:
            return target_dir, "dvd", meta_info

        # 'no_meta' 처리 케이스
        move_type = "no_meta"

        # 설정에서 '메타 없는 영상 이동 경로'를 가져옴
        no_meta_path_str = config.get('메타매칭실패시이동폴더', '').strip()

        final_path_obj = None
        # 1. 설정된 경로가 있고, 실제로 존재하는 디렉토리인지 확인
        if no_meta_path_str and Path(no_meta_path_str).is_dir():
            # 유효한 경로이므로 Path 객체로 변환하여 사용
            final_path_obj = Path(no_meta_path_str)
            logger.info(f"메타 없음: 설정된 폴더로 이동합니다 - {final_path_obj}")
        else:
            # 2. 설정된 경로가 없거나 유효하지 않으면, '처리실패이동폴더' 아래에 [NO META] 폴더를 사용
            temp_path_str = config.get('처리실패이동폴더', '').strip()
            # Task.start에서 temp_path_str의 유효성은 이미 검증됨
            final_path_obj = Path(temp_path_str).joinpath("[NO META]")
            logger.info(f"메타 없음: 기본 [NO META] 폴더로 이동합니다 - {final_path_obj}")

        return final_path_obj, move_type, None


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
            data.update({
                "studio": meta_data.get("studio", "NO_STUDIO"),
                "code": original_title,
                "actor": f"{','.join(actor_names[:1])}",
                "actor_2": f"{','.join(actor_names[:2])}",
                "actor_3": f"{','.join(actor_names[:3])}",
                "year": meta_data.get("year", ""),
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
        if "studio" not in data:
            data['studio'] = processed_label.upper()

        # 3. {num_X_Y}, {year4} 등 추가 변수 처리
        try:
            if '{' in folder_format and number_part_raw:
                main_number_part = re.split(r'[-_\s]', number_part_raw, 1)[0]
                for match in re.finditer(r'\{num_(\d+)_(\d+)\}', folder_format):
                    x, y = map(int, match.groups())
                    data[f'num_{x}_{y}'] = main_number_part.zfill(y)[:x]
                if '{year4}' in folder_format and re.match(r'^\d{6}$', main_number_part):
                    yy = main_number_part[4:6]
                    data['year4'] = f"20{yy}" if int(yy) < 50 else f"19{yy}"
        except Exception as e:
            logger.error(f"폴더 포맷 변수 처리 중 오류: {e}")

        # 4. 안전한 포맷팅 및 후처리
        safe_fmt = SafeFormatter()
        folders = safe_fmt.format(folder_format, **data)
        folders = re.sub(r'\{[a-zA-Z0-9_.-]+\}', '', folders)
        folders = re.sub(r'\s{2,}', ' ', folders).strip()
        final_parts = [part for part in folders.split("/") if part]
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
        if config.get('드라이런', False):
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

            logger.debug(f"Plex Mate 스캔 API 호출: URL={url}, Data={data}")
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
    def __find_custom_path_rule(info, rules_list):
        """규칙 리스트를 순회하며 파일 정보와 일치하는 첫 번째 커스텀 경로 규칙을 찾습니다."""
        if not rules_list:
            return None

        file_label = info['label']
        original_filename = info['original_file'].name

        for rule in rules_list:
            # 파일명 패턴 조건 확인
            if rule['filename_pattern']:
                try:
                    if re.search(rule['filename_pattern'], original_filename, re.IGNORECASE):
                        logger.info(f"커스텀 경로 규칙 '{rule['name']}' 매칭 (파일명패턴: {original_filename})")
                        return rule
                except re.error as e:
                    logger.error(f"커스텀 경로 규칙 '{rule['name']}'의 파일명 정규식 오류: {e}")
                    continue

            # 레이블 패턴 조건 확인
            if rule['label_pattern']:
                try:
                    if re.search('^' + rule['label_pattern'], file_label, re.IGNORECASE):
                        logger.info(f"커스텀 경로 규칙 '{rule['name']}' 매칭 (레이블: {file_label})")
                        return rule
                except re.error as e:
                    logger.error(f"커스텀 경로 규칙 '{rule['name']}'의 레이블 정규식 오류: {e}")
                    continue
        return None


    @staticmethod
    def _find_external_subtitle(config, info, sub_config):
        """지정된 경로에서 품번에 해당하는 외부 자막 파일이 있는지 확인합니다."""
        rule = sub_config.get('규칙', {})
        base_path_str = rule.get('경로')
        if not base_path_str: return None

        base_path = Path(base_path_str)
        if not base_path.is_dir():
            logger.warning(f"자막 검색 경로가 유효하지 않습니다: {base_path}")
            return None

        # 폴더 구조 포맷 결정 (규칙에 없으면 기본 이동폴더포맷 사용)
        folder_format = rule.get('폴더구조') or config.get('이동폴더포맷')
        
        # 메타 정보 없이 파싱 정보만으로 폴더 경로 생성
        relative_folders = Task.process_folder_format(config, info, folder_format, meta_data=None)
        target_sub_dir = base_path.joinpath(*relative_folders)

        if not target_sub_dir.is_dir():
            # logger.debug(f"예상 자막 폴더를 찾을 수 없습니다: {target_sub_dir}")
            return None

        code_with_hyphen = info['pure_code']
        code_without_hyphen = info['pure_code'].replace('-', '')
        boundary_pattern = r'(?![0-9])'

        patterns = [
            re.compile(re.escape(code_with_hyphen) + boundary_pattern, re.IGNORECASE),
            re.compile(re.escape(code_without_hyphen) + boundary_pattern, re.IGNORECASE)
        ]

        for file in target_sub_dir.iterdir():
            if file.suffix.lower() in sub_config['자막파일확장자']:
                stem = file.stem
                for pattern in patterns:
                    if pattern.search(stem):
                        logger.info(f"외부 자막 파일 발견: {file.name} (품번: {info['pure_code']})")
                        return target_sub_dir
        return None



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

