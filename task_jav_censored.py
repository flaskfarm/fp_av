from .setup import *
from pathlib import Path
import base64, json
import traceback
import re
import os
import shutil
import time
import string
import requests
from datetime import datetime

ModelSetting = P.ModelSetting
from .tool import ToolExpandFileProcess, UtilFunc, VIDEO_EXTS, SUBTITLE_EXTS
from .model_jav_censored import ModelJavCensoredItem
from support import SupportYaml, SupportUtil, SupportDiscord
from .task_make_yaml import Task as TaskMakeYaml


class TaskBase:
    @F.celery.task
    def start(*args):
        logger.info(args)
        job_type = args[0]

        if job_type in ['default', 'dry_run']:
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
            }

            config['PLEXMATE_URL'] = F.SystemModelSetting.get('ddns')

            is_dry_run = config.get('드라이런')
            if is_dry_run:
                logger.warning("Dry Run 모드가 활성화되었습니다. (파일 이동/DB 저장 없음)")
                config['이름'] = "dry_run"

            TaskBase.__task(config)

        elif job_type == 'yaml':
            try:
                yaml_filepath = ModelSetting.get('jav_censored_yaml_filepath') 
                if not yaml_filepath:
                    logger.error("YAML 파일 경로가 설정되지 않았습니다.")
                    return

                yaml_data = SupportYaml.read_yaml(yaml_filepath)
                for job in yaml_data.get('작업', []):
                    if not job.get('사용', True):
                        continue
                    job['재시도'] = False

                    TaskBase.__task(job)
            except Exception as e:
                logger.error(f"YAML 파일 처리 중 오류 발생: {str(e)}")
                logger.error(traceback.format_exc())


    @staticmethod
    def __task(config):
        """
        작업 설정을 받아 공통 로직(사이트 목록 추가 등)을 적용하고 실제 Task를 실행하는 헬퍼 메소드
        """
        # 사이트 목록 로드
        site_list_to_search = []
        use_dmm_mgs_only = config.get('메타검색에공식사이트만사용', ModelSetting.get_bool("jav_censored_meta_dvd_use_dmm_only"))

        if use_dmm_mgs_only:
            logger.info(f"작업 [{config.get('이름', 'N/A')}] : DMM+MGS만 사용하도록 설정됨.")
            site_list_to_search = ['dmm', 'mgstage']
        else:
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

        config['사이트목록'] = site_list_to_search

        # 실제 Task 실행
        Task.start(config)


class SafeFormatter(string.Formatter):
    def get_value(self, key, args, kwargs):
        if isinstance(key, str):
            return kwargs.get(key, f'{{{key}}}')
        else:
            return super().get_value(key, args, kwargs)


class Task:
    config = None
    metadata_modules = {}


    @staticmethod
    def __start_shared_logic(config, task_context):
        """[공용] 모든 JAV 파일 처리 작업의 공통 실행 흐름을 담당합니다."""

        # 0. 파싱 규칙 로드 (meta_module_name을 컨텍스트에서 가져옴)
        Task._load_parsing_rules(config)

        # 1. 파일 목록 수집
        logger.debug(f"처리 파일 목록 생성")
        files = Task.__collect_initial_files(task_context['module_name'])
        if not files:
            logger.info("처리할 파일이 없습니다.")
            return

        # 2. 파싱 및 목록 분리
        logger.debug(f"파싱 및 기본 정보 추출")
        execution_plan = []
        no_label_files = []

        for file in files:
            parsing_rules = config.get('파싱규칙')
            cleanup_list = config.get('품번파싱제외키워드')
            info = Task.__prepare_initial_info(file, parsing_rules, cleanup_list, mode=task_context['parse_mode'])

            if info:
                execution_plan.append(info)
            else:
                no_label_files.append(file)

        logger.debug(f"파싱 성공: {len(execution_plan)}개, 파싱 실패(NO LABEL): {len(no_label_files)}개")

        # 2-1. 파싱 실패 파일 처리
        if no_label_files:
            for file in no_label_files:
                Task.__move_to_no_label_folder(file)

        if not execution_plan:
            logger.debug("파싱에 성공한 파일이 없어 작업을 종료합니다.")
            return

        # 3. 분할 파일 처리
        if config.get('분할파일처리', True):
            logger.debug(f"분할 파일 세트 식별 및 처리 시작")
            Task.__process_part_sets(execution_plan)

        # 4. 최종 파일명 조립
        for info in execution_plan:
            Task.__assemble_final_plan(info)

        # 5. 실제 파일 이동
        task_context['execute_plan'](execution_plan, task_context['db_model'])


    @staticmethod
    def start(config):
        Task.config = config

        # censored 작업에 필요한 고유한 정보(컨텍스트)를 정의
        task_context = {
            'module_name': 'jav_censored',
            'parse_mode': 'censored',
            'execute_plan': Task.__execute_plan,
            'db_model': ModelJavCensoredItem,
        }

        Task.__start_shared_logic(config, task_context)


    # ====================================================================
    # --- 헬퍼 함수들 (Helper Functions) ---
    # ====================================================================


    @staticmethod
    def _load_parsing_rules(config):
        """ metadata 플러그인에서 통합 파싱 규칙을 로드하여 config에 추가합니다."""
        try:
            meta_module = Task.get_meta_module('jav_censored')
            
            if meta_module and hasattr(meta_module, 'get_jav_settings'):
                jav_settings = meta_module.get_jav_settings()
                parsing_rules = jav_settings.get('jav_parsing_rules', {})
                config['파싱규칙'] = parsing_rules
                
                if parsing_rules:
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
    def __move_to_no_label_folder(file_path: Path):
        """품번 추출에 실패한 파일을 '처리실패이동폴더/[NO LABEL]'로 이동시킵니다."""
        config = Task.config

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
    def __collect_initial_files(module_name):
        """파일 시스템에서 처리할 초기 파일 목록을 수집하고, 통계를 반환합니다."""
        config = Task.config
        no_censored_path = Path(config['처리실패이동폴더'].strip())
        if not no_censored_path.is_dir():
            logger.warning("'처리 실패시 이동 폴더'가 유효하지 않아 작업을 중단합니다.")
            return [], {}

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
            if ext in VIDEO_EXTS:
                stats['video'] += 1
            elif ext in SUBTITLE_EXTS:
                stats['subtitle'] += 1
            else:
                stats['etc'] += 1

        total_count = sum(stats.values())
        video_count = stats.get('video', 0)
        subtitle_count = stats.get('subtitle', 0)
        etc_count = stats.get('etc', 0)
        
        log_msg = f"파일 수집 완료: 총 {total_count}개 "
        log_msg += f"(동영상: {video_count}개, 자막: {subtitle_count}개"
        if etc_count > 0:
            log_msg += f", 기타: {etc_count}개"
        log_msg += ")"
        logger.info(log_msg)

        all_files.sort(key=lambda p: [int(c) if c.isdigit() else c.lower() for c in re.split('([0-9]+)', p.name)])

        return all_files


    @staticmethod
    def __prepare_initial_info(file, parsing_rules, cleanup_list, mode='censored'):
        """파일을 파싱하여 메타 검색 이전에 가능한 모든 정보를 추출합니다."""

        parsed = ToolExpandFileProcess.parse_jav_filename(file.name, parsing_rules, cleanup_list, mode=mode)
        if not parsed:
            return {'is_parsed': False, 'original_file': file, 'file_type': 'unparsed'}

        ext = file.suffix.lower()
        if ext in VIDEO_EXTS:
            file_type = 'video'
        elif ext in SUBTITLE_EXTS:
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
            'raw_part': parsed['part'],
            'parsed_part_type': ToolExpandFileProcess._parse_part_number(parsed['part']),
            'ext': parsed['ext'],
            'meta_info': None,
        }
        logger.debug(f"- Parsed: code='{info['pure_code']}', label='{info['label']}', number='{info['number']}', part='{info['parsed_part_type']}'")
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
            if not info['number']:
                continue

            # 1. 원본 stem에서 파싱된 number가 나타나는 모든 위치를 찾음
            indices = [m.start() for m in re.finditer(re.escape(info['number']), stem, re.IGNORECASE)]
            if not indices:
                continue

            for idx in indices:
                # 2. number의 각 위치를 기준으로 뒷부분(tail)을 자름
                tail_part = stem[idx + len(info['number']):]

                # 3. tail에서 숫자 파트 또는 알파벳 파트 패턴을 찾음
                pattern = r'^(?:(?P<part_num>[-_.\s]\d{1,2})|(?P<part_alpha>[-_.\s]?[a-zA-Z]))\b(?P<suffix>.*)$'
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
                        'original_number': info['number'],
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
    def __assemble_final_plan(info):
        """최종 search_name, newfilename을 조립합니다."""
        config = Task.config

        if info.get('is_part_of_set'):
            code = info['part_set_code']
            part = info['parsed_part_type']
            prefix = info['part_set_prefix']
            number = info['part_set_number']
            suffix = info['part_set_suffix']
            total_size = info['part_set_total_size']
            ext = info['ext']

            original_template = f"{prefix}{number}_{suffix}"

            info['newfilename'] = f"{code} [{original_template}({total_size})]{part}{ext}"
            info['search_name'] = code
            return

        info['search_name'] = info['pure_code']
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
    def __execute_plan(execution_plan, db_model):
        """메타 검색, 파일 처리를 실행하고 이동 경로 변경 시 스캔을 요청합니다."""
        config = Task.config

        # 품번 그룹화
        code_groups = {}
        for info in execution_plan:
            code = info['search_name']
            if code not in code_groups: code_groups[code] = []
            code_groups[code].append(info)

        total_files = len(execution_plan)
        logger.info(f"처리할 품번 그룹 {len(code_groups)}개 (총 파일 {total_files}개)")

        delay_seconds = config.get('파일당딜레이', 0)
        scan_enabled = config.get("PLEXMATE스캔", False)
        processed_count = 0
        last_scan_path = None

        for idx, (pure_code, group_infos) in enumerate(code_groups.items()):

            try:
                # 1. 그룹 대표 정보로 target_dir과 meta_info를 한 번만 결정
                meta_target_dir, meta_move_type, meta_info = None, None, None
                if config.get('메타사용') == 'using':
                    meta_target_dir, meta_move_type, meta_info = Task.__get_target_with_meta(pure_code)
                    if delay_seconds > 0:
                        time.sleep(delay_seconds)

                if config.get('메타사용') == 'using':
                    target_dir = meta_target_dir
                    move_type = meta_move_type
                else: # 메타 미사용 시
                    move_type = "normal"
                    target_paths = Task.get_path_list(config['라이브러리폴더'])
                    folders = Task.process_folder_format(move_type, pure_code)
                    target_dir = Path(target_paths[0]).joinpath(*folders) if target_paths else None

                # 2. 스캔 경로 변경 감지 및 요청
                if scan_enabled and target_dir != last_scan_path and last_scan_path is not None:
                    if config.get('드라이런', False):
                        logger.info(f"[Dry Run] 경로 변경 감지. 이전 경로 스캔 요청(Pass): {last_scan_path}")
                    else:
                        logger.info(f"경로 변경 감지. 이전 경로 스캔 요청: {last_scan_path}")
                        Task.__request_plex_mate_scan(last_scan_path)

                # 3. 그룹 내 각 파일 처리
                for info in group_infos:
                    processed_count += 1
                    logger.info(f"[{processed_count:03d}/{total_files:03d}] {info['original_file'].name}")

                    # info 객체에 계산된 최종 정보들을 담아줍니다.
                    info['target_dir'] = target_dir
                    info['move_type'] = move_type
                    info['meta_info'] = meta_info

                    entity = Task.__file_move_logic(info, db_model)

                    if entity and entity.move_type is not None:
                        entity.save()

                # 4. 이동 성공 시, 현재 그룹의 경로를 '마지막 스캔 경로'로 업데이트
                if scan_enabled and target_dir is not None:
                    last_scan_path = target_dir

            except Exception as e:
                logger.error(f"'{pure_code}' 그룹 처리 중 예외 발생: {e}")
                logger.error(traceback.format_exc())

        # 5. 모든 루프가 끝난 후, 마지막으로 처리된 경로에 대해 스캔을 요청
        if scan_enabled and last_scan_path is not None and not config.get('드라이런', False):
            logger.info(f"모든 파일 처리 완료. 마지막 경로 스캔 요청: {last_scan_path}")
            Task.__request_plex_mate_scan(last_scan_path)


    @staticmethod
    def __file_move_logic(info, model_class):
        """실제 파일 이동, 중복 처리, DB 기록, 방송 등을 담당합니다."""
        config = Task.config
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
            if UtilFunc.is_duplicate(file, newfile):
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


    # ====================================================================
    # --- Legacy functions ---
    # ====================================================================

    @staticmethod
    def __get_target_with_meta_dvd(search_name):
        meta_module = Task.get_meta_module('jav_censored')
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
    def __request_plex_mate_scan(scan_path: Path, db_item=None):
        """Plex Mate에 웹 API를 통해 스캔을 요청합니다."""
        if Task.config.get('드라이런', False):
            logger.warning(f"[Dry Run] Plex Mate 스캔 요청 시뮬레이션: {scan_path}")
            return

        try:
            base_url = Task.config.get('PLEXMATE_URL')
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



