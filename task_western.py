from .setup import *
from pathlib import Path
import traceback
import re
import os
import shutil
import time
from datetime import datetime
from itertools import groupby

ModelSetting = P.ModelSetting
from .task_jav_censored import Task as CensoredTask
from .tool import ToolExpandFileProcess, UtilFunc, SafeFormatter
from .model_western import ModelWesternItem
from support import SupportYaml, SupportDiscord
from .task_make_yaml import Task as TaskMakeYaml

class TaskBase:
    @F.celery.task(bind=True, acks_late=False)
    def start(self, *args):
        logger.info(f"Task 수신 인자: {args}")
        job_type = args[0]

        delivery_info = getattr(self.request, 'delivery_info', {}) or {}
        is_redelivered = delivery_info.get('redelivered') or getattr(self.request, 'redelivered', False)

        if is_redelivered:
            logger.warning(f"[{job_type}] 이전 세션 비정상 종료로 재전송(redelivered)된 고아 태스크 실행을 안전하게 취소합니다.")
            return

        is_manual_retry = (job_type == 'manual_path')

        if is_manual_retry and len(args) > 1:
            target_paths = [args[1]]
            logger.info(f"수동 경로 재처리 모드 시작: {target_paths}")
        else:
            target_paths = ModelSetting.get("western_download_path").splitlines()

        change_filename = ModelSetting.get_bool("western_change_filename")
        change_filename_type = ModelSetting.get("western_change_filename_type") or "folder"

        rename_from_folder = change_filename and (change_filename_type == "folder")
        use_meta_filename = change_filename and (change_filename_type == "meta")

        config = {
            "이름": job_type,
            "사용": True,
            "처리실패이동폴더": ModelSetting.get("western_temp_path").strip(),
            "중복파일이동폴더": ModelSetting.get("western_remove_path").strip(),
            "다운로드폴더": target_paths,
            "라이브러리폴더": ModelSetting.get("western_target_path").splitlines(),

            "최소크기": ModelSetting.get_int("western_min_size"),
            "최대기간": ModelSetting.get_int("western_max_age"),
            "품번파싱제외키워드": ModelSetting.get_list("western_filename_cleanup_list", "|"),
            "파일처리하지않을파일명": ModelSetting.get("western_filename_not_allowed_list") or "",

            # 서양식 폴더 포맷 ({studio}/{title} 권장)
            "이동폴더포맷": ModelSetting.get("western_folder_format"),
            "메타사용": ModelSetting.get("western_use_meta"),
            
            "검색키워드정제패턴": ModelSetting.get("western_search_cleanup_pattern") or "",

            "파일명변경": change_filename,
            "파일명변경타입": change_filename_type,
            "폴더명으로파일명변경": rename_from_folder,
            "메타데이터기반파일명변경": use_meta_filename,
            "파일명템플릿": ModelSetting.get("western_filename_template"),

            # 서양에서 사용되지 않는 옵션 생략/False 처리
            "파일명에미디어정보포함": False,
            "분할파일처리": False,
            "원본파일명포함여부": False,

            "메타매칭커트라인": ModelSetting.get_int("western_meta_min_score"),

            "메타매칭시이동폴더": ModelSetting.get("western_meta_path").strip(),
            "메타매칭실패시이동": ModelSetting.get_bool("western_meta_no_move"),
            "메타매칭실패시이동폴더": ModelSetting.get("western_meta_no_path").strip(),
            "매칭실패이동후스캔": ModelSetting.get_bool("western_meta_no_scan_include"),

            "무비타입별도처리": ModelSetting.get_bool("western_movie_type_use_separate_path"),
            "무비타입이동경로": ModelSetting.get("western_movie_type_path").strip(),
            "무비타입폴더포맷": ModelSetting.get("western_movie_type_folder_format").strip(),

            "부가영상인식패턴": re.sub(r'\s+', '', ModelSetting.get("western_featurette_regex") or ""),

            "재시도": True,
            "방송": False,
            "방송경로변환": None,
            
            # 부가파일 생성
            "부가파일생성_YAML": ModelSetting.get_bool("western_make_yaml"),
            "부가파일생성_NFO": ModelSetting.get_bool("western_make_nfo"),
            "부가파일생성_JSON": ModelSetting.get_bool("western_make_json"),
            "부가파일생성_IMAGE": ModelSetting.get_bool("western_make_image"),
            "부가파일생성_TRAILER": ModelSetting.get_bool("western_make_trailer"),
            "부가파일덮어쓰기": ModelSetting.get_bool("western_make_overwrite"),
            "부가파일미디어경로포함": ModelSetting.get_bool("western_include_media_path"),

            # 동반자막
            "동반자막처리활성화": ModelSetting.get_bool("western_companion_enable"),
            "동반자막언어코드추가": ModelSetting.get_bool("western_companion_add_ko"),
            "동반자막한국어자막판별": ModelSetting.get_bool("western_companion_detect_kor"),
            "동반자막경로별도처리": ModelSetting.get_bool("western_companion_use_separate_path"),
            "동반자막처리경로": ModelSetting.get("western_companion_path").strip(),
            "동반자막처리경로_메타실패시": ModelSetting.get("western_companion_meta_fail_path").strip(),

            # 메타 취득 시 임시 오버라이드 옵션
            "공유용라이브러리": ModelSetting.get_bool("western_is_shared_library") if ModelSetting.get("western_is_shared_library") is not None else False,
            "배우이미지우선순위": ModelSetting.get("western_override_actor_img_order") or "",
            "메타이미지모드": ModelSetting.get("western_override_image_mode") or "",
            "메타옵션_이미지제거": ModelSetting.get_bool("western_strip_images") if ModelSetting.get("western_strip_images") is not None else False,

            # 기타
            "파일당딜레이": ModelSetting.get_int("western_delay_per_file"),
            "PLEXMATE스캔": ModelSetting.get_bool("western_scan_with_plex_mate"),
            "드라이런": ModelSetting.get_bool("western_dry_run"),
        }

        # 확장 설정 (YAML 로드)
        config['parse_mode'] = 'western'
        CensoredTask._load_extended_settings(config)

        if is_manual_retry:
            logger.debug("수동 재처리 모드: '메타 매칭 실패 시 이동 설정', '폴더명으로 파일명 변경'을 강제로 False로 전환합니다.")
            config["메타매칭실패시이동"] = False
            config["파일명변경"] = False
            config["폴더명으로파일명변경"] = False
            config["메타데이터기반파일명변경"] = False

        # 작업 실행 분기
        if job_type in ['default', 'dry_run', 'manual_path']:
            final_config = config.copy()
            final_config["이름"] = job_type
            if final_config.get('드라이런', False):
                logger.warning(f"'{final_config['이름']}' 작업: Dry Run 모드가 활성화되었습니다.")

            TaskBase.__task(final_config)

        elif job_type == 'yaml':
            yaml_filepath = args[1]
            try:
                yaml_data = SupportYaml.read_yaml(yaml_filepath)
                if not yaml_data:
                    logger.error(f"YAML 파일을 읽을 수 없거나 비어있습니다: {yaml_filepath}")
                    return

                for job in yaml_data.get('작업', []):
                    if not job.get('사용', True): continue

                    job_name = job.get('이름', '이름 없는 작업')
                    logger.info(f"=========================================")
                    logger.info(f"YAML 작업 실행 시작: [{job_name}]")
                    logger.info(f"=========================================")

                    final_config = config.copy()
                    
                    # Safe Update
                    for key, value in job.items():
                        if value is None or (isinstance(value, str) and not value.strip()):
                            continue
                        final_config[key] = value

                    # 하위 딕셔너리 업데이트
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
                logger.error(traceback.format_exc())

    @staticmethod
    def __task(config):
        config['module_name'] = 'western'
        Task.start(config)


class Task:
    metadata_module = None

    @staticmethod
    def start(config):
        task_context = {
            'module_name': 'western',
            'parse_mode': 'western',
            'execute_plan': Task.__execute_plan,
            'db_model': ModelWesternItem,
        }
        
        folder_format = config.get('이동폴더포맷', '')
        if '{label}' in folder_format or '{code}' in folder_format:
            logger.debug(f"Western 모듈: 폴더 포맷에 {{label}}이나 {{code}} 대신 {{studio}}, {{title}}, {{filename}} 사용을 권장합니다.")

        Task.__start_western_logic(config, task_context)


    @staticmethod
    def __start_western_logic(config, task_context):
        """Western 전용 파일 처리 시작 로직 (Censored에서 분리)"""
        task_context['subtitle_cache'] = {}
        task_context['scanned_directories_cache'] = set()

        # 1. 파일 목록 수집
        all_files = CensoredTask.__collect_initial_files(config, config['module_name'])
        if not all_files:
            return

        rename_map = {} 
        
        if config.get('폴더명으로파일명변경', False):
            from collections import defaultdict
            dir_to_files = defaultdict(list)

            for f in all_files:
                dir_to_files[f.parent].append(f)
            
            for parent_dir, files_in_dir in dir_to_files.items():
                is_sub_dir = any(parent_dir != Path(dl_path) and Path(dl_path) in parent_dir.parents for dl_path in config['다운로드폴더'])
                
                if is_sub_dir:
                    video_exts = config.get('video_exts', set())
                    video_files = [f for f in files_in_dir if f.suffix.lower() in video_exts]
                    
                    if len(video_files) == 1:
                        v_file = video_files[0]
                        folder_name = parent_dir.name
                        
                        if v_file.stem != folder_name:
                            rename_map[v_file] = folder_name
                            logger.info(f"서브 폴더명으로 파일명 교체 예약: '{v_file.name}' -> '{folder_name}{v_file.suffix}'")

        # 2. 서양식 기본 정보 추출
        parsed_infos = []
        unparsed_infos = []

        for file in all_files:
            effective_name = file.name
            if file in rename_map:
                effective_name = rename_map[file] + file.suffix

            info = Task.__prepare_western_initial_info(config, file, effective_name)
            
            if info.get('is_parsed'):
                parsed_infos.append(info)
            else:
                unparsed_infos.append(info)

        if unparsed_infos:
            for info in unparsed_infos:
                CensoredTask.__move_to_no_label_folder(config, info['original_file'])

        if not parsed_infos:
            return

        # 3. 실행 계획 수립 (동반자막/기타 파일 처리)
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
        
        # 동반 자막 처리
        if is_companion_enabled:
            videos, unmatched_subs = ToolExpandFileProcess.pair_companion_subtitles(videos, subtitles, config)
            
            execution_plan.extend(videos)
            execution_plan.extend(unmatched_subs)
        else:
            execution_plan.extend(videos)
            execution_plan.extend(subtitles)

        # 정렬
        file_type_order = {'video': 0, 'subtitle': 1, 'etc': 2}
        execution_plan.sort(key=lambda info: (
            info['original_file'].name, 
            file_type_order.get(info['file_type'], 9)
        ))

        # 4. 서양식 단순 분할 파일 세트 묶기
        Task.__process_western_part_sets(execution_plan)

        # 5. 파일명은 항상 원본 파일명 유지 (단, 분할 대표 키는 유지)
        for info in execution_plan:
            info['newfilename'] = info.get('effective_name', info['original_file'].name)
            info['is_part_of_set'] = False

        # 6. 실행
        task_context['execute_plan'](config, execution_plan, task_context['db_model'], task_context)


    @staticmethod
    def __process_western_part_sets(execution_plan):
        """
        Plex 표준 분할 파일 패턴을 찾아 같은 영화의 파트들을 그룹화합니다.
        Plex 표준: [ ._-](cd|disc|disk|dvd|part|pt)[0-9]+
        """
        import re
        from collections import defaultdict

        video_infos = [info for info in execution_plan if info.get('file_type') == 'video']
        
        # 분할 파일 패턴 (예: " - pt1", "_cd2", ".part3")
        # 정규식 설명: 마지막 확장자 앞에 붙은 Plex 분할 키워드 추출
        part_pattern = re.compile(r'([ ._\-](?:cd|disc|disk|dvd|part|pt)[0-9]+)$', re.IGNORECASE)

        # Base 이름(분할 태그를 제외한 이름)을 기준으로 그룹핑
        base_groups = defaultdict(list)
        for info in video_infos:
            stem = info['original_file'].stem
            match = part_pattern.search(stem)
            
            if match:
                # 분할 태그를 잘라낸 앞부분을 순수 파일명(base_name)으로 취급
                base_name = stem[:match.start()]
                info['western_part_tag'] = match.group(1) # 예: "-pt1"
            else:
                base_name = stem
                info['western_part_tag'] = ""
                
            base_groups[base_name].append(info)

        # 각 그룹별 검증 및 세트 플래그 설정
        for base_name, group_infos in base_groups.items():
            if len(group_infos) < 2:
                continue # 파일이 하나면 세트가 아님

            # 모든 파일이 분할 태그를 가지고 있는지 확인 (섞여 있으면 세트로 묶지 않음)
            if not all(info['western_part_tag'] for info in group_infos):
                continue

            # 서양은 파일명 변경을 안 하므로, 'pure_code'(대표 키워드)만 하나로 통일해 줌
            # 이렇게 하면 나중에 __execute_plan의 groupby 로직에 의해 이 파일들이 하나의 묶음으로 처리됨
            for info in group_infos:
                info['is_part_of_set'] = True
                
                # JAV 시스템이 이들을 같은 영화로 묶어서 1회만 메타 검색/생성 하도록 유도
                info['pure_code'] = base_name 
                info['search_keyword'] = base_name 
                
                # 파일명 변경은 없지만 템플릿(title, filename)용 기준을 분할 태그가 빠진 이름으로 세팅
                info['title'] = base_name
                info['filename'] = base_name

            logger.info(f"Western 분할 파일 세트 식별: '{base_name}' ({len(group_infos)}개 파일)")


    @staticmethod
    def __prepare_western_initial_info(config, file, effective_name=None):
        """서양 영상용 초기 정보 준비 (파일 분류 및 스튜디오 추출)"""
        if not effective_name: effective_name = file.name
        
        ext = file.suffix.lower()
        if ext in config.get('video_exts', set()):
            file_type = 'video'
        elif ext in config.get('subtitle_exts', set()):
            file_type = 'subtitle'
        elif ext in {'.json', '.txt', '.yaml', '.nfo'}:
            file_type = 'meta'
        else:
            file_type = 'etc'

        cleanup_pattern = config.get('검색키워드정제패턴', '')
        featurette_pattern = config.get('부가영상인식패턴', '')
        
        base_info = ToolExpandFileProcess.init_western_info(effective_name, cleanup_pattern, featurette_pattern)

        if not base_info:
            return {'is_parsed': False, 'original_file': file, 'file_type': 'unparsed'}

        txt_file = file.with_suffix('.txt')
        json_file = file.with_suffix('.json')
        manual_endpoint = None
        local_meta_json = None
        
        if file_type == 'video':
            if txt_file.exists() and txt_file.is_file():
                try:
                    with open(txt_file, 'r', encoding='utf-8') as f:
                        content = f.read().strip()
                        # TPDB url 형태인지 간단히 검증
                        if 'api.theporndb.net' in content or '/movies/' in content or '/scenes/' in content:
                            # URL에서 엔드포인트 부분만 추출 (예: /movies/bbf08ee2-...)
                            from urllib.parse import urlparse
                            parsed_url = urlparse(content)
                            manual_endpoint = parsed_url.path # '/movies/uuid' 형태
                            logger.info(f"수동 매칭 TXT 감지: '{file.name}' -> {manual_endpoint}")
                except Exception as e:
                    logger.error(f"수동 매칭 TXT 읽기 에러: {e}")

            # 로컬 JSON 메타데이터 파일 확인 추가
            if json_file.exists() and json_file.is_file():
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        local_meta_json = json.load(f)
                        logger.info(f"로컬 메타데이터 JSON 감지: '{json_file.name}'")
                except Exception as e:
                    logger.error(f"로컬 메타데이터 JSON 읽기 에러: {e}")

        info = {
            'original_file': file,
            'effective_name': effective_name,
            'file_size': file.stat().st_size,
            'file_type': file_type,
            'is_parsed': True,
            'meta_info': None,
            'final_media_info': None,
            'manual_endpoint': manual_endpoint,
            'local_meta_json': local_meta_json
        }
        info.update(base_info)

        return info


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
        
        # 3-1. Movie 타입 식별 및 별도 경로 처리
        is_movie_type = False
        if is_meta_success and meta_info:
            # TPDB 플러그인이 제공하는 속성 확인
            code = meta_info.get('code', '')
            content_type = meta_info.get('content_type', '')
            
            # WPM_1234 처럼 M이 포함되어 있거나, 속성이 'movie'인 경우
            if content_type == 'movie' or (len(code) >= 3 and code[2] == 'M'):
                is_movie_type = True

        if is_movie_type and config.get('무비타입별도처리', False):
            movie_path = config.get('무비타입이동경로')
            movie_format = config.get('무비타입폴더포맷')
            
            if movie_path:
                final_path_str = movie_path
                final_move_type = 'movie_success' 
                is_failed_move = False
                
                # 포맷 지능적 상속
                if movie_format:
                    final_format_str = movie_format
                else:
                    if '{' in movie_path:
                        final_format_str = ""
                        
        # 3-2. 일반 성공/실패/미사용 처리 (무비 타입 별도 처리를 안 탔을 때)
        elif is_meta_success:
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
            # 3-3. 메타 실패 처리
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
            code_tags = ['{code}', '{title}', '{filename}']
            info['is_code_folder'] = any(tag in last_segment for tag in code_tags)
        else:
            info['is_code_folder'] = False

        folders = CensoredTask.process_folder_format(config, info, final_format_str, meta_info)
        
        current_path = base_path
        for folder_part in folders:
            matched_existing_folder = None
            existing_dir_path = None
            
            if current_path.exists() and current_path.is_dir():
                for existing_dir in current_path.iterdir():
                    if existing_dir.is_dir() and existing_dir.name.lower() == folder_part.lower():
                        matched_existing_folder = existing_dir.name
                        existing_dir_path = existing_dir
                        break
            
            if matched_existing_folder:
                if matched_existing_folder == folder_part:
                    final_folder_name = matched_existing_folder
                else:
                    if meta_info:
                        try:
                            temp_path = existing_dir_path.with_name(folder_part + "_TEMP_RENAME")
                            existing_dir_path.rename(temp_path)
                            temp_path.rename(current_path.joinpath(folder_part))
                            
                            logger.info(f"폴더명 교정 (메타데이터 반영): '{matched_existing_folder}' -> '{folder_part}'")
                            final_folder_name = folder_part
                        except Exception as e:
                            logger.warning(f"폴더명 대소문자 교정 실패 (권한 등): {e}. 기존 이름 유지.")
                            final_folder_name = matched_existing_folder
                    else:
                        final_folder_name = matched_existing_folder
            else:
                final_folder_name = folder_part
                
            current_path = current_path.joinpath(final_folder_name)

        target_dir = current_path

        if info.get('is_featurette') and info.get('is_code_folder'):
            target_dir = target_dir.joinpath("Featurettes")
            final_move_type = "featurette_video"

        return target_dir, final_move_type, meta_info


    @staticmethod
    def _get_metadata(config, info):
        local_meta = info.get('local_meta_json')
        if local_meta:
            logger.info(f"'{info.get('pure_code', '')}' 로컬 JSON 메타데이터를 사용하여 검색을 건너뜁니다.")
            return local_meta

        meta_module = CensoredTask.get_meta_module('western')
        if not meta_module:
            return None
            
        min_score_cutoff = config.get('메타매칭커트라인', 80)

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

        search_name = info.get('search_keyword') or info.get('pure_code', '알수없음')

        try:
            delay_seconds = config.get('파일당딜레이', 0)
            if delay_seconds > 0:
                time.sleep(delay_seconds)

            best_match = None
            manual_endpoint = info.get('manual_endpoint')
            
            if manual_endpoint:
                logger.info(f"'{search_name}' 수동 엔드포인트로 강제 메타 획득 시도: {manual_endpoint}")
                if manual_endpoint.startswith('/movies/'):
                    uuid_str = manual_endpoint.replace('/movies/', '')
                    forced_code = f"WPM_{uuid_str}"
                elif manual_endpoint.startswith('/scenes/'):
                    uuid_str = manual_endpoint.replace('/scenes/', '')
                    forced_code = f"WPS_{uuid_str}"
                else:
                    logger.warning(f"지원하지 않는 수동 엔드포인트 형식입니다: {manual_endpoint}")
                    forced_code = None
                
                if forced_code:
                    meta_info = meta_module.info(forced_code, extra_opts=extra_opts)
                    if meta_info:
                        logger.info(f"'{search_name}' 수동 매칭 성공!: {meta_info.get('originaltitle')} (번역스킵:{extra_opts.get('skip_trans', '기본값')}, 공유모드:{is_shared})")
                        return meta_info
                    else:
                        logger.warning(f"'{search_name}' 수동 매칭 실패 (잘못된 URL이거나 서버 오류). 자동 검색으로 넘어갑니다.")

            media_path = str(info['original_file']) if info.get('original_file') else None
            search_result = meta_module.search(search_name, manual=False, media_path=media_path)
            
            if search_result:
                best_match = next((item for item in search_result if item.get('score', 0) >= min_score_cutoff), None)
            
            if best_match:
                meta_info = meta_module.info(best_match["code"], extra_opts=extra_opts)
                if meta_info:
                    match_site = best_match.get('site', 'N/A')
                    logger.info(f"'{search_name}' 메타 검색 성공: {meta_info.get('originaltitle')} (from: {match_site}, 번역스킵:{extra_opts.get('skip_trans', '기본값')}, 공유모드:{is_shared})")
                    return meta_info
            
            logger.info(f"'{search_name}'에 대한 유효한 메타 정보를 찾지 못했습니다.")
            
        except Exception as e:
            logger.error(f"Western 메타 검색 에러 ('{search_name}'): {e}")
            logger.error(traceback.format_exc())
            
        return None


    @staticmethod
    def assemble_western_filename(config, info, meta_info):
        """서양 메타데이터 기반 파일명 조립 (남자 배우 배제, actor_N 지원, res_tag 연동)"""
        template = config.get('파일명템플릿') or "{studio} - {actor_3} - {title} ({year}) [{res_tag}]"
        file_path = info['original_file']
        safe_fn = ToolExpandFileProcess.get_safe_filename

        # 배우 필터링 (남자 배우 배제)
        actor_list = meta_info.get('actor') or []
        valid_actors = []
        for a in actor_list:
            if not isinstance(a, dict):
                continue
            gender = str(a.get('gender') or '').strip().lower()
            role = str(a.get('role') or '').strip().lower()
            if gender in ['male', 'm', '남자', '남성'] or role in ['male', 'm', '남자', '남성']:
                continue
            name = a.get('name') or a.get('name_ko') or a.get('name_org') or a.get('name_en')
            if name:
                valid_actors.append(safe_fn(str(name).strip()))

        # 해상도 및 미디어 정보 추출 (ffprobe 우선, 실패 시 정규식 폴백)
        res_tag, v_codec, a_codec = "", "", ""
        ext_config = config.get('미디어정보설정', {})
        media_info = info.get('media_info')
        if not media_info:
            ext_config = config.get('미디어정보설정', {})
            media_info = ToolExpandFileProcess._get_media_info(file_path, ext_config)
            info['media_info'] = media_info
        
        if media_info and media_info.get('is_valid'):
            res_tag = media_info.get('res_tag', '')
            v_codec = media_info.get('v_codec', '')
            a_codec = media_info.get('a_codec', '')
        else:
            stem_lower = file_path.stem.lower()
            if re.search(r'\b(4320p|8k)\b', stem_lower): res_tag = "8K"
            elif re.search(r'\b(2880p|6k)\b', stem_lower): res_tag = "6K"
            elif re.search(r'\b(2160p|4k|uhd)\b', stem_lower): res_tag = "4K"
            elif re.search(r'\b(1440p|3k|2k|qhd)\b', stem_lower): res_tag = "3K"
            elif re.search(r'\b(1080p|fhd)\b', stem_lower): res_tag = "FHD"
            elif re.search(r'\b(720p|hd)\b', stem_lower): res_tag = "HD"
            elif re.search(r'\b(480p|576p|sd)\b', stem_lower): res_tag = "SD"

        file_size = info.get('file_size')
        if file_size is None and file_path.exists():
            try:
                file_size = file_path.stat().st_size
            except Exception:
                file_size = 0
        file_size_str = str(file_size or 0)

        # 템플릿 변수 매핑
        studio_str = safe_fn(meta_info.get('studio') or info.get('studio', 'NO_STUDIO'))

        # Western 메타데이터는 'title'에 '[Studio] Actor - Title'을 조합해 넣고, 'originaltitle'에 순수 제목을 넣음
        raw_title = meta_info.get('originaltitle') or meta_info.get('title') or file_path.stem

        # 혹시 originaltitle이 없어 title로 폴백했을 때 '[Studio] Actor - ' 접두어가 붙어있는 경우 정제
        if raw_title.startswith('[') and ']' in raw_title:
            bracket_end = raw_title.find(']')
            after_bracket = raw_title[bracket_end + 1:].strip()
            # 대괄호 뒤에 '배우 - ' 가 붙어있으면 배우명도 제거
            for actor_name in valid_actors:
                if after_bracket.lower().startswith(actor_name.lower()):
                    after_bracket = after_bracket[len(actor_name):].strip(' -_')
                    break
            if after_bracket:
                raw_title = after_bracket

        title_str = safe_fn(raw_title)

        year_str = str(meta_info.get('year') or '')
        code_str = safe_fn(meta_info.get('code') or file_path.stem)

        studio_1 = '#'
        if studio_str:
            match = re.search(r'[a-zA-Z0-9]', studio_str)
            studio_1 = "09" if match and match.group(0).isdigit() else (match.group(0).upper() if match else '#')

        data = {
            'studio': studio_str,
            'studio_1': studio_1,
            'title': title_str,
            'year': year_str,
            'code': code_str,
            'filename': safe_fn(file_path.stem),
            'res_tag': res_tag,
            'v_codec': v_codec,
            'a_codec': a_codec,
            'actor': ", ".join(valid_actors),
            'bytes': file_size_str,
            'filesize': file_size_str,
        }

        # 동적 {actor_N} 변수 생성
        for match in re.finditer(r'\{actor_(\d+)\}', template):
            n = int(match.group(1))
            data[f'actor_{n}'] = ", ".join(valid_actors[:n])

        # 포맷팅 및 안전한 파일명 정제
        safe_fmt = SafeFormatter()
        formatted = safe_fmt.format(template, **data)
        formatted = formatted.replace("()", "").replace("[]", "").replace("{}", "")
        formatted = re.sub(r'\{[a-zA-Z0-9_.-]+\}', '', formatted)
        formatted = re.sub(r'\s{2,}', ' ', formatted)
        formatted = re.sub(r'\s*-\s*-\s*', ' - ', formatted)
        formatted = safe_fn(formatted.strip(' ._-'))

        # Plex 분할 파일 태그(-pt1 등)가 있다면 끝에 보존
        part_tag = info.get('western_part_tag', '')
        if part_tag and not formatted.endswith(part_tag):
            formatted += part_tag

        return f"{formatted}{file_path.suffix.lower()}"


    @staticmethod
    def _get_first_letter(text: str) -> str:
        """
        문자열의 첫 글자를 기준으로 알파벳(A-Z), 숫자(09), 기타(#)를 반환합니다.
        대괄호나 기호로 시작하더라도 첫 번째 유효 영문/숫자를 탐색합니다.
        """
        if not text:
            return '#'
        match = re.search(r'[a-zA-Z0-9]', text)
        if match:
            ch = match.group(0)
            if ch.isdigit():
                return '09'
            else:
                return ch.upper()
        return '#'
    @staticmethod
    def _get_subfolder_to_move(file_path: Path, download_paths: list) -> Path | None:
        """
        파일이 다운로드 폴더 하위의 서브폴더에 속해 있는지 검사합니다.
        - 서브폴더에 속한 경우: 해당 서브폴더(file_path.parent) 반환
        - 다운로드 루트에 직접 위치한 단독 파일인 경우: None 반환 (다운로드 폴더 자체 이동 방지)
        """
        try:
            file_resolved = file_path.resolve()
            parent_resolved = file_resolved.parent
            
            for dl in download_paths:
                if not str(dl).strip():
                    continue
                dl_resolved = Path(dl).resolve()
                
                # 파일의 부모가 다운로드 루트 자체인 경우 -> 단독 파일
                if parent_resolved == dl_resolved:
                    return None
                
                # 파일의 부모가 다운로드 루트의 하위 폴더인 경우 -> 이동할 서브폴더
                if dl_resolved in parent_resolved.parents:
                    return file_path.parent
        except Exception as e:
            logger.error(f"경로 검사 중 오류 ({file_path}): {e}")
            
        return None

    @staticmethod
    def __file_move_logic(config, info, model_class, task_context=None):
        """Western 전용 파일/폴더 이동 로직 (메타 실패 시 폴더 보존 및 알파벳 서브폴더 자동 분류)"""
        if task_context is None:
            task_context = {}
        moved_folders = task_context.setdefault('moved_folders', {})
        
        is_dry_run = config.get('드라이런', False)
        file = info['original_file']
        newfilename = info.get('newfilename', file.name)
        target_dir = info.get('target_dir')
        move_type = info.get('move_type')

        entity = model_class(config.get('이름'), str(file.parent), file.name)

        if move_type is None or target_dir is None:
            logger.warning(f"'{file.name}'의 최종 이동 경로를 결정할 수 없어 건너뜁니다.")
            return entity.set_move_type(None)

        if move_type == "failed_video":
            failed_video_path_str = config.get('처리실패이동폴더', '').strip()
            if not failed_video_path_str and not config.get('미디어정보실패시이동경로'):
                logger.info(f"미디어 분석 실패: '처리실패이동폴더'가 비어있어 이동을 건너뜁니다: {file.name}")
                return entity.set_move_type("failed_video_skipped")

            newfile = target_dir.joinpath(file.name)

            if is_dry_run:
                logger.warning(f"[Dry Run] 미디어 분석 실패 이동 예정: '{file}' -> '{newfile}'")
                return None

            target_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(file), str(newfile))
            logger.info(f"미디어 분석 실패 파일을 이동했습니다: {newfile}")
            return entity.set_target(newfile).set_move_type(move_type)

        # 메타 매칭 실패 (meta_fail, no_meta) 처리
        if move_type in ["no_meta", "meta_fail"]:
            # 서브폴더 존재 여부 확인 (다운로드 루트 폴더 보호)
            subfolder = Task._get_subfolder_to_move(file, config.get('다운로드폴더', []))

            # 이동 경로에 포맷({..}) 지정 여부 확인 및 알파벳 서브폴더 적용
            fail_path_setting = config.get('메타매칭실패시이동폴더', '').strip()
            has_custom_format = ('{' in fail_path_setting and '}' in fail_path_setting)

            # 포맷 미지정 시: 서브폴더면 폴더명, 단독 파일이면 파일명 기준으로 첫 글자 추출
            base_name_for_letter = subfolder.name if subfolder else file.stem
            first_letter = Task._get_first_letter(base_name_for_letter)

            # 포맷 템플릿이 없을 때만 알파벳 서브폴더(A-Z, 09, #)를 타겟 경로 하위에 추가
            dest_dir = target_dir
            if not has_custom_format:
                dest_dir = target_dir.joinpath(first_letter)

            if subfolder:
                subfolder_key = str(subfolder.resolve())

                # 이미 앞선 분할 파일에 의해 폴더가 통째로 이동된 경우
                if subfolder_key in moved_folders:
                    dst_folder = moved_folders[subfolder_key]
                    newfile = dst_folder.joinpath(file.name)
                    logger.info(f"메타 실패 폴더가 이미 이동되었습니다 (동일 폴더 내 파일): {newfile}")
                    return entity.set_target(newfile).set_move_type(move_type)

                # 처음으로 서브폴더를 통째로 이동하는 경우
                dst_folder = dest_dir.joinpath(subfolder.name)

                if is_dry_run:
                    logger.warning(f"[Dry Run] 메타 실패 (서브폴더 전체 이동 예정): '{subfolder}' -> '{dst_folder}'")
                    return None

                dest_dir.mkdir(parents=True, exist_ok=True)

                try:
                    if dst_folder.exists():
                        timestamp = int(datetime.now().timestamp())
                        dst_folder = dest_dir.joinpath(f"[{timestamp}] {subfolder.name}")

                    shutil.move(str(subfolder), str(dst_folder))
                    moved_folders[subfolder_key] = dst_folder
                    logger.info(f"메타 실패 폴더 전체를 이동했습니다: '{subfolder}' -> '{dst_folder}'")

                    newfile = dst_folder.joinpath(file.name)
                    return entity.set_target(newfile).set_move_type(move_type)
                except Exception as e:
                    logger.error(f"메타 실패 폴더 이동 중 오류: {subfolder} -> {dst_folder}, 오류: {e}")
                    logger.error(traceback.format_exc())
                    return entity.set_move_type("move_fail")

            else:
                # 단독 파일인 경우: 다운로드 폴더는 보존하고 파일만 단독 이동
                newfile = target_dir.joinpath(newfilename)

                if is_dry_run:
                    logger.warning(f"[Dry Run] 메타 실패 (단독 파일 이동 예정): '{file}' -> '{newfile}'")
                    return None

                target_dir.mkdir(parents=True, exist_ok=True)
                try:
                    if newfile.exists():
                        file.unlink()
                        return entity.set_move_type("no_meta_deleted_due_to_duplication")
                    else:
                        shutil.move(str(file), str(newfile))
                        logger.info(f"메타 실패 단독 파일을 이동했습니다: {newfile}")
                        return entity.set_target(newfile).set_move_type(move_type)
                except Exception as e:
                    logger.error(f"메타 실패 파일 이동 중 오류: {file} -> {newfile}, 오류: {e}")
                    return entity.set_move_type("move_fail")

        # 메타 성공 등 일반 이동은 기존 공통 로직 사용
        return CensoredTask.__file_move_logic(config, info, model_class)


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
            'normal', 'subbed', 'custom_path', 
            'companion_kor', 'companion_kor_sub',
            'meta_success', 'movie_success',
            'vr', 'featurette_video'
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
            
            first_info = next((i for i in group_infos if i.get('file_type') == 'video'), group_infos[0])
            has_local_meta = bool(first_info.get('local_meta_json'))
            
            group_log_prefix = f"[{item_count+1:03d}/{len(execution_plan):03d}]"
            logger.info(f"{group_log_prefix} 메타 검색 및 경로 계산: {first_info.get('newfilename', first_info['original_file'].name)}")

            config['_skip_trans_temp'] = (not any_meta_option_on) or (not make_overwrite)
            target_dir_for_group, _, meta_info_for_group = Task._get_final_target_path(config, first_info, task_context, do_meta_search=True)

            if meta_info_for_group and any_meta_option_on and not make_overwrite and not has_local_meta:
                needs_files = False
                if target_dir_for_group:
                    if not target_dir_for_group.exists():
                        needs_files = True
                    else:
                        is_code_folder = first_info.get('is_code_folder', False)
                        video_stem = Path(first_info.get('newfilename', first_info['original_file'].name)).stem
                        
                        if first_info.get('is_part_of_set') and first_info.get('western_part_tag'):
                            part_tag = first_info.get('western_part_tag')
                            if video_stem.endswith(part_tag):
                                video_stem = video_stem[:-len(part_tag)]
                                
                        identifier = video_stem
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

            if meta_info_for_group and config.get('메타데이터기반파일명변경', False):
                for info in group_infos:
                    if info.get('file_type') == 'video':
                        new_name = Task.assemble_western_filename(config, info, meta_info_for_group)
                        info['newfilename'] = new_name
                        logger.info(f"메타데이터 기반 파일명 생성: '{info['original_file'].name}' -> '{new_name}'")

            processed_dirs_for_group = set()

            for info in group_infos:
                try:
                    item_count += 1
                    log_prefix = f"[{item_count:03d}/{total_items_in_plan:03d}]"
                    
                    logger.debug(f"{log_prefix} 개별 파일 처리 시작: {info.get('newfilename', info['original_file'].name)}")
                    
                    target_dir, move_type, meta_info = None, None, meta_info_for_group

                    template = config.get('파일명템플릿') or ''
                    need_media_info = config.get('메타데이터기반파일명변경', False) and any(k in template for k in ['{res_tag}', '{v_codec}', '{a_codec}', '{fps}'])
                    
                    is_media_info_failed = False
                    if need_media_info and info.get('file_type') == 'video':
                        ext_config = config.get('미디어정보설정', {})
                        media_info_res = ToolExpandFileProcess._get_media_info(info['original_file'], ext_config)
                        info['media_info'] = media_info_res
                        if not media_info_res or not media_info_res.get('is_valid', True):
                            is_media_info_failed = True

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
                        logger.warning(f"'{info['original_file'].name}'의 이동 경로를 결정할 수 없어 건너뜁니다.")
                        continue
                    
                    if move_type in ['no_meta', 'meta_fail']:
                        fail_path_setting = config.get('메타매칭실패시이동폴더', '').strip()
                        has_custom_format = ('{' in fail_path_setting and '}' in fail_path_setting)
                        subfolder = Task._get_subfolder_to_move(info['original_file'], config.get('다운로드폴더', []))

                        if subfolder:
                            # A. 서브폴더가 있는 경우: 서브폴더명 첫 글자 알파벳 하위 경로 적용
                            if not has_custom_format:
                                first_letter = Task._get_first_letter(subfolder.name)
                                target_dir = target_dir.joinpath(first_letter)
                        else:
                            # B. 단독 파일인 경우: 파일명 앞단의 스튜디오를 파싱하여 [알파벳]/[스튜디오] 폴더로 지정
                            studio = info.get('studio') or info.get('label') or ''
                            safe_studio = ToolExpandFileProcess.get_safe_filename(studio)
                            if safe_studio and safe_studio.lower() not in ['unknown', 'no_studio']:
                                first_letter = Task._get_first_letter(safe_studio)
                                target_dir = target_dir.joinpath(safe_studio) if has_custom_format else target_dir.joinpath(first_letter, safe_studio)
                            else:
                                first_letter = Task._get_first_letter(info['original_file'].stem)
                                target_dir = target_dir if has_custom_format else target_dir.joinpath(first_letter)

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
                                    target_json_name = Path(info.get('newfilename', info['original_file'].name)).stem
                                    if info.get('is_part_of_set') and info.get('western_part_tag'):
                                        part_tag = info.get('western_part_tag')
                                        if target_json_name.endswith(part_tag):
                                            target_json_name = target_json_name[:-len(part_tag)]
                                            
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
                        
                        if info['should_create_meta'] and meta_info_for_group and move_type != 'featurette_video':
                            logger.info(f"{log_prefix} 부가 파일 사전 준비 중...: {target_dir}")
                            printable_meta_info = meta_info_for_group.copy()
                            printable_meta_info.pop('original', None)
                            
                            video_stem_for_meta = Path(info.get('newfilename', info['original_file'].name)).stem
                            if info.get('is_part_of_set') and info.get('western_part_tag'):
                                part_tag = info.get('western_part_tag')
                                if video_stem_for_meta.endswith(part_tag):
                                    video_stem_for_meta = video_stem_for_meta[:-len(part_tag)]

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
                                    module_name='western',
                                    original_filename=video_stem_for_meta
                                )
                            except Exception as meta_e:
                                logger.error(f"부가 파일 생성 중 오류: {meta_e}")

                    is_meta_fail = (move_type in ['no_meta', 'meta_fail'])
                    is_subfolder_fail = is_meta_fail and bool(Task._get_subfolder_to_move(info['original_file'], config.get('다운로드폴더', [])))

                    # 2. 동반 자막 선이동 (단, 메타 실패 폴더 통째 이동 대상인 경우 자막도 폴더에 보존되므로 개별 선이동 제외)
                    if 'companion_subs_list' in info and not is_subfolder_fail:
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

                    # 3. 본 영상 이동 (Western 전용 __file_move_logic 호출)
                    entity = Task.__file_move_logic(config, info, db_model, task_context)
                    
                    if entity or config.get('드라이런', False):
                        if entity: 
                            if entity.target_path:
                                entity.save()
                                
                                if info.get('file_type') == 'video':
                                    txt_file = info['original_file'].with_suffix('.txt')
                                    if txt_file.exists() and info.get('manual_endpoint') and not config.get('드라이런', False):
                                        try:
                                            txt_file.unlink()
                                            logger.debug(f"수동 매칭 트리거 파일 삭제 완료: {txt_file.name}")
                                        except Exception as e:
                                            logger.warning(f"수동 매칭 트리거 파일 삭제 실패: {txt_file.name}, 오류: {e}")
                            else:
                                logger.warning(f"'{info['original_file'].name}' 이동 실패: 대상 경로가 없습니다.")
                                continue

                        # 4. 스캔 요청
                        if scan_enabled and entity and entity.target_path and entity.move_type in valid_scan_types:
                            if info.get('file_type') == 'video':
                                CensoredTask.__request_plex_mate_scan(config, Path(entity.target_path), entity)

                        # 4. 스캔 요청
                        if scan_enabled and entity and entity.target_path and entity.move_type in valid_scan_types:
                            if info.get('file_type') == 'video':
                                CensoredTask.__request_plex_mate_scan(config, Path(entity.target_path), entity)

                except Exception as e:
                    logger.error(f"'{info.get('pure_code', '알 수 없음')}' 파일 처리 중 예외 발생: {e}")
                    logger.error(traceback.format_exc())

        logger.info("fp_av_western: 모든 작업이 완료되었습니다.")
