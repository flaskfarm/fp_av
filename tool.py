import os
import re
import shutil
import time
import traceback
import string
import subprocess
import json

from os import PathLike
from pathlib import Path
from typing import Generator

from .setup import P, logger
from support import SupportUtil
from typing import Dict, Any


class SafeFormatter(string.Formatter):
    def get_value(self, key, args, kwargs):
        if isinstance(key, str):
            return kwargs.get(key, f'{{{key}}}')
        else:
            return super().get_value(key, args, kwargs)


class ToolExpandFileProcess:

    VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".ts", ".wmv", ".m2ts", "mts", ".m4v", ".flv", ".asf", ".mpg", ".ogm"}
    SUBTITLE_EXTS = {".srt", ".smi", ".ass", ".ssa", "idx", "sub", ".sup", ".ttml", ".vtt"}

    ##########################
    # preprocess_cleanup
    ##########################
    @classmethod
    def preprocess_cleanup(cls, path, min_size: int = 0, max_age: int = 0):
        """max_age in sec"""
        path = Path(path)
        if not path.is_dir():
            return
        # iterate depth=1 items only
        for child in path.iterdir():
            if child.stat().st_mtime < time.time() - max_age:
                # old enough
                if child.is_dir() and not list(cls._iterdir(child, min_size=min_size)):
                    shutil.rmtree(child)
                elif child.is_file() and not cls._is_legit_file(child, min_size=min_size):
                    child.unlink()

    @classmethod
    def _iterdir(cls, path, min_size: int = 0) -> Generator[Path, None, None]:
        """generate a list of file with specific conditions

        files-only / recursive / min_size in MiB
        """
        path = Path(path)
        if not path.is_dir():
            return
        for file in path.rglob("*"):
            if not cls._is_legit_file(file):
                continue
            if file.is_dir():
                continue
            if cls._is_legit_file(file, min_size=min_size):
                yield file


    @classmethod
    def _is_legit_file(cls, path: PathLike, min_size: int = 0):
        if not path.is_file():
            return False
        return path.stat().st_size >= min_size * 1024**2 or path.suffix.lower() in cls.SUBTITLE_EXTS


    ##########################
    # preprocess_listdir
    ##########################
    @classmethod
    def preprocess_listdir(cls, source, errpath, config):
        is_dry_run = config.get('드라이런', False)

        source = Path(source)
        if not source.is_dir():
            return

        min_size = config.get('최소크기', 0)
        disallowed_keys = config.get('파일처리하지않을파일명', [])

        files = []
        for file in cls._iterdir(source, min_size=min_size):
            if file.suffix.lower() in cls.SUBTITLE_EXTS:
                files.append(file)
                continue

            newdir = None
            if len(file.name) == 40 and file.name.isalnum():
                newdir = errpath.joinpath("[FILENAME HASHED]")
            elif len(file.name.encode("utf-8")) > 200:
                newdir = errpath.joinpath("[FILENAME TOO LONG]")
            elif any(p in file.name.lower() for p in map(str.lower, disallowed_keys)):
                newdir = errpath.joinpath("[FILENAME NOT ALLOWED]")
            else:
                files.append(file)
                continue

            if newdir is not None:
                if not is_dry_run:
                    newdir.mkdir(parents=True, exist_ok=True)

                newfile = newdir.joinpath(file.name)

                if is_dry_run:
                    logger.warning(f"[Dry Run] 전처리: '{file.name}' -> '{newfile}' (이동 예정)")
                else:
                    newdir.mkdir(parents=True, exist_ok=True)
                    shutil.move(file, newfile)
            else:
                if errpath is None:
                    logger.warning(f"전처리 실패 파일 건너뛰기 (처리실패폴더 미지정): {file.name}")

        return files


    # --- 처리 메인 함수 ---

    @classmethod
    def parse_jav_filename(cls, original_filename, parsing_rules=None, cleanup_list=None, mode='censored'):
        if not original_filename or not isinstance(original_filename, str):
            return None
        logger.debug(f"filename: '{original_filename}'")

        base, ext = os.path.splitext(original_filename)

        try:
            # 전처리
            cleaned_base = cls._preprocess_base(base, cleanup_list=cleanup_list)
            parsed_code, remaining_part = None, ""

            # 품번 추출
            if parsing_rules:
                special_key = f"{mode}_special_rules"
                special_rules = parsing_rules.get(special_key, [])
                generic_rules = parsing_rules.get('generic_rules', [])
                all_rules = special_rules + generic_rules
                if all_rules:
                    parsed_code, remaining_part = cls._apply_parsing_rules(cleaned_base, all_rules)

            if not parsed_code:
                parsed_code, remaining_part = cls._apply_fallback_rules(cleaned_base)

            if parsed_code:
                label_part, number_part_raw = parsed_code

                if mode == 'censored' and number_part_raw.isdigit():
                    number_part_processed = str(int(number_part_raw)).zfill(3)
                else:
                    number_part_processed = number_part_raw

                code_part = f"{label_part}-{number_part_processed}" if number_part_processed else label_part

                return {
                    'code': code_part,
                    'label': label_part,
                    'number': number_part_processed,
                    'raw_number': number_part_raw,
                    'part': remaining_part,
                    'ext': ext
                }

        except Exception as e:
            logger.error(f"파일명 파싱 중 오류 발생: {original_filename} - {e}")
            logger.error(traceback.format_exc())
            raise e

        return None


    # --- 헬퍼 함수들 ---

    @classmethod
    def _preprocess_base(cls, base, cleanup_list=None):
        """파일명(base)에서 일반적인 노이즈를 제거합니다."""
        base = base.lower() # 모든 처리는 소문자 기준으로

        # 사용자 정의 목록을 사용하여 1차 정리
        if cleanup_list:
            # 정규식 특수문자를 이스케이프하고, 긴 키워드가 먼저 처리되도록 정렬
            sorted_list = sorted([re.escape(kw.strip()) for kw in cleanup_list if kw.strip()], key=len, reverse=True)
            if sorted_list:
                # 모든 키워드를 | 로 연결하여 하나의 정규식 패턴 생성
                pattern = '|'.join(sorted_list)
                # \b(키워드1|키워드2|...)\b 와 같은 형태로 단어 단위로만 치환
                base = re.sub(r'\b(' + pattern + r')\b', ' ', base, flags=re.I)

        # 괄호를 공백으로 변환하고, 연속된 공백을 하나로 합침
        base = re.sub(r'[\[\]\(\)]+', ' ', base)
        base = re.sub(r'\s+', ' ', base).strip()

        # 웹사이트 주소 제거 (앞에 다른 문자가 있어도 처리)
        tlds = 'cc|cn|com|net|me|org|xyz|vip|tv|la'
        base = re.sub(r'[\w.-]+\.(%s)[-@_ ]' % tlds, '', base).strip()

        # DMM 접두사 제거
        base = re.sub(r'^[hn]_\d', '', base, flags=re.I)

        # 화질/코덱 등 '명백한' 접미사 제거
        misc_suffixes = r'[-_. ](720p|1080p|2160p|2k|4k|8k|sd|fhd|uhd|hq|uhq|h264|h265|hevc)'
        combined_pattern = r'(%s)?$' % (misc_suffixes)
        base = re.sub(combined_pattern, '', base, flags=re.I)

        # DMM 전용 'rz' 접미사 제거
        base = re.sub(r'[rz]$', '', base, flags=re.I)

        # 최종적으로 앞뒤의 불필요한 구분 기호 제거
        base = base.strip(' ._-')

        # logger.debug(f"- 전처리 후 base: {base}")
        return base


    @classmethod
    def _apply_parsing_rules(cls, base, rules_list):
        """주어진 규칙 리스트를 순서대로 적용하여 품번을 파싱합니다."""
        # logger.debug(f"  - 파싱 규칙 적용 시작. 총 {len(rules_list)}개 규칙, 대상: '{base}'")

        for i, line in enumerate(rules_list):
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            parts = line.split('=>')
            if len(parts) != 2:
                logger.warning(f"    - 규칙 {i+1}: 형식 오류 (건너뜀) - '{line}'")
                continue

            pattern, template = parts[0].strip(), parts[1].strip()
            # logger.debug(f"    - 규칙 {i+1} 시도: 패턴='{pattern}'")

            try:
                match = re.match(pattern, base, re.I)
                if match:
                    # logger.debug(f"      - 매칭 성공! 그룹: {match.groups()}")
                    groups = match.groups()
                    label_part, num_part = "", ""

                    if '|' in template: # label|number 형식
                        label_template, num_template = [t.strip() for t in template.split('|', 1)]
                        try:
                            label_part = label_template.format(*groups)
                            num_part = num_template.format(*groups)
                        except IndexError as e:
                            logger.error(f"        - 템플릿 적용 오류 (그룹 인덱스): {e}...")
                            continue
                    else: # 단일 템플릿 형식. 전체를 label, number는 빈 값으로 간주
                        try:
                            label_part = template.format(*groups)
                        except IndexError as e:
                            logger.error(f"        - 템플릿 적용 오류 (그룹 인덱스): {e}...")
                            continue

                    matched_string = match.group(0)
                    remaining_part = base[base.find(matched_string) + len(matched_string):]

                    logger.debug(f"  - Pre-Parsed: {base} > label='{label_part}', num='{num_part}'")
                    return (label_part.lower(), num_part), remaining_part

            except (IndexError, re.error) as e:
                logger.error(f"    - 규칙 {i+1} 적용 중 예외 발생: {e} - '{line}'")

        return None, "" # 실패 시 (None, "") 반환


    @classmethod
    def _apply_fallback_rules(cls, base):
        """최종 폴백으로 원본 파일명에서 (label, number) 튜플을 찾습니다."""
        match = re.search(r"\b(?P<code>[a-z]+[-_]?\d+)(?P<part>.*)", base, re.I)
        if match:
            code = match.group('code').replace('_', '-')
            part_str = match.group('part')

            label_part, number_part = "", ""
            if '-' in code:
                parts = code.rsplit('-', 1)
                if len(parts) == 2 and parts[1].isdigit():
                    label_part, number_part = parts
                else: # carib-123125-001 같은 경우
                    label_part = code
            else: # 하이픈 없는 경우
                match_ln = re.match(r'([a-zA-Z]+)(\d+)', code)
                if match_ln:
                    label_part, number_part = match_ln.groups()
                else:
                    label_part = code

            logger.debug(f"  - 폴백 규칙 매칭 성공: label='{label_part}', number='{number_part}', part='{part_str}'")
            return (label_part, number_part), part_str

        logger.debug("  - 폴백 규칙 매칭 실패")
        return None, ""


    ##########################
    # advanced naming process
    ##########################

    @staticmethod
    def _format_conditional_template(template_str, data):
        """
        [[...]] 구문을 해석하여 조건부 템플릿을 포맷팅합니다.
        예: "[[{res_tag}]][[{v_codec}]]"
        """
        def repl(match):
            inner_template = match.group(1)
            try:
                keys = re.findall(r'\{(\w+)\}', inner_template)
                if not keys or all(data.get(key) for key in keys):
                    # 조건 만족 시, 내부 템플릿을 포맷팅하여 반환
                    return string.Formatter().format(inner_template, **data)
                else:
                    # 조건 불만족 시, 블록 전체를 빈 문자열로 대체
                    return ""
            except Exception as e:
                logger.error(f"조건부 템플릿 포맷팅 중 오류: {e}")
                return ""

        # [[ 와 ]] 사이의 내용을 찾아 repl 함수로 치환
        formatted_str = re.sub(r'\[\[(.*?)\]\]', repl, template_str, flags=re.DOTALL)

        # 후처리: 연속된 공백을 하나로 만들고, 양 끝의 불필요한 공백/구분자 제거
        # 예: " FHD. .H264 " -> "FHD.H264"
        # 예: ".FHD.H264." -> "FHD.H264"
        formatted_str = ' '.join(formatted_str.split())
        return formatted_str.strip(' ._-')


    @classmethod
    def assemble_filename(cls, config, info):
        """주어진 config와 info를 바탕으로 최종 파일명을 조립합니다."""

        # 1. 파일명 변경 옵션 자체가 꺼져있으면 즉시 원본명 반환
        if not config.get('파일명변경', True):
            return info['original_file'].name

        original_filename_stem = info['original_file'].stem
        ext_config = config.get('미디어정보설정', {})
        use_media_info = config.get('파일명에미디어정보포함', False)

        # --- 2. 이미 처리된 모든 파일(단일/분할)에 대한 재처리 방지 로직 ---
        processed_pattern = config.get('이미처리된파일명패턴')
        is_already_processed = processed_pattern and re.match(processed_pattern, original_filename_stem)

        if is_already_processed:
            logger.debug(f"이미 처리된 파일 형식입니다: {info['original_file'].name}")

            if use_media_info and ext_config.get('enable_reprocessing', True):
                skip_pattern = ext_config.get('reprocess_skip_pattern')
                if skip_pattern and re.search(skip_pattern, original_filename_stem, re.IGNORECASE):
                    logger.debug(" -> 미디어 정보가 이미 포함되어 있어 건너뜁니다.")
                    return info['original_file'].name

                media_info_str = ""
                media_info_to_use = info.get('final_media_info')
                if media_info_to_use and media_info_to_use.get('is_valid', True):
                    template = ext_config.get('media_info_template', '')
                    media_info_str = cls._format_conditional_template(template, media_info_to_use)

                if media_info_str:
                    insert_pattern = ext_config.get('reprocess_insert_pattern')
                    if insert_pattern:
                        match = re.match(insert_pattern, original_filename_stem)
                        if match:
                            logger.debug(" -> 기존 파일명에 미디어 정보를 동적 삽입합니다.")
                            base, prefix, suffix = match.groups()
                            final_base = f"{base}{prefix}{media_info_str} {suffix.lstrip()}"
                            return f"{final_base}{info['ext']}"

            return info['original_file'].name

        # --- 3. "새로운 파일" (신규 단일/분할)에 대해서만 이름 생성 로직 실행 ---
        media_info_str = ""
        if use_media_info:
            media_info_to_use = info.get('final_media_info')
            if media_info_to_use and media_info_to_use.get('is_valid', True):
                template = ext_config.get('media_info_template', '')
                media_info_str = cls._format_conditional_template(template, media_info_to_use)

        base = info['pure_code']
        original_part_str = ""
        if config.get('원본파일명포함여부', True):
            option = config.get('원본파일명처리옵션', 'original')
            is_part_set = info.get('is_part_of_set')

            ori_name_raw = ""
            if is_part_set:
                # 신규 분할 파일: prefix와 suffix를 합쳐 원본명 대표 생성
                ori_name_raw = f"{info.get('part_set_prefix', '')} {info.get('part_set_suffix', '')}"
                file_size = info.get('part_set_total_size')
            else:
                # 신규 단일 파일
                ori_name_raw = info['original_file'].stem
                file_size = info.get('file_size')

            # ori_name 정리(cleanup) 로직
            cleaned_ori_name = ori_name_raw.replace("[", "(").replace("]", ")")
            cleaned_ori_name = re.sub(r'--+', '-', cleaned_ori_name)
            cleaned_ori_name = re.sub(r'__+', '_', cleaned_ori_name)
            cleaned_ori_name = re.sub(r'[\(\[\{]\s*[\)\]\}]', '', cleaned_ori_name)
            cleaned_ori_name = re.sub(r'[\s._-]{2,}', ' ', cleaned_ori_name)
            cleaned_ori_name = cleaned_ori_name.strip(' _.,-')

            if option == "original":
                original_part_str = cleaned_ori_name
            elif option == "original_bytes":
                original_part_str = f"{cleaned_ori_name}({file_size or 0})"
            elif option == "original_giga":
                size_gb_str = SupportUtil.sizeof_fmt(file_size or 0, suffix="B")
                size_gb_str = size_gb_str.replace("i", "").replace("Bytes", "").replace("B", "").strip()
                original_part_str = f"{cleaned_ori_name}({size_gb_str}G)"
            elif option == "bytes":
                original_part_str = str(file_size or 0)

        # 최종 조립
        combined_info = ' '.join(filter(None, [media_info_str, original_part_str]))
        final_base = base
        if combined_info:
            final_base += f" [{combined_info}]"

        part = info.get('parsed_part_type', '')
        final_stem = f"{final_base}{part}"

        return f"{final_stem}{info['ext']}"


    ##########################
    # ffprobe & media info
    ##########################

    @classmethod
    def _get_media_info(cls, file_path, ffprobe_config) -> Dict[str, Any] | None:
        """ffprobe를 사용하여 미디어 정보를 추출하고 표준화합니다. (폴백 로직 적용)"""
        ffprobe_bin = ffprobe_config.get('ffprobe_path', '/usr/bin/ffprobe')
        if not os.path.exists(ffprobe_bin):
            logger.warning(f"ffprobe를 찾을 수 없습니다: {ffprobe_bin}")
            return None

        try:
            cmd = [ffprobe_bin, "-v", "quiet", "-show_format", "-show_streams", "-print_format", "json", str(file_path)]
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30)

            if result.returncode != 0:
                logger.error(f"ffprobe 실행 실패: {file_path.name} - {result.stderr.strip()}")
                return {'is_valid': False, 'error': 'ffprobe_execution_failed'}

            data = json.loads(result.stdout)
            video_stream = next((s for s in data.get('streams', []) if s.get('codec_type') == 'video'), None)
            audio_stream = next((s for s in data.get('streams', []) if s.get('codec_type') == 'audio'), None)

            media_info: Dict[str, Any] = {'is_valid': True}

            # 필수 스트림 확인
            if not video_stream or not audio_stream:
                logger.warning(f"{file_path.name}: 유효한 비디오 또는 오디오 스트림을 찾을 수 없습니다.")
                media_info['is_valid'] = False

            width = video_stream.get('width', 0) if video_stream else 0
            height = video_stream.get('height', 0) if video_stream else 0
            v_codec_raw = video_stream.get('codec_name') if video_stream else None
            a_codec_raw = audio_stream.get('codec_name') if audio_stream else None

            if not all([width, height, v_codec_raw, a_codec_raw]):
                media_info['is_valid'] = False

            # --- 프레임레이트 계산 (폴백 적용) ---
            fr_str = '0/0'
            if video_stream:
                fr_str = video_stream.get('avg_frame_rate') or video_stream.get('r_frame_rate') or '0/0'
                if fr_str == '0/0':
                    logger.debug(f"{file_path.name}: 유효한 프레임레이트 정보를 찾을 수 없습니다.")

            num_str, den_str = fr_str.split('/')

            # 분자/분모가 0이거나 정수가 아닌 경우 대비
            try:
                num, den = float(num_str), float(den_str)
                raw_fps = num / den if den != 0 else 0.0
            except ValueError:
                raw_fps = 0.0

            # 표준 프레임레이트 값으로 보정
            fps_tolerance = ffprobe_config.get('tolerance', {}).get('fps', 0.01)
            standard_fps_list = ffprobe_config.get('standard_fps_values', [])
            standardized_fps = raw_fps
            for std_fps in standard_fps_list:
                if abs(raw_fps - std_fps) <= fps_tolerance:
                    standardized_fps = std_fps
                    break

            # 소수점 3자리까지 반올림
            final_fps_float = round(standardized_fps, 3)

            # 문자열로 변환 시 .0 제거
            final_fps_str = f"{final_fps_float}".rstrip('0').rstrip('.')

            # --- 오디오 비트레이트 계산 (폴백 적용) ---
            a_bitrate_kbps = 0
            if audio_stream:
                # 1순위: bit_rate 필드
                raw_bitrate = audio_stream.get('bit_rate')
                # 2순위: bit_rate가 없으면 tags에서 'BPS'로 시작하는 키 검색
                if not raw_bitrate:
                    tags = audio_stream.get('tags', {})
                    for key, value in tags.items():
                        if key.upper().startswith('BPS'):
                            raw_bitrate = value
                            logger.debug(f"  - Audio: bit_rate 폴백 사용: tags['{key}'] = {value}")
                            break
                a_bitrate_kbps = round(float(raw_bitrate or 0) / 1000)

            # 해상도 태그(res_tag) 계산
            res_tag_raw = ""
            if height > 0:
                res_tiers = ffprobe_config.get('resolution_tiers', [])
                for tier in res_tiers:
                    if tier['min_height'] <= height < tier['max_height']:
                        res_tag_raw = tier['tag']
                        break

            # tag_title 추출 및 정제
            tag_title = data.get('format', {}).get('tags', {}).get('title', '').strip()
            if tag_title:
                tag_title = re.sub(r'^(.*\.(cc|club|cn|com|download|me|net|org|pro|tv|vip|xyz)).*$', r'\1', tag_title)
                tag_title = tag_title.replace('https://', '').replace('/', '_').replace('\\', '_').replace(' ', '_')

            # tag_title을 제외한 주요 미디어 정보를 대문자로 변환
            v_codec = v_codec_raw.upper() if v_codec_raw else None
            a_codec = a_codec_raw.upper() if a_codec_raw else None
            res_tag = res_tag_raw.upper() if res_tag_raw else None

            # 최종 미디어 정보 딕셔너리 구성
            media_info.update({
                'width': width,
                'height': height,
                'res_tag': res_tag,
                'v_codec': v_codec,
                'a_codec': a_codec,
                'a_bitrate': a_bitrate_kbps,
                'fps_float': final_fps_float,
                'fps': final_fps_float,
                'tag_title': tag_title,
            })
            return media_info

        except Exception as e:
            logger.error(f"미디어 정보 추출 중 예외 발생: {file_path.name} - {e}")
            logger.error(traceback.format_exc())
            return {'is_valid': False, 'error': str(e)}


class UtilFunc:

    @staticmethod
    def is_duplicate(src: Path, dst: Path, config: dict) -> bool:
        """설정에 따라 파일의 중복 여부를 결정합니다."""

        if not dst.parent.is_dir():
            return False

        method = config.get('중복체크방식', 'flexible')

        # --- 1. 가장 엄격한 방식: 파일명과 크기가 모두 동일해야 함 ---
        if method == "strict":
            if dst.exists():
                return src.stat().st_size == dst.stat().st_size
            return False

        # --- 2. 파일명만으로 체크 ---
        elif method == "filename_only":
            return dst.exists()

        # --- 3. 유연한 방식 (기본값): 파일명이 같거나, 또는 크기가 같으면 중복 ---
        else: # "flexible"
            if dst.exists():
                return True
            # 대상 폴더 파일들의 크기 집합을 미리 생성
            bytes_in_dst = {f.stat().st_size for f in dst.parent.iterdir() if f.is_file()}
            return src.stat().st_size in bytes_in_dst


    @staticmethod
    def move(src: Path, trg: Path):
        if trg.exists():
            trg = trg.with_name(f"[{int(time.time())}] {trg.name}")
        shutil.move(src, trg)
        logger.debug("Moved: %s -> %s", src.name, trg)
