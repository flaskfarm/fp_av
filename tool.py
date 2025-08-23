import os
import re
import shutil
import time
import traceback
from os import PathLike
from pathlib import Path
from typing import Generator

from .setup import P, logger

# EXTENSION = "mp4|avi|mkv|ts|wmv|m2ts|smi|srt|ass|m4v|flv|asf|mpg|ogm"

VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".ts", ".wmv", ".m2ts", "mts", ".m4v", ".flv", ".asf", ".mpg", ".ogm"}
SUBTITLE_EXTS = {".srt", ".smi", ".ass", ".ssa", "idx", "sub", ".sup", ".ttml", ".vtt"}

class ToolExpandFileProcess:

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
        return path.stat().st_size >= min_size * 1024**2 or path.suffix.lower() in SUBTITLE_EXTS


    ##########################
    # preprocess_listdir
    ##########################
    @classmethod
    def preprocess_listdir(cls, source, errpath, config, min_size: int = 0, disallowed_keys=None):
        source = Path(source)
        if not source.is_dir():
            return

        if disallowed_keys is None:
            disallowed_keys = []

        min_size = config.get('최소크기', 0)
        disallowed_keys = config.get('파일처리하지않을파일명', [])

        files = []
        for file in cls._iterdir(source, min_size=min_size):
            if file.suffix.lower() in SUBTITLE_EXTS:
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
                newdir.mkdir(exist_ok=True)
                newfile = newdir.joinpath(file.name)
                shutil.move(file, newfile)

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
                label_part, number_part = parsed_code

                # Censored 모드일 때만 숫자 부분에 패딩(zfill) 적용
                if mode == 'censored' and number_part.isdigit():
                    number_part = str(int(number_part)).zfill(3)

                code_part = f"{label_part}-{number_part}" if number_part else label_part

                return {
                    'code': code_part,
                    'label': label_part,
                    'number': number_part,
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
        """[헬퍼 1/5] 파일명(base)에서 일반적인 노이즈를 제거합니다."""
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

        logger.debug(f"- 전처리 후 base: {base}")
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

                    logger.debug(f"  - Parsed: {base} > label='{label_part}', num='{num_part}', part='{remaining_part}'")
                    return (label_part.lower(), num_part), remaining_part

            except (IndexError, re.error) as e:
                logger.error(f"    - 규칙 {i+1} 적용 중 예외 발생: {e} - '{line}'")

        return None, "" # 실패 시 (None, "") 반환


    @classmethod
    def _parse_part_number(cls, remaining_part):
        """[헬퍼 4/5] 나머지 문자열에서 파트 넘버를 해석하여 'cdN' 형태로 반환합니다."""
        if not remaining_part:
            return ""

        part_str = remaining_part.strip(' ._-')

        cd_match = re.search(r'\b(cd[1-8])$', remaining_part, re.I)
        if cd_match:
            # 'cdN' 패턴이 명확하게 있으면, 그것을 최우선으로 반환
            return cd_match.group(1).lower()

        # 패턴: (cd 또는 part)(숫자1-8) 또는 (단독 문자 a-h)
        # 문자열 '전체'가 이 패턴과 일치해야 유효한 파트 넘버로 간주
        part_pattern = r"^(?:cd|part)?(?P<part_no>[1-8])$|^[a-h]$"
        match = re.match(part_pattern, part_str, re.I)

        if match:
            if match.group("part_no"):
                return f"cd{match.group('part_no')}"
            return f"cd{ord(part_str.lower()) - ord('a') + 1}"

        # 유효한 파트 넘버 패턴이 아니면 빈 문자열 반환
        return ""


    @classmethod
    def _apply_fallback_rules(cls, base):
        """[헬퍼 5/5] 최종 폴백으로 원본 파일명에서 (label, number) 튜플을 찾습니다."""
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


class UtilFunc:

    def is_duplicate(src: Path, dst: Path) -> bool:
        """이름 뿐만 아니라 용량도 확인하여 중복 결정"""
        if dst.exists():
            return True
        bytes_in_dst = [f.stat().st_size for f in dst.parent.iterdir() if f.is_file()]
        return src.stat().st_size in bytes_in_dst

    def move(src: Path, trg: Path):
        if trg.exists():
            trg = trg.with_name(f"[{int(time.time())}] {trg.name}")
        shutil.move(src, trg)
        logger.debug("Moved: %s -> %s", src.name, trg)

