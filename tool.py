import os
import re
import shutil
import time
import traceback
from os import PathLike
from pathlib import Path
from typing import Generator

from .setup import P, logger

EXTENSION = "mp4|avi|mkv|ts|wmv|m2ts|smi|srt|ass|m4v|flv|asf|mpg|ogm"

class ToolExpandFileProcess:
    sub_exts = [".srt", ".sup", ".smi", ".ass", ".ssa", ".vtt"]
    
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
        return path.stat().st_size >= min_size * 1024**2 or path.suffix.lower() in cls.sub_exts

    ##########################
    # preprocess_listdir
    ##########################
    @classmethod
    def preprocess_listdir(cls, source, errpath, config, min_size: int = 0, disallowed_keys=None):
        source = Path(source)
        if not source.is_dir():
            return
        #errpath = Path(errpath)
        #if not errpath.is_dir():
        #    return
        if disallowed_keys is None:
            disallowed_keys = []

        min_size = config.get('최소크기', 0)
        disallowed_keys = config.get('파일처리하지않을파일명', [])

        special_parser_rules = config.get('레이블특수처리규칙')
        cleanup_list = config.get('파일명정리목록')

        files = []
        for file in cls._iterdir(source, min_size=min_size):
            if file.suffix.lower() in cls.sub_exts:
                files.append(file)
                continue
            newdir = None
            if len(file.name) == 40 and file.name.isalnum():
                newdir = errpath.joinpath("[FILENAME HASHED]")
            elif len(file.name.encode("utf-8")) > 200:
                newdir = errpath.joinpath("[FILENAME TOO LONG]")
            elif any(p in file.name.lower() for p in map(str.lower, disallowed_keys)):
                newdir = errpath.joinpath("[FILENAME NOT ALLOWED]")
            elif cls.change_filename_censored(file.name, special_parser_rules, cleanup_list) is None:
                newdir = errpath.joinpath("[FILENAME CANT CHANGE]")
            else:
                #try:
                #    from .secret import Secret
                #    error_type = Secret.run_ffprobe(file)
                #    if error_type is not None:
                #        newdir = errpath.joinpath(error_type)
                #except ImportError:
                #    pass
                pass

            if newdir is not None:
                newdir.mkdir(exist_ok=True)
                newfile = newdir.joinpath(file.name)
                shutil.move(file, newfile)
            else:
                files.append(file)
        return files


    # --- 처리 메인 함수 ---

    @classmethod
    def change_filename_censored(cls, original_filename, special_parser_rules=None, cleanup_list=None):
        if not original_filename or not isinstance(original_filename, str):
            return None
        logger.debug(f"filename: '{original_filename}'")

        base, ext = os.path.splitext(original_filename)

        try:
            # 전처리
            cleaned_base = cls._preprocess_base(base, cleanup_list=cleanup_list)
            code_part, remaining_part = None, ""
            
            # 품번 추출
            if special_parser_rules:
                code_part, remaining_part = cls._apply_special_parser_rules(cleaned_base, special_parser_rules)

            if not code_part:
                code_part, remaining_part = cls._apply_generic_rules(cleaned_base)

            if not code_part:
                code_part, remaining_part = cls._apply_fallback_rules(original_filename)

            # --- 결과 조합 ---
            if code_part:
                # zfill 처리는 순수한 code_part에만 적용합니다.
                tmps = code_part.split("-")
                if len(tmps) > 1 and tmps[1].isdigit():
                    code_part = f"{tmps[0]}-{str(int(tmps[1])).zfill(3)}"

                return {
                    'code': code_part.lower(),
                    'part': remaining_part,
                    'ext': ext
                }

        except Exception as exception:
            logger.error(f"파일명 파싱 중 치명적인 오류 발생: {original_filename}")
            logger.error(f"오류: {exception}")
            logger.error(traceback.format_exc())
            raise exception

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

        # 숫자 접두사, DMM 접두사 제거
        base = re.sub(r'^\d{3,}[-_]', '', base)
        base = re.sub(r'^[hn]_\d', '', base, flags=re.I)

        # 화질/코덱 등 '명백한' 접미사 제거
        # -c, -5 등 모호한 패턴은 여기서 처리하지 않음
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
    def _apply_special_parser_rules(cls, base, special_parser_rules):
        """[헬퍼 2/5] 메타데이터의 특수 파서 규칙을 적용합니다."""

        # custom_rules
        if special_parser_rules and (custom_rules := special_parser_rules.get('custom_rules')):
            for line in custom_rules:
                line = line.strip()
                if not line or line.startswith('#'): continue

                parts = line.split('=>')
                if len(parts) != 2: continue

                pattern, template = parts[0].strip(), parts[1].strip()
                try:
                    match = re.match(pattern, base, re.I)
                    if match:
                        # 템플릿을 사용하여 최종 품번 생성
                        template_parts = template.split('|')
                        if len(template_parts) == 2: # label|number 형식
                            label_template, num_template = template_parts
                            groups = match.groups()
                            label_part = label_template.format(*groups)
                            number_part = num_template.format(*groups)
                            result_code = f"{label_part}-{number_part}"
                        else: # 단일 템플릿 형식
                            result_code = template.format(*match.groups())

                        # 나머지 부분(part) 추출
                        matched_string = match.group(0)
                        start_index = base.find(matched_string)
                        remaining_part = base[start_index + len(matched_string):]

                        logger.debug(f"  - 레이블 특수처리 성공: {line} -> code='{result_code.lower()}', part='{remaining_part}'")
                        return result_code.lower(), remaining_part
                except (IndexError, re.error) as e:
                    logger.error(f"Custom Rule 적용 오류: {line} - {e}")

        # logger.debug("  - Special Parser 규칙 매칭 실패")
        return None, ""


    @classmethod
    def _apply_generic_rules(cls, base):
        """[헬퍼 3/5] 범용 규칙을 적용하여 품번과 파트 넘버를 분리합니다."""
        # logger.debug(f"[일반 규칙 시도] 입력 base: '{base}'")

        # 범용 규칙
        match = re.match(r"^(?P<code>[a-z]+\d*)[-_]?(?P<no>\d+)(?P<part>.*)", base, re.I)
        if match:
            code = f"{match.group('code')}-{match.group('no')}"
            part = match.group('part')
            logger.debug(f"  - 범용 규칙 매칭 성공: code='{code}', part='{part}'")
            return code, part

        # logger.debug("  - 일반 규칙 매칭 실패")
        return None, ""


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
    def _apply_fallback_rules(cls, original_filename):
        """[헬퍼 5/5] 최종 폴백으로 원본 파일명에서 품번을 찾습니다."""

        base, _ = os.path.splitext(original_filename.lower())

        match = re.search(r"\b(?P<code>[a-z]+\d*[-_]?\d+)(?P<part>.*)", base, re.I)
        if match:
            code = match.group('code').replace('_', '-')
            part = match.group('part')
            logger.debug(f"  - 폴백 규칙 매칭 성공: code='{code}', part='{part}'")
            return code, part

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