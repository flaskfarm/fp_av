from .util_runtime import cooperate, cached_media_info, pause
import os
import re
import shutil
import time
import traceback
import string
import subprocess
import json
import sys

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


class SidecarBundle:
    """Stage newly generated sidecars; never copy existing source files."""
    def __init__(self, target, overwrite=False):
        import tempfile
        self.target = Path(target)
        self.target.mkdir(parents=True, exist_ok=True)
        self.folder = Path(tempfile.mkdtemp(prefix='.fp_av_prepare_', dir=self.target))
        self.overwrite = overwrite
        self.published = []
        self.backups = []
        self.sources = []
        self.reserved = set()
        self.ready = {}
        self.committed = False
        self.cleanup_pending = False

    def _reserve_name(self, name):
        if (Path(name).name != name or name.casefold() in self.reserved
                or os.path.lexists(self.folder / name)):
            raise FileExistsError('준비 경로가 이미 예약되어 있습니다: ' + name)
        self.reserved.add(name.casefold())

    def write_json(self, data, name, source):
        stat = Path(source).stat()
        proof = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
        self._reserve_name(name)
        with open(self.folder / name, 'x', encoding='utf-8') as fp:
            json.dump(data, fp, ensure_ascii=False, indent=4)
        self.sources.append((Path(source), proof, name))

    def publish(self):
        for staged in list(self.folder.iterdir()):
            cooperate()
            if not staged.is_file():
                continue
            target = self.target / staged.name
            if os.path.lexists(target):
                if not self.overwrite:
                    # The configured policy is to reuse existing metadata/media.
                    import filecmp
                    if filecmp.cmp(staged, target, shallow=False):
                        stat = target.stat()
                        self.ready[staged.name] = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
                    staged.unlink()
                    logger.info('기존 부가파일 재사용 (덮어쓰기 비활성화): %s', target)
                    continue
                backup = self.folder / ('.backup_' + staged.name)
                UtilFunc.move(target, backup)
                self.backups.append((target, backup))
            UtilFunc.move(staged, target)
            stat = target.stat()
            self.published.append((target, (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)))
            self.ready[staged.name] = self.published[-1][1]

    def commit(self):
        self.committed = True
        for source, proof, name in self.sources:
            if name not in self.ready:
                continue
            try:
                target = self.target / name
                current = target.stat()
                if self.ready[name] != (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns):
                    raise OSError('저장된 부가파일이 변경되었습니다.')
                if source.exists() and source.resolve() != target.resolve():
                    stat = source.stat()
                    if proof != (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns):
                        raise OSError('원본 JSON이 변경되었습니다.')
                    source.unlink()
            except OSError:
                self.cleanup_pending = True
                logger.warning('영상 이동 완료 후 원본 JSON 정리를 보류합니다: %s', source.name)

    def close(self):
        preserve = self.cleanup_pending
        if not self.committed:
            for target, owned in reversed(self.published):
                try:
                    if target.exists():
                        stat = target.stat()
                        if (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns) != owned:
                            raise FileExistsError('생성된 부가파일이 다른 작업에 의해 변경되었습니다.')
                        target.unlink()
                except OSError:
                    preserve = True
                    logger.warning('부가파일 정리를 보류합니다: %s', target.name)
            for target, backup in reversed(self.backups):
                try:
                    UtilFunc.move(backup, target)
                except OSError:
                    preserve = True
                    logger.warning('기존 부가파일 복원을 보류합니다: %s', target.name)
        if not preserve:
            shutil.rmtree(self.folder)

class ToolExpandFileProcess:
    ##########################
    # preprocess_cleanup
    ##########################
    @classmethod
    def preprocess_cleanup(cls, path, min_size: int = 0, max_age: int = 0, subtitle_exts: set = None, dry_run=False):
        """max_age in sec"""
        path = Path(path)
        if not path.is_dir():
            return

        subtitle_exts = subtitle_exts or set()
        
        # 1. 현재 폴더 내의 "유효한(보존 대상) 본편 미디어/자막"의 이름(stem)을 수집
        valid_stems = set()
        for f in path.iterdir():
            cooperate()
            if f.is_file():
                if f.stat().st_size >= min_size * 1024**2 or f.suffix.lower() in subtitle_exts:
                    valid_stems.add(f.stem.lower())
                    
        logger.info(f"전처리: 비어있거나 유효한 파일이 없는 오래된 폴더/파일을 삭제합니다.")
        
        # 2. 파일 및 폴더 삭제 로직
        for child in path.iterdir():
            cooperate()
            try:
                if child.stat().st_mtime < time.time() - max_age:
                    # old enough
                    if child.is_dir():
                        if not list(cls._iterdir(child, min_size=min_size, subtitle_exts=subtitle_exts)):
                            logger.debug(f"폴더 삭제: {child}")
                            if not dry_run:
                                shutil.rmtree(child)
                    elif child.is_file():
                        # 본편 영상이나 자막은 유지
                        if child.stat().st_size >= min_size * 1024**2 or child.suffix.lower() in subtitle_exts:
                            continue
                            
                        # 본편보다 작은 파일 중, 본편 영상과 이름(stem)이 완벽히 동일한 메타 파일만 제한적 보호
                        protected_exts = {'.json', '.txt', '.yaml', '.nfo'}
                        if child.suffix.lower() in protected_exts:
                            if child.stem.lower() in valid_stems:
                                continue
                                
                        # 짝꿍 영상이 없는 고아(Orphan) 파일이거나 보호대상이 아닌 파일(이미지, 광고 등)이면 삭제
                        logger.debug(f"파일 삭제 (찌꺼기/고아 파일): {child}")
                        if not dry_run:
                            child.unlink()
            except Exception as e:
                logger.warning(f"전처리 중 '{child}' 처리 실패: {e}")

    @classmethod
    def _iterdir(cls, path, min_size: int = 0, subtitle_exts: set = None) -> Generator[Path, None, None]:
        """generate a list of file with specific conditions"""
        path = Path(path)
        if not path.is_dir():
            return

        subtitle_exts = subtitle_exts or set()
        for file in path.rglob("*"):
            cooperate()
            if not cls._is_legit_file(file, subtitle_exts=subtitle_exts):
                continue
            if file.is_dir():
                continue
            if cls._is_legit_file(file, min_size=min_size, subtitle_exts=subtitle_exts):
                yield file


    @classmethod
    def _is_legit_file(cls, path: PathLike, min_size: int = 0, subtitle_exts: set = None):
        if not path.is_file():
            return False
        subtitle_exts = subtitle_exts or set()
        return path.stat().st_size >= min_size * 1024**2 or path.suffix.lower() in subtitle_exts


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
        subtitle_exts = config.get('subtitle_exts', set()) 

        # --- 예외 키워드/정규식 파싱 로직 ---
        raw_disallowed_setting = config.get('파일처리하지않을파일명', "")
        
        # YAML에서 리스트 형태로 들어왔을 경우를 대비
        if isinstance(raw_disallowed_setting, list):
            lines = raw_disallowed_setting
        else:
            lines = str(raw_disallowed_setting).splitlines()

        normal_keys = []
        regex_patterns = []

        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            if line.lower().startswith('regex||'):
                # 정규식 모드: 파이프(|)로 자르지 않고 전체를 정규식으로 취급
                pattern_str = line[7:].strip()
                try:
                    regex_patterns.append(re.compile(pattern_str, re.IGNORECASE))
                except re.error as e:
                    logger.error(f"예외 키워드 정규식 파싱 오류 '{pattern_str}': {e}")
            else:
                # 일반 모드: 기존처럼 파이프(|) 구분 지원
                for k in line.split('|'):
                    k = k.strip()
                    if k:
                        normal_keys.append(k.lower())

        files = []
        for file in cls._iterdir(source, min_size=min_size, subtitle_exts=subtitle_exts):
            cooperate()
            if file.suffix.lower() in subtitle_exts:
                files.append(file)
                continue

            newdir = None
            if len(file.name) == 40 and file.name.isalnum():
                newdir = errpath.joinpath("[FILENAME HASHED]") if errpath else None
            elif len(file.name.encode("utf-8")) > 200:
                newdir = errpath.joinpath("[FILENAME TOO LONG]") if errpath else None
            else:
                is_not_allowed = False
                filename_lower = file.name.lower()
                
                # 1. 일반 문자열 포함 여부 검사
                if any(nk in filename_lower for nk in normal_keys):
                    is_not_allowed = True
                # 2. 정규식 매칭 검사
                elif any(rp.search(file.name) for rp in regex_patterns):
                    is_not_allowed = True

                if is_not_allowed:
                    newdir = errpath.joinpath("[FILENAME NOT ALLOWED]") if errpath else None
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
                    UtilFunc.move(file, newfile)
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
            cleaned_base = cls._preprocess_base(base, cleanup_list=cleanup_list)
            parsed_code, remaining_part = None, ""
            search_label = None

            if parsing_rules:
                special_key = f"{mode}_special_rules"
                special_rules = parsing_rules.get(special_key, [])
                generic_rules = parsing_rules.get('generic_rules', [])
                all_rules = special_rules + generic_rules
                if all_rules:
                    parsed_code, remaining_part, search_label = cls._apply_parsing_rules(cleaned_base, all_rules)

            if not parsed_code:
                parsed_code, remaining_part = cls._apply_fallback_rules(cleaned_base)
                search_label = None

            if parsed_code:
                label_part, number_part_raw = parsed_code

                if mode == 'censored' and number_part_raw.isdigit():
                    number_part_processed = str(int(number_part_raw)).zfill(3)
                else:
                    number_part_processed = number_part_raw

                code_part = f"{label_part}-{number_part_processed}" if number_part_processed else label_part

                base_search_label = search_label if search_label else label_part
                
                search_keyword = None
                if base_search_label:
                    search_keyword = f"{base_search_label}-{number_part_raw}" if number_part_raw else base_search_label
                
                label_part = re.sub(r'^0+', '', label_part)

                return {
                    'code': code_part,
                    'label': label_part,
                    'number': number_part_processed,
                    'raw_number': number_part_raw,
                    'part': remaining_part,
                    'ext': ext,
                    'search_keyword': search_keyword,
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
        base = base.lower()

        if cleanup_list:
            sorted_list = sorted([re.escape(kw.strip()) for kw in cleanup_list if kw.strip()], key=len, reverse=True)
            if sorted_list:
                pattern = '|'.join(sorted_list)
                base = re.sub(r'[-@_. ]?(' + pattern + r')[-@_. ]?', ' ', base, flags=re.I)

        base = re.sub(r'\(\d{6,}\)', ' ', base)
        base = re.sub(r'[\[\]\(\)\{\}]+', ' ', base)
        base = re.sub(r'^[hn]_\d', '', base, flags=re.I)

        tlds = 'cc|cn|com|net|me|org|xyz|vip|tv|la|info|link|online|site|top|io|gg'
        base = re.sub(r'[-@_. ]?[a-z0-9]+\.(%s)[-@_. ]?' % tlds, ' ', base).strip()

        misc_pattern = r'(?:hd)?720p|(?:fhd)?1080p|2160p|2k|4k|6k|8k|fhd|uhd|'
        misc_pattern += r'h264|h265|hevc|x265|mpeg|wmv[0-9]?|rv(?:[0-9]{,2})?|'
        misc_pattern += r'aac|dts|mp3|ogg|flac|wav|wma(?:pro|v\d)?|pcm(?:_s16le)?|ac3|eac3|vorbis|opus|'
        misc_pattern += r'\d+[.0-9]*fps|\d+kbps|'
        misc_pattern += r'\d{3,}x\d{3,}|\dk[0-9.]+fps'
        base = re.sub(r'[\-@_,. \[\{\(](%s)[\-@_,. \]\}\)]' % misc_pattern, ' ', base).strip()

        base = re.sub(r'[\-@_,. \[\{\(](%s)[\-@_,. \]\}\)0-9](part|pt|cd)[-@_. ]?\d{,2}$', '', base)
        base = re.sub(r'[rsz]$', '', base)
        
        base = re.sub(r'\s+', ' ', base).strip(' ._-')

        return base


    @classmethod
    def _apply_parsing_rules(cls, base, rules_list):
        """주어진 규칙 리스트를 순서대로 적용하여 품번을 파싱합니다."""
        # logger.debug(f"  - 파싱 규칙 적용 시작. 총 {len(rules_list)}개 패턴, 대상: '{base}'")

        for i, line in enumerate(rules_list):
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            parts = line.split('=>')
            if len(parts) != 2:
                logger.warning(f"    - 패턴 {i+1}: 형식 오류 (건너뜀) - '{line}'")
                continue

            pattern, template = parts[0].strip(), parts[1].strip()
            # logger.debug(f"    - 패턴 {i+1} 시도: 패턴='{pattern}'")

            try:
                match = re.match(pattern, base, re.I)
                if match:
                    # logger.debug(f"      - 매칭 성공! 그룹: {match.groups()}")
                    groups = tuple("" if g is None else g for g in match.groups())
                    label_part, num_part = "", ""
                    search_label = None

                    template_parts = [t.strip() for t in template.split('|')]

                    if len(template_parts) >= 2: # label|number 형식이거나 label|number|... 형식
                        label_template, num_template = template_parts[0], template_parts[1]
                        try:
                            label_part = label_template.format(*groups)
                            num_part = num_template.format(*groups)
                            
                            if len(template_parts) >= 3:
                                search_label_template = template_parts[2]
                                search_label = search_label_template.format(*groups)
                        except IndexError as e:
                            logger.error(f"        - 템플릿 적용 오류 (그룹 인덱스): {e}...")
                            continue
                    elif len(template_parts) == 1: # 단일 템플릿 형식
                        try:
                            label_part = template_parts[0].format(*groups)
                        except IndexError as e:
                            logger.error(f"        - 템플릿 적용 오류 (그룹 인덱스): {e}...")
                            continue

                    matched_string = match.group(0)
                    remaining_part = base[base.find(matched_string) + len(matched_string):]

                    logger.debug(f"  - Pre-Parsed: {base} > label='{label_part}', num='{num_part}', search_label='{search_label}'")
                    return (label_part.lower(), num_part), remaining_part, search_label

            except (IndexError, re.error) as e:
                logger.error(f"    - 패턴 {i+1} 적용 중 예외 발생: {e} - '{line}'")

        return None, "", None


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

            logger.debug(f"  - 폴백 패턴 매칭 성공: label='{label_part}', number='{number_part}', part='{part_str}'")
            return (label_part, number_part), part_str

        logger.debug("  - 폴백 패턴 매칭 실패")
        return None, ""


    @classmethod
    def pascal_case_preserve(cls, text):
        """
        하이픈(-)과 어포스트로피(')를 기준으로 단어를 쪼갠 뒤,
        각 단어의 첫 글자를 대문자로 만들고(나머지는 보존) 다시 합칩니다.
        """
        if not text:
            return ""
        words = re.split(r'[\'-]', text)
        result_words = []
        for w in words:
            if not w: continue
            result_words.append(w[0].upper() + w[1:])
        
        return "".join(result_words)


    @classmethod
    def init_western_info(cls, original_filename, cleanup_pattern="", featurette_pattern=""):
        if not original_filename or not isinstance(original_filename, str):
            return None

        base, ext = os.path.splitext(original_filename)
        clean_base = base

        # 1. 파일명에서 화질, 불필요한 태그 등 정리
        if cleanup_pattern:
            try:
                clean_base = re.sub(cleanup_pattern, '', clean_base, flags=re.IGNORECASE).strip()
            except re.error as e:
                logger.error(f"Western 검색 키워드 정제 정규식 오류: {e}")
        else:
            clean_base = re.sub(r'\[.*?\]$', '', clean_base).strip()

        # 2. 부가 영상(Featurette) 판별
        is_featurette = False
        if featurette_pattern:
            try:
                f_match = re.search(featurette_pattern, clean_base, re.IGNORECASE)
                if f_match:
                    is_featurette = True
                    clean_base = clean_base[:f_match.start()]
            except re.error as e:
                logger.error(f"Western 부가 영상 인식 정규식 오류: {e}")

        # 3. 스튜디오 추출 로직
        raw_studio = clean_base
        
        # A) 날짜 패턴 (YY.MM.DD, YYYY-MM-DD 등)을 찾아 그 앞부분을 스튜디오로 간주
        date_match = re.search(r'\b(\d{2,4}[-.\s_]\d{2}[-.\s_]\d{2})\b', clean_base)
        if date_match:
            raw_studio = clean_base[:date_match.start()]
            
        # B) 날짜가 없을 경우, ' - ' (공백 하이픈 공백) 기준 분리
        elif ' - ' in clean_base:
            raw_studio = clean_base.split(' - ')[0]
            
        # C) 최후의 수단: 첫 번째 기호(점, 띄어쓰기, 언더바, 하이픈)까지만 분리하여 스튜디오로 간주
        else:
            split_match = re.split(r'[\.\s_-]+', clean_base, 1)
            raw_studio = split_match[0] if split_match else clean_base

        # 괄호 및 양끝 찌꺼기 기호 제거
        raw_studio = re.sub(r'[\(\)\[\]\{\}]', '', raw_studio)
        raw_studio = raw_studio.strip(' -._')

        # D) 하이픈, 언더바, 점을 공백으로 변환 후 각 단어 첫 글자만 대문자로 (Title Case 적용)
        # O'Brien 같이 어포스트로피가 들어간 이름은 원형을 보존하도록 처리
        def format_studio_name(text):
            text = text.replace('-', ' ').replace('_', ' ').replace('.', ' ')
            words = text.split()
            result_words = []
            for w in words:
                if not w: continue
                sub_words = w.split("'")
                sub_res = [sw[0].upper() + sw[1:] if sw else "" for sw in sub_words]
                result_words.append("'".join(sub_res))
            return " ".join(result_words)

        clean_studio = format_studio_name(raw_studio)
        if not clean_studio:
            clean_studio = "Unknown"

        return {
            'code': base,               
            'pure_code': clean_base,
            'label': clean_studio,           
            'studio': clean_studio,          
            'title': clean_base,
            'number': '',
            'raw_number': '',
            'ext': ext,
            'search_keyword': clean_base,
            'is_featurette': is_featurette,
        }


    @staticmethod
    def get_safe_filename(text):
        if not text:
            return ""
        
        text = text.replace(':', ' -')
        text = text.replace('/', '_')
        text = text.replace('\\', '_')
        text = text.replace('"', "'")
        
        text = re.sub(r'[*?<>|]', '', text)
        
        # 3. 양끝 공백 정리
        return text.strip()

    @staticmethod
    def pair_companion_subtitles(videos, subtitles, config):
        """
        동반 자막 처리 공통 함수
        """
        unmatched_subs = []
        subtitle_exts = config.get('subtitle_exts', set())
        can_detect_korean = config.get('동반자막한국어자막판별', False)
        
        if can_detect_korean:
            UtilFunc._initialize_chardet()

        claimed = {str(sub['original_file'].absolute()) for video in videos
                   for sub in video.get('companion_subs_list', [])}
        for s_info in subtitles:
            cooperate()
            source_key = str(s_info['original_file'].absolute())
            if source_key in claimed:
                continue
            found_pair = False
            
            s_info['is_korean'] = True 
            if can_detect_korean:
                if not UtilFunc.is_korean_subtitle(s_info['original_file'], config):
                    s_info['is_korean'] = False
            
            sub_name = s_info['original_file'].name
            
            for v_info in sorted(videos, key=lambda x: len(x['original_file'].name), reverse=True):
                cooperate()
                video_stem = v_info['original_file'].stem
                
                if sub_name.startswith(video_stem):
                    remainder = sub_name[len(video_stem):].lower()
                    
                    is_valid_tail = False
                    for ext in subtitle_exts:
                        if remainder == ext: 
                            is_valid_tail = True
                            break
                        elif re.match(r'^\.[a-z]{2,3}' + re.escape(ext) + r'$', remainder):
                            is_valid_tail = True
                            break

                    if is_valid_tail:
                        if 'companion_subs_list' not in v_info:
                            v_info['companion_subs_list'] = []
                        
                        s_info['_companion_language'] = sub_name[len(video_stem):-len(s_info['original_file'].suffix)].lstrip('.')
                        if sum(video['original_file'].stem == video_stem for video in videos) > 1:
                            s_info['_subtitle_pairing'] = 'ambiguous'
                            logger.warning('동반자막 영상 매칭이 모호하여 기존 우선순위에 연결하고 원본은 보존합니다: %s', sub_name)
                        v_info['companion_subs_list'].append(s_info)
                        claimed.add(source_key)
                        found_pair = True
                        break
            
            if not found_pair:
                unmatched_subs.append(s_info)
                
        return videos, unmatched_subs


    ##########################
    # advanced naming process
    ##########################

    @staticmethod
    def _format_conditional_template(template_str, data, format_all=False):
        """
        [[...]] 구문을 해석하여 조건부 템플릿을 포맷팅합니다.
        예: "[[{res_tag}]][[{v_codec}]]"
        """
        def repl(match):
            inner_template = match.group(1)
            try:
                keys = re.findall(r'\{(\w+)\}', inner_template)
                if not keys or all(data.get(key) for key in keys):
                    if format_all:
                        # 블록 선택 후 전체를 한 번만 포맷하여 값 안의 중괄호를 재해석하지 않습니다.
                        return inner_template
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
        if format_all:
            formatted_str = SafeFormatter().format(formatted_str, **data)

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
                
                # 1. 스킵 패턴 확인
                skip_pattern = ext_config.get('reprocess_skip_pattern')
                if skip_pattern and re.search(skip_pattern, original_filename_stem, re.IGNORECASE):
                    logger.debug(" -> 스킵 패턴에 일치하여 건너뜁니다.")
                    return info['original_file'].name

                # 2. 미디어 정보 문자열 조립
                media_info_str = ""
                media_info_to_use = info.get('final_media_info')
                if media_info_to_use and media_info_to_use.get('is_valid', True):
                    template = ext_config.get('media_info_template', '')
                    media_info_str = cls._format_conditional_template(template, media_info_to_use)

                # 3. 중복 검사 및 삽입
                if media_info_str:
                    insert_pattern = ext_config.get('reprocess_insert_pattern')
                    if insert_pattern:
                        match = re.match(insert_pattern, original_filename_stem)
                        if match:
                            base, prefix, suffix = match.groups()
                            suffix = suffix.lstrip()

                            if suffix.startswith(media_info_str):
                                return info['original_file'].name
                            
                            # 중복이 아니라고 판단되면 삽입 진행
                            logger.debug(" -> 기존 파일명에 미디어 정보를 동적 삽입합니다.")
                            final_base = f"{base}{prefix}{media_info_str} {suffix}"
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
                raw_prefix = info.get('part_set_prefix', '')
                clean_prefix = re.sub(r'[-_\s]?(cd|part|pt)$', '', raw_prefix, flags=re.I)

                # 신규 분할 파일
                ori_name_raw = f"{clean_prefix}{info.get('part_set_suffix', '')}"
                file_size = info.get('part_set_total_size')
            else:
                # 신규 단일 파일
                ori_name_raw = info['original_file'].stem
                file_size = info.get('file_size')

            # ori_name 정리(cleanup) 로직
            # 1) 대괄호를 소괄호로 변경
            cleaned_ori_name = ori_name_raw.replace("[", "(").replace("]", ")")
            
            # 2) 내용이 없는 빈 괄호 제거
            cleaned_ori_name = re.sub(r'[\(\[\{]\s*[\)\]\}]', '', cleaned_ori_name)
            
            # 3) 연속된 기호/공백을 각각 원본 형식에 맞춰 압축
            cleaned_ori_name = re.sub(r'\s{2,}', ' ', cleaned_ori_name)
            # cleaned_ori_name = re.sub(r'\.{2,}', '.', cleaned_ori_name)
            cleaned_ori_name = re.sub(r'-{2,}', '-', cleaned_ori_name)
            cleaned_ori_name = re.sub(r'_{2,}', '_', cleaned_ori_name)
            
            # 4) 양 끝의 불필요한 기호 제거
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
        final_stem = cls.get_safe_filename(final_stem)

        return f"{final_stem}{info['ext']}"


    ##########################
    # ffprobe & media info
    ##########################

    @classmethod
    @cached_media_info
    def _get_media_info(cls, file_path, ffprobe_config) -> Dict[str, Any] | None:
        """ffprobe를 사용하여 미디어 정보를 추출하고 표준화합니다."""
        ffprobe_bin = ffprobe_config.get('ffprobe_path', '/usr/bin/ffprobe')
        if not os.path.exists(ffprobe_bin):
            logger.warning(f"ffprobe를 찾을 수 없습니다: {ffprobe_bin}")
            return None

        data = None
        max_retries = 2
        retry_delay = 2

        # 네트워크 지연 등을 고려한 재시도 루프
        for attempt in range(max_retries + 1):
            try:
                cmd = [ffprobe_bin, "-v", "quiet", "-show_format", "-show_streams", "-print_format", "json", str(file_path)]
                result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30)

                if result.returncode != 0:
                    if attempt < max_retries:
                        logger.warning(f"ffprobe 실행 실패 (시도 {attempt+1}/{max_retries+1}), {retry_delay}초 후 재시도: {file_path.name}")
                        pause(retry_delay)
                        continue
                    else:
                        logger.error(f"ffprobe 실행 최종 실패: {file_path.name} - {result.stderr.strip()}")
                        return {'is_valid': False, 'error': 'ffprobe_execution_failed'}

                if not result.stdout.strip():
                    if attempt < max_retries:
                        logger.warning(f"ffprobe 빈 응답 수신 (시도 {attempt+1}/{max_retries+1}), {retry_delay}초 후 재시도: {file_path.name}")
                        pause(retry_delay)
                        continue
                    else:
                        logger.error(f"ffprobe 최종 실패: 빈 응답 수신 - {file_path.name}")
                        return {'is_valid': False, 'error': 'ffprobe_empty_response'}

                data = json.loads(result.stdout)
                break  # 성공 시 루프 탈출

            except subprocess.TimeoutExpired:
                if attempt < max_retries:
                    logger.warning(f"ffprobe 타임아웃 (시도 {attempt+1}/{max_retries+1}), {retry_delay}초 후 재시도: {file_path.name}")
                    pause(retry_delay)
                    continue
                else:
                    logger.error(f"ffprobe 타임아웃 최종 실패: {file_path.name}")
                    return {'is_valid': False, 'error': 'ffprobe_timeout'}

            except json.JSONDecodeError as e:
                if attempt < max_retries:
                    logger.warning(f"ffprobe 결과 JSON 파싱 실패 (시도 {attempt+1}/{max_retries+1}), {retry_delay}초 후 재시도: {file_path.name}")
                    pause(retry_delay)
                    continue
                else:
                    logger.error(f"ffprobe 결과 JSON 파싱 최종 실패: {file_path.name} - {str(e)}")
                    return {'is_valid': False, 'error': 'json_decode_failed'}

            except Exception as e:
                logger.error(f"미디어 정보 추출 중 예외 발생: {file_path.name} - {e}")
                logger.error(traceback.format_exc())
                return {'is_valid': False, 'error': str(e)}

        # 데이터를 정상적으로 획득한 경우에만 아래 로직 실행
        if not data:
            return {'is_valid': False, 'error': 'unknown_error'}

        try:
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
                avg_fr = video_stream.get('avg_frame_rate')
                r_fr = video_stream.get('r_frame_rate')
                
                # avg_frame_rate가 유효하면 우선 사용
                if avg_fr and avg_fr != '0/0':
                    fr_str = avg_fr
                # avg가 없거나 0/0이면 r_frame_rate 사용
                elif r_fr and r_fr != '0/0':
                    fr_str = r_fr
                
                if fr_str == '0/0':
                    logger.debug(f"{file_path.name}: 유효한 프레임레이트 정보를 찾을 수 없습니다.")

            num_str, den_str = fr_str.split('/')

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
            logger.error(f"미디어 정보 후처리 중 예외 발생: {file_path.name} - {e}")
            logger.error(traceback.format_exc())
            return {'is_valid': False, 'error': str(e)}


class UtilFunc:
    _CHARDET_INSTALLED = None
    chardet = None

    @staticmethod
    def _initialize_chardet():
        if UtilFunc._CHARDET_INSTALLED is not None:
            return UtilFunc._CHARDET_INSTALLED

        try:
            import chardet as chardet_lib
            UtilFunc.chardet = chardet_lib
            UtilFunc._CHARDET_INSTALLED = True
            # logger.debug("chardet 라이브러리가 성공적으로 로드되었습니다.")
        except ImportError:
            logger.warning("chardet 라이브러리를 찾을 수 없습니다. 자동 설치를 시도합니다...")
            try:
                subprocess.run(
                    [sys.executable, "-m", "pip", "install", "chardet"],
                    check=True, capture_output=True, text=True
                )
                import chardet as chardet_lib
                UtilFunc.chardet = chardet_lib
                UtilFunc._CHARDET_INSTALLED = True
                logger.info("chardet 라이브러리를 성공적으로 설치하고 로드했습니다.")
            except Exception as e:
                logger.error("chardet 자동 설치에 실패했습니다. 자막 언어 감지 기능이 비활성화됩니다.")
                logger.error(f"오류: {e}")
                if isinstance(e, subprocess.CalledProcessError):
                    logger.error(f"Pip STDERR: {e.stderr}")
                UtilFunc._CHARDET_INSTALLED = False

        return UtilFunc._CHARDET_INSTALLED


    @staticmethod
    def is_korean_subtitle(file_path: Path, config: dict) -> bool:
        """파일 내용을 분석하여 한국어 자막인지 판별합니다."""
        companion_config = config.get('동반자막처리', {})
        if not companion_config.get('한국어자막판별', False):
            return True 

        if not UtilFunc._initialize_chardet():
            return False

        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(2048)
            if not raw_data: return False

            encoding = (UtilFunc.chardet.detect(raw_data)['encoding'] or 'utf-8')
            content = raw_data.decode(encoding, errors='ignore')

            if len(re.findall(r'[가-힣]', content)) > 50:
                logger.debug(f"'{file_path.name}'은(는) 한국어 자막으로 판별되었습니다.")
                return True
        except Exception as e:
            logger.error(f"자막 파일 '{file_path.name}' 분석 중 오류: {e}")
            return False

        logger.debug(f"'{file_path.name}'은(는) 한국어 자막이 아닌 것으로 판별되었습니다.")
        return False


    @staticmethod
    def is_duplicate(src: Path, dst: Path, config: dict) -> bool:
        """설정에 따라 파일의 중복 여부를 결정합니다."""
        if not dst.parent.is_dir():
            return False

        method = config.get('중복체크방식', 'flexible')

        if method == "strict":
            if dst.exists():
                return src.stat().st_size == dst.stat().st_size
            return False

        elif method == "filename_only":
            return dst.exists()

        else: # "flexible"
            if dst.exists():
                return True
            source_size = src.stat().st_size
            for candidate in dst.parent.iterdir():
                cooperate()
                if candidate.is_file() and candidate.stat().st_size == source_size:
                    return True
            return False


    @staticmethod
    def display_relative_path(path, download_roots):
        """Display a source path relative to its configured download folder."""
        path = Path(path)
        roots = [Path(root) for root in download_roots if path.is_relative_to(root)]
        if not roots:
            return path.name
        return path.relative_to(max(roots, key=lambda root: len(root.parts))).as_posix()

    @staticmethod
    def destination_state(src, dst, config=None):
        src, dst = Path(src), Path(dst)
        if src.resolve() == dst.resolve() or (src.exists() and dst.exists() and os.path.samefile(src, dst)):
            return 'same'
        if dst.exists() or dst.is_symlink():
            return 'duplicate' if config is not None and UtilFunc.is_duplicate(src, dst, config) else 'conflict'
        return 'new'

    @staticmethod
    def unique_destination(target, timestamp=None, group_stem=None):
        target = Path(target)
        if not os.path.lexists(target):
            return target
        stamp = int(time.time()) if timestamp is None else timestamp
        sequence = 1
        while True:
            cooperate()
            prefix = str(stamp) if sequence == 1 else f'{stamp}-{sequence}'
            candidate = (target.with_name(f'{group_stem}.duplicate{sequence}{target.name[len(group_stem):]}')
                         if group_stem else target.with_name(f'[{prefix}] {target.name}'))
            if not os.path.lexists(candidate):
                return candidate
            sequence += 1

    @staticmethod
    def move_unique(source, target, timestamp=None, group_stem=None):
        # Retry only name collisions; the actual no-clobber move is authoritative.
        for attempt in range(1000):
            cooperate()
            candidate = UtilFunc.unique_destination(target, timestamp, group_stem)
            try:
                return UtilFunc.move(source, candidate)
            except FileExistsError:
                continue
        raise FileExistsError('고유 이동 경로를 확보하지 못했습니다.')

    @staticmethod
    def move(src: Path, trg: Path):
        """Rename an existing file or directory; never turn a move into a copy."""
        src, trg = Path(src), Path(trg)
        if UtilFunc.destination_state(src, trg) == 'same':
            return trg
        if src.is_dir() and trg.resolve().is_relative_to(src.resolve()):
            raise ValueError('원본 폴더 내부로 이동할 수 없습니다.')
        if os.path.lexists(trg):
            raise FileExistsError(str(trg))
        # The filesystem (including mergerfs) decides how to rename the entry.
        # EXDEV and other rename errors are reported; no copy/delete fallback.
        os.rename(src, trg)
        logger.debug("Moved: %s -> %s", src.name, trg)
        return trg
