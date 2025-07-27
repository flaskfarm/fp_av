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
                if child.is_dir() and not list(cls.__iterdir(child, min_size=min_size)):
                    shutil.rmtree(child)
                elif child.is_file() and not cls.__is_legit_file(child, min_size=min_size):
                    child.unlink()

    @classmethod
    def __iterdir(cls, path, min_size: int = 0) -> Generator[Path, None, None]:
        """generate a list of file with specific conditions

        files-only / recursive / min_size in MiB
        """
        path = Path(path)
        if not path.is_dir():
            return
        for file in path.rglob("*"):
            if not cls.__is_legit_file(file):
                continue
            if file.is_dir():
                continue
            if cls.__is_legit_file(file, min_size=min_size):
                yield file


    @classmethod
    def __is_legit_file(cls, path: PathLike, min_size: int = 0):
        if not path.is_file():
            return False
        return path.stat().st_size >= min_size * 1024**2 or path.suffix.lower() in cls.sub_exts

    ##########################
    # preprocess_listdir
    ##########################
    @classmethod
    def preprocess_listdir(cls, source, errpath, min_size: int = 0, disallowed_keys=None):
        source = Path(source)
        if not source.is_dir():
            return
        #errpath = Path(errpath)
        #if not errpath.is_dir():
        #    return
        if disallowed_keys is None:
            disallowed_keys = []
        files = []
        for file in cls.__iterdir(source, min_size=min_size):
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
            elif cls.change_filename_censored(file.name) is None:
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

    # 이 아래는 거의 그대로
    @classmethod
    def change_filename_censored(cls, filename):
        # 24id
        # 2021-06-30
        tmp = os.path.splitext(filename)
        # 2025.07.14 soju6jan
        #if tmp[-1].lower() in [".srt", ".sup", ".smi", ".ass", ".ssa", ".vtt"]:
        #    return filename.lower()

        match = re.compile(r"\d{2}id", re.I).search(filename.lower())
        id_before = None
        if match:
            id_before = match.group(0)
            filename = filename.lower().replace(id_before, "zzid")

        try:
            filename = cls.change_filename_censored_old(filename)
            if filename is not None:
                if id_before is not None:
                    filename = filename.replace("zzid", id_before)
                base, ext = os.path.splitext(filename)
                tmps = base.split("-")
                tmp2 = tmps[1].split("cd")
                if len(tmp2) == 1:
                    tmp = "%s-%s%s" % (tmps[0], str(int(tmps[1])).zfill(3), ext)
                elif len(tmp2) == 2:
                    tmp = "%s-%scd%s%s" % (tmps[0], str(int(tmp2[0])).zfill(3), tmp2[1], ext)
                return tmp
        except Exception as exception:
            logger.debug("filename : %s", filename)
            logger.error("Exception:%s", exception)
            logger.error(traceback.format_exc())
            return filename

    @classmethod
    def change_filename_censored_old(cls, filename):
        # logger.debug('get_plex_filename:%s', file)
        original_filename = filename
        # return file
        filename = filename.lower()

        # -h264 제거
        filename = filename.replace("-h264", "")
        # filename = filename.replace("-264", "")  # juq-264에서 오류가 나므로 주석처리
        # 2019-10-06 -■-IBW-670Z_1080p.mkv => ibw-6701080 [-■-IBW-670Z_1080p].mkv
        filename = filename.replace("z_1080p", "").replace("z_720p", "")
        filename = filename.replace("z_", "")
        filename = filename.replace("-c.", ".")
        filename = filename.replace("c.", ".")
        # 2021-11-09
        for sp in [".com@", ".com-"]:
            tmp = filename.split(sp)
            # logger.error(tmp)
            if len(tmp) == 2:
                filename = tmp[1]

        # if file.find('@') != -1:
        #    file = file.split('@')[1]

        # 1080p
        regex = r"^(?P<code>.*?)\.1080p\.(?P<ext>%s)$" % EXTENSION
        match = re.compile(regex).match(filename)
        if match:
            filename = "%s.%s" % (match.group("code"), match.group("ext"))
        # fhd
        # regex = r'^(?P<code>.*?)fhd\.(?P<ext>%s)$' % EXTENSION
        # 2019-10-06
        # sdmu-676_FHD.mp4 => sdmu-676cd-1 [sdmu-676_FHD].mp4
        regex = r"^(?P<code>.*?)(\_|\-)fhd\.(?P<ext>%s)$" % EXTENSION
        match = re.compile(regex).match(filename)
        if match:
            filename = "%s.%s" % (match.group("code"), match.group("ext"))

        # [ ]숫자 제거
        regex = r"^\[.*?\]\d+(?P<code>.*?)\.(?P<ext>%s)$"
        match = re.compile(regex).match(filename)
        if match:
            filename = "%s.%s" % (match.group("code"), match.group("ext"))

        # [ ] 제거
        regex = r"^\[.*?\](?P<code>.*?)\.(?P<ext>%s)$" % EXTENSION
        match = re.compile(regex).match(filename)
        if match:
            filename = "%s.%s" % (match.group("code"), match.group("ext"))

        # ( ) 제거
        regex = r"^\(.*?\)(?P<code>.*?)\.(?P<ext>%s)$" % EXTENSION
        match = re.compile(regex).match(filename)
        if match:
            filename = "%s.%s" % (match.group("code"), match.group("ext"))

        # 3,4자리 숫자
        regex = r"^\d{3,4}(?P<code>.*?)\.(?P<ext>%s)$" % EXTENSION
        match = re.compile(regex).match(filename)
        if match:
            filename = "%s.%s" % (match.group("code"), match.group("ext"))

        regex = r"^.*\.com\-?\d*\-?\d*@?(?P<code>.*?)(\-h264)??\.(?P<ext>%s)$" % EXTENSION
        match = re.compile(regex).match(filename)
        if match:
            filename = "%s.%s" % (match.group("code"), match.group("ext"))

        regex = r"^(?P<dummy>.*\.com.*?)(?P<code>[a-z]+)"
        match = re.compile(regex).match(filename)
        if match:
            filename = filename.replace(match.group("dummy"), "")

        # -5 제거
        regex = r"^(?P<code>.*?)\-5.(?P<ext>%s)$" % EXTENSION
        match = re.compile(regex).match(filename)
        if match:
            filename = "%s.%s" % (match.group("code"), match.group("ext"))

        # dhd1080.com@1fset00597hhb.mp4
        # regex = r'^.*?com@(\d)?(?P<code>[a-z]+\d+)\w+.(?P<ext>%s)$' % EXTENSION
        # match = re.compile(regex).match(file)
        # if match:
        #    file = '%s.%s' % (match.group('code'), match.group('ext'))

        # s-cute
        regex = r"^s-cute\s(?P<code>\d{3}).*?.(?P<ext>%s)$" % EXTENSION
        match = re.compile(regex).match(filename)
        if match:
            ret = "scute-%s.%s" % (match.group("code"), match.group("ext"))
            return ret.lower()

        logger.debug("5. %s", filename)
        regex_list = [
            r"^(?P<name>[a-zA-Z]+)[-_]?(?P<no>\d+)(([-_]?(cd|part)?(?P<part_no>\d))|[-_]?(?P<part_char>\w))?\.(?P<ext>%s)$"
            % EXTENSION,
            r"^\w+.\w+@(?P<name>[a-zA-Z]+)[-_]?(?P<no>\d+)(([-_\.]?(cd|part)?(?P<part_no>\d))|[-_\.]?(?P<part_char>\w))?\.(?P<ext>%s)$"
            % EXTENSION,
        ]
        for regex in regex_list:
            match = re.compile(regex).match(filename)
            if match:
                ret = filename
                part = None
                if match.group("part_no") is not None:
                    part = "cd%s" % match.group("part_no")
                elif match.group("part_char") is not None:
                    part = "cd%s" % (ord(match.group("part_char").lower()) - ord("a") + 1)
                if part is None:
                    ret = "%s-%s.%s" % (match.group("name").lower(), match.group("no"), match.group("ext"))
                else:
                    ret = "%s-%s%s.%s" % (match.group("name").lower(), match.group("no"), part, match.group("ext"))
                # logger.debug('%s -> %s' % (file, ret))
                return ret.lower()

        # T28 - 매치여야함.
        # logger.debug('N2 before:%s', file)
        regex = r"(?P<name>[a-zA-Z]+\d+)\-(?P<no>\d+).*?\.(?P<ext>%s)$" % EXTENSION
        match = re.compile(regex).match(filename)
        if match:
            ret = "%s-%s.%s" % (match.group("name"), match.group("no"), match.group("ext"))
            # logger.debug('N2. %s -> %s' % (file, ret))
            return ret.lower()

        # 오리지널로 ABC123 매치여야함.
        # hjd2048.com-0113meyd466-264.mp4
        # logger.debug('N3 before:%s', original_filename)

        regex = r"^(?P<name>[a-zA-Z]{3,})\-?(?P<no>\d+).*?\.(?P<ext>%s)$" % EXTENSION
        # logger.debug(file)
        match = re.compile(regex).match(filename)
        if match:
            ret = "%s-%s.%s" % (match.group("name"), match.group("no"), match.group("ext"))
            # logger.debug('N3. %s -> %s' % (file, ret))]
            # logger.debug('match 00')
            return ret.lower()

        regex = r"^(?P<name>[a-zA-Z]{3,})\-?(?P<no>\d+).*?\.(?P<ext>%s)$" % EXTENSION
        match = re.compile(regex).match(original_filename)
        if match:
            ret = "%s-%s.%s" % (match.group("name"), match.group("no"), match.group("ext"))
            # logger.debug('N3. %s -> %s' % (file, ret))]
            # logger.debug('match 11')
            return ret.lower()

        # 서치
        # logger.debug('N1 before:%s', file)
        regex = r"(?P<name>[a-zA-Z]+)\-(?P<no>\d+).*?\.(?P<ext>%s)$" % EXTENSION
        match = re.compile(regex).search(filename)
        if match:
            ret = "%s-%s.%s" % (match.group("name"), match.group("no"), match.group("ext"))
            # logger.debug('N1. %s -> %s' % (file, ret))
            # logger.debug('match 22')
            return ret.lower()

        # 서치
        # logger.debug('N1 before:%s', file)
        regex = r"(?P<name>[a-zA-Z]+)\-(?P<no>\d+).*?\.(?P<ext>%s)$" % EXTENSION
        match = re.compile(regex).search(original_filename)
        if match:
            ret = "%s-%s.%s" % (match.group("name"), match.group("no"), match.group("ext"))
            # logger.debug('N1. %s -> %s' % (file, ret))
            # logger.debug('match 33')
            return ret.lower()

        # 21-01-08 fbfb.me@sivr00103.part1.mp4
        regex = r"\w+.\w+@(?P<name>[a-zA-Z]+)(?P<no>\d{5})\.(cd|part)(?P<part_no>\d+)\.(?P<ext>%s)$" % EXTENSION
        match = re.compile(regex).match(original_filename)
        if match:
            ret = filename
            part = None
            if match.group("part_no") is not None:
                part = "cd%s" % match.group("part_no")
            if part is None:
                ret = "%s-%s.%s" % (match.group("name").lower(), match.group("no"), match.group("ext"))
            else:
                ret = "%s-%s%s.%s" % (match.group("name").lower(), match.group("no"), part, match.group("ext"))
            # logger.debug('%s -> %s' % (file, ret))
            return ret.lower()

        # 20-02-02
        regex = r"\w+.\w+@(?P<name>[a-zA-Z]+)(?P<no>\d{5}).*?.(?P<ext>%s)$" % EXTENSION
        match = re.compile(regex).search(original_filename)
        if match:
            no = match.group("no").replace("0", "").zfill(3)
            ret = "%s-%s.%s" % (match.group("name"), no, match.group("ext"))
            # logger.debug('match 44')
            return ret.lower()

        # logger.debug('%s -> %s' % (file, None))
        return None














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