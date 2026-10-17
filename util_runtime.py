"""Small cooperative pauses and media caches scoped to one processing job."""
import copy
import json
import time
from collections import OrderedDict
from contextvars import ContextVar
from functools import wraps

_JOB_CHECKPOINT = ContextVar('fp_av_job_checkpoint', default=None)

_MEDIA_CACHE = ContextVar('fp_av_media_cache', default=None)
_CACHE_LIMIT = 4096
_LAST_YIELD = ContextVar('fp_av_last_yield', default=None)
_YIELD_INTERVAL = 0.02


def pause(seconds=0.001):
    check = _JOB_CHECKPOINT.get()
    if check and seconds > .2:
        deadline = time.monotonic() + seconds
        while True:
            check()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            pause(min(.2, remaining))
        return
    if check:
        check()
    _sleep(seconds)
    if check:
        check()


def _sleep(seconds):
    try:
        from gevent import monkey, sleep
    except ImportError:
        time.sleep(seconds)
        return
    if monkey.is_module_patched('socket') or monkey.is_module_patched('time'):
        sleep(seconds)
    else:
        time.sleep(seconds)


def cooperate():
    check = _JOB_CHECKPOINT.get()
    if check:
        check()
    # Frequent checkpoints remain cheap even in subtitle pairing's nested loops.
    now = time.monotonic()
    last = _LAST_YIELD.get()
    if last is None or now - last >= _YIELD_INTERVAL:
        pause()
        _LAST_YIELD.set(time.monotonic())


def metadata_delay(config):
    """User-configured delay before external metadata retrieval, in seconds."""
    seconds = max(0.0, float(config.get('파일당딜레이', 0) or 0))
    if seconds:
        pause(seconds)


def processing_job(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        token = _MEDIA_CACHE.set(OrderedDict())
        yield_token = _LAST_YIELD.set(None)
        try:
            cooperate()
            return func(*args, **kwargs)
        finally:
            _MEDIA_CACHE.reset(token)
            _LAST_YIELD.reset(yield_token)
    return wrapped


def cached_media_info(probe):
    @wraps(probe)
    def wrapped(cls, file_path, ffprobe_config):
        cooperate()
        cache = _MEDIA_CACHE.get()
        if cache is None:
            return probe(cls, file_path, ffprobe_config)
        try:
            stat = file_path.stat()
            key = (str(file_path.absolute()), stat.st_size, stat.st_mtime_ns,
                   json.dumps(ffprobe_config, sort_keys=True, ensure_ascii=False))
        except (OSError, TypeError, ValueError):
            return probe(cls, file_path, ffprobe_config)
        if key in cache:
            cache.move_to_end(key)
            return copy.deepcopy(cache[key])
        check = _JOB_CHECKPOINT.get()
        if check:
            check(force=True)
        result = probe(cls, file_path, ffprobe_config)
        # Cache failures and None too: retries already belong to the probe itself.
        cache[key] = copy.deepcopy(result)
        if len(cache) > _CACHE_LIMIT:
            cache.popitem(last=False)
        return result
    return wrapped
