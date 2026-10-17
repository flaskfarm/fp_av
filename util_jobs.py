"""Cross-process AV job status and bounded logs, independent of file rotation."""
import json
import logging
import sqlite3
import time
import traceback
import uuid
from contextvars import ContextVar
from contextlib import contextmanager
from functools import wraps
from pathlib import Path

_ACTIVE = ('queued', 'running', 'stopping')
_current = ContextVar('fp_av_current_job', default=None)


class JobCancelled(BaseException):
    """Bypass per-file exception handlers; caught at the Celery task boundary."""


class JobStore:
    def __init__(self, path):
        self.path = str(path)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('CREATE TABLE IF NOT EXISTS jobs (family TEXT PRIMARY KEY, data TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY AUTOINCREMENT, family TEXT, run_id TEXT, message TEXT)')

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=5)
        try:
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def read(db, family):
        row = db.execute('SELECT data FROM jobs WHERE family=?', (family,)).fetchone()
        return json.loads(row[0]) if row else {'state': 'idle', 'family': family, 'current': 0, 'total': 0}

    @staticmethod
    def write(db, family, status):
        db.execute('INSERT OR REPLACE INTO jobs VALUES (?,?)', (family, json.dumps(status, ensure_ascii=False)))

    def reserve(self, family, source, mode, task_id=None):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            old = self.read(db, family)
            if old['state'] in _ACTIVE and time.time() - old['updated_at'] < 1800:
                return None
            now = time.time()
            status = dict(family=family, run_id=uuid.uuid4().hex, task_id=task_id,
                          source=source, mode=mode, state='queued', stage='실행 대기',
                          item='', job_name='', current=0, total=0, stop_requested=False,
                          started_at=now, updated_at=now, finished_at=None)
            self.write(db, family, status)
            db.execute('DELETE FROM logs WHERE family=?', (family,))
            return status

    def claim(self, family, run_id):
        """Only one worker may consume a queued reservation."""
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            status = self.read(db, family)
            if status.get('run_id') != run_id or status['state'] != 'queued' or status['stop_requested']:
                return None
            status.update(state='running', stage='설정 읽기', updated_at=time.time())
            self.write(db, family, status)
            return status

    def update(self, family, run_id, **changes):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            status = self.read(db, family)
            if status.get('run_id') != run_id or status['state'] not in _ACTIVE:
                return None
            expected = changes.pop('_expected_update', None)
            if expected is not None and status['updated_at'] != expected:
                return None
            status.update(changes, updated_at=time.time())
            self.write(db, family, status)
            return status

    def stop(self, family, run_id):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            status = self.read(db, family)
            if status.get('run_id') != run_id or status['state'] not in _ACTIVE:
                return False
            queued = status['state'] == 'queued'
            status.update(stop_requested=True, state='stopped' if queued else 'stopping', updated_at=time.time())
            if queued:
                status.update(stage='중단됨', finished_at=time.time())
            self.write(db, family, status)
            return True

    def reset(self, family, run_id=None):
        """Clear only this idle/finished panel, without erasing a newer run."""
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            status = self.read(db, family)
            if status['state'] in _ACTIVE or (status.get('run_id') or '') != (run_id or ''):
                return False
            self.write(db, family, dict(family=family, state='idle', stage='대기 중',
                                       current=0, total=0, item='', job_name='',
                                       run_id=None, stop_requested=False, updated_at=time.time()))
            db.execute('DELETE FROM logs WHERE family=?', (family,))
            return True

    def status(self, family):
        with self.connect() as db:
            return self.read(db, family)

    def snapshot(self, family):
        with self.connect() as db:
            # One read transaction keeps a new run's status and logs together.
            db.execute('BEGIN')
            status = self.read(db, family)
            rows = db.execute('SELECT id,message FROM logs WHERE family=? AND run_id=? ORDER BY id',
                              (family, status.get('run_id', ''))).fetchall()
        if status['state'] in _ACTIVE and time.time() - status['updated_at'] >= 1800:
            changed = self.update(family, status['run_id'], state='interrupted', stage='상태 갱신이 중단되었습니다', finished_at=time.time(), _expected_update=status['updated_at'])
            if changed:
                status = changed
        return {'status': status, 'logs': [{'id': row[0], 'message': row[1]} for row in rows]}

    def append(self, family, run_id, messages):
        if not messages:
            return
        with self.connect() as db:
            db.executemany('INSERT INTO logs(family,run_id,message) VALUES (?,?,?)',
                           [(family, run_id, message) for message in messages])
            db.execute('DELETE FROM logs WHERE family=? AND id NOT IN '
                       '(SELECT id FROM logs WHERE family=? ORDER BY id DESC LIMIT 300)', (family, family))


def store():
    from .setup import path_data
    return JobStore(Path(path_data) / 'db' / 'fp_av_jobs.sqlite3')


class Monitor(logging.Handler):
    def __init__(self, backend, status):
        super().__init__()
        self.backend, self.status = backend, status
        self.pending = []
        self.last_check = self.last_flush = 0
        self.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s %(message)s', datefmt='%H:%M:%S'))

    def emit(self, record):
        if _current.get() is self:
            self.pending.append(self.format(record)[:8000])
            if record.levelno >= logging.INFO or len(self.pending) >= 50 or time.monotonic() - self.last_flush >= .5:
                self.flush()

    def flush(self):
        if getattr(self, 'pending', None):
            pending, self.pending = self.pending, []
            self.backend.append(self.status['family'], self.status['run_id'], pending)
            self.last_flush = time.monotonic()

    def checkpoint(self, force=False, **changes):
        now = time.monotonic()
        self.status.update(changes)
        if not force and now - self.last_check < .2:
            return
        self.last_check = now
        self.flush()
        shared = self.backend.status(self.status['family'])
        if (shared.get('run_id') != self.status['run_id'] or shared.get('stop_requested')
                or shared['state'] not in _ACTIVE):
            raise JobCancelled()
        # Never overwrite a stop request with the worker's local copy.
        result = self.backend.update(self.status['family'], self.status['run_id'],
                                     **{k: self.status[k] for k in ('stage', 'item', 'current', 'total', 'job_name')})
        if result is None:
            raise JobCancelled()


def checkpoint():
    monitor = _current.get()
    if monitor:
        monitor.checkpoint()


def progress(stage=None, current=None, total=None, item=None, job_name=None, force=False):
    monitor = _current.get()
    if monitor:
        monitor.checkpoint(force=force, **{k: v for k, v in
                           dict(stage=stage, current=current, total=total, item=item, job_name=job_name).items() if v is not None})


def monitored_task(family):
    """Wrap the entire Celery invocation, including every job in a YAML file."""
    def decorate(func):
        @wraps(func)
        def wrapped(task, *args, **kwargs):
            from .setup import logger
            backend = store()
            run_id = kwargs.pop('monitor_token', None)
            mode = args[0] if args else 'default'
            channel = family + ('_yaml' if mode == 'yaml' else '')
            if run_id:
                status = backend.snapshot(channel)['status']
                if status.get('run_id') != run_id or status['state'] not in _ACTIVE:
                    return
            else:
                status = backend.reserve(channel, channel, mode, getattr(task.request, 'id', None))
                if status is None:
                    logger.warning('이미 실행 중인 %s 작업이 있어 중복 실행을 건너뜁니다.', channel)
                    return
            status = backend.claim(channel, status['run_id'])
            if status is None:
                return
            monitor = Monitor(backend, status)
            token = _current.set(monitor)
            from .util_runtime import _JOB_CHECKPOINT
            checkpoint_token = _JOB_CHECKPOINT.set(monitor.checkpoint)
            logger.addHandler(monitor)
            state, stage = 'completed', '완료'
            try:
                monitor.checkpoint(force=True)
                progress('설정 읽기', force=True)
                logger.info('AV 작업 시작: %s (%s)', channel, mode)
                result = func(task, *args, **kwargs)
                monitor.checkpoint(force=True)
                return result
            except JobCancelled:
                state, stage = 'stopped', '중단됨'
                logger.info('사용자 요청으로 AV 작업을 중단했습니다.')
            except Exception:
                state, stage = 'failed', '오류로 종료'
                logger.error(traceback.format_exc())
                raise
            finally:
                try:
                    monitor.flush()
                    backend.update(channel, status['run_id'], state=state, stage=stage, finished_at=time.time())
                finally:
                    logger.removeHandler(monitor)
                    _current.reset(token)
                    _JOB_CHECKPOINT.reset(checkpoint_token)
        return wrapped
    return decorate


class JobPanelMixin:
    """Normal authenticated plugin routes for all three modules and YAML variants."""
    def process_normal(self, sub, req):
        if sub != 'job_stream':
            return None
        from flask import Response, stream_with_context
        backend = store()
        @stream_with_context
        def events():
            yield ': ' + ' ' * 2048 + '\n\n'
            last, ping = None, time.monotonic()
            while True:
                payload = json.dumps(backend.snapshot(self.name), ensure_ascii=False)
                if payload != last:
                    yield 'data: ' + payload + '\n\n'
                    last = payload
                elif time.monotonic() - ping >= 15:
                    yield ': ping\n\n'
                    ping = time.monotonic()
                from .util_runtime import pause
                pause(1)
        return Response(events(), mimetype='text/event-stream', headers={
            'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

    def process_command(self, command, arg1, arg2, arg3, req):
        return self.job_command(command, arg1, arg2)

    def job_command(self, command, arg1, arg2):
        if command not in ('job_status', 'job_start', 'job_stop', 'job_reset'):
            return None
        from .setup import P
        from flask import jsonify
        family = self.name.removesuffix('_yaml')
        backend = store()
        if command == 'job_status':
            return jsonify(ret='success', **backend.snapshot(self.name))
        if command == 'job_reset':
            reset = backend.reset(self.name, arg1)
            if reset:
                return jsonify(ret='success', msg='진행 상황과 로그를 초기화했습니다.', **backend.snapshot(self.name))
            return jsonify(ret='warning', msg='실행 중이거나 상태가 변경된 작업은 초기화할 수 없습니다.')
        if command == 'job_stop':
            stopped = backend.stop(self.name, arg1)
            return jsonify(ret='success' if stopped else 'warning',
                           msg='중단을 요청했습니다.' if stopped else '작업이 종료되었거나 변경되었습니다.')
        mode = 'yaml' if self.name.endswith('_yaml') else 'default'
        path = str(arg1 or '').strip()
        if mode == 'yaml':
            path = path or P.ModelSetting.get(f'{self.name}_filepath')
            if not path or not Path(path).is_file():
                return jsonify(ret='warning', msg='유효한 YAML 파일 경로를 지정하세요.')
        elif path:
            if not Path(path).is_dir():
                return jsonify(ret='warning', msg='처리할 폴더가 존재하지 않습니다.')
            mode = 'one_path'
        task_id = uuid.uuid4().hex
        status = backend.reserve(self.name, self.name, mode, task_id)
        if status is None:
            return jsonify(ret='warning', msg='이미 이 작업이 실행 중입니다.')
        try:
            from importlib import import_module
            task = import_module(f'.task_{family}', __package__).TaskBase.start
            args = (mode, path) if path else (mode,)
            task.apply_async(args=args, kwargs={'monitor_token': status['run_id']}, task_id=task_id)
        except Exception as exc:
            backend.append(self.name, status['run_id'], ['작업 실행 요청 실패: ' + str(exc)[:8000]])
            backend.update(self.name, status['run_id'], state='failed', stage='실행 요청 실패', finished_at=time.time())
            return jsonify(ret='error', msg=str(exc))
        return jsonify(ret='success', msg='작업 실행을 요청했습니다.', run_id=status['run_id'])
