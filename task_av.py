"""One monitored YAML run, dispatched in file/job order to the existing processors."""
from importlib import import_module
from pathlib import Path

from support import SupportYaml
from .setup import *
from .util_jobs import monitored_task, progress, _current
from .util_runtime import cooperate


class TaskBase:
    @F.celery.task(bind=True, acks_late=False)
    @monitored_task('av')
    def start(self, *args, dry_run=None):
        delivery = getattr(self.request, 'delivery_info', {}) or {}
        if delivery.get('redelivered') or getattr(self.request, 'redelivered', False):
            logger.warning('이전 세션에서 재전송된 YAML 작업을 취소합니다.')
            return
        if args[0] != 'yaml':
            raise ValueError('통합 YAML 작업 모드가 아닙니다.')
        paths = args[1] if isinstance(args[1], list) else [args[1]]
        if not paths or any(not Path(path).is_file() for path in paths):
            raise ValueError('선택한 YAML 파일이 없거나 실행 시점에 존재하지 않습니다.')

        files = []
        for filepath in paths:
            cooperate()
            data = SupportYaml.read_yaml(filepath)
            selected = []
            for index, job in enumerate(data.get('작업', []), 1):
                if not isinstance(job, dict):
                    raise ValueError(f'{Path(filepath).name} / 작업 #{index}: 작업의 모듈을 확인할 수 없습니다.')
                if not job.get('사용', True):
                    continue
                module = job.get('모듈')
                if module not in ('jav_censored', 'jav_uncensored', 'western'):
                    name = job.get('이름') or f'#{index}'
                    raise ValueError(f'{Path(filepath).name} / {name}: 유효하지 않은 모듈 {module!r} (jav_censored, jav_uncensored, western만 허용)')
                selected.append((module, job))
            files.append((filepath, selected))

        monitor = _current.get()
        for filepath, selected in files:
            cooperate()
            filename = Path(filepath).name
            logger.info('YAML 파일 실행 시작: %s', filename)
            for module, job in selected:
                cooperate()
                label = f"{filename} / {job.get('이름', '이름 없는 작업')} / {module}"
                if monitor:
                    monitor.yaml_job_label = label
                try:
                    progress(stage='YAML 작업 시작', job_name=label, current=0, total=0, item='', force=True)
                    logger.info('YAML 작업 실행 시작: %s', label)
                    processor = import_module('.task_' + module, __package__).TaskBase
                    processor.run_yaml_job(job, dry_run=dry_run)
                finally:
                    if monitor:
                        monitor.yaml_job_label = None
