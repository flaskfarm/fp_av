(function () {
  'use strict';
  $(function () {
    var panel = document.querySelector('.av-job-panel');
    if (!panel || panel.dataset.ready) return;
    panel.dataset.ready = 'true';
    function el(name) { return panel.querySelector('.av-job-' + name); }
    var labels = {idle:'대기 중', queued:'실행 대기', running:'실행 중', stopping:'중단 요청됨', stopped:'중단됨', completed:'완료', failed:'오류', interrupted:'연결 중단으로 종료'};
    var status = {}, runId = null, cursor = 0, source = null, connected = false, disposed = false, pending = false;
    var list = window.AvJobList;
    var dryToggle = panel.querySelector('.av-job-dryrun');
    var dryRun = list ? list.dryRun() : false;
    var dryKey = 'fp_av:job_panel:' + panel.dataset.module + ':v1';
    if (!list) {
      try { dryRun = JSON.parse(window.localStorage.getItem(dryKey)) === true; } catch (error) { /* Default OFF. */ }
    }
    function dryButton() {
      if (!dryToggle) return;
      dryToggle.textContent = '드라이런 ' + (dryRun ? 'ON' : 'OFF');
      dryToggle.setAttribute('aria-pressed', String(dryRun));
      dryToggle.className = 'btn btn-sm mr-2 av-job-dryrun ' + (dryRun ? 'btn-warning' : 'btn-outline-secondary');
      el('start').textContent = dryRun ? '드라이런 실행' : '1회 실행';
    }
    if (dryToggle) dryToggle.addEventListener('click', function () {
      if (active() || pending) return;
      dryRun = !dryRun;
      if (list) list.setDryRun(dryRun);
      else {
        try { window.localStorage.setItem(dryKey, JSON.stringify(dryRun)); } catch (error) { /* Storage may be disabled. */ }
      }
      dryButton();
    });
    dryButton();
    function active() { return ['queued', 'running', 'stopping'].indexOf(status.state) >= 0; }
    function buttons() {
      el('start').disabled = active() || pending;
      el('reset').disabled = active() || pending;
      el('stop').disabled = !active() || status.state === 'stopping';
      if (dryToggle) dryToggle.disabled = active() || pending;
    }
    function clock() {
      if (!status.started_at) { el('time').textContent = ''; return; }
      var seconds = Math.max(0, Math.floor((status.finished_at || Date.now()/1000) - status.started_at));
      el('time').textContent = '경과 ' + Math.floor(seconds/60) + '분 ' + seconds%60 + '초';
    }
    function render(snapshot) {
      if ((snapshot.status || {}).updated_at < (status.updated_at || 0)) return;
      status = snapshot.status || {};
      if (list) list.observe(status);
      if (runId !== status.run_id) {
        runId = status.run_id;
        cursor = 0;
        el('logs').textContent = '';
      }
      el('state').textContent = labels[status.state] || status.state || '대기 중';
      el('state').className = 'badge av-job-state badge-' + (active() ? 'info' : status.state === 'failed' ? 'danger' : status.state === 'completed' ? 'success' : 'secondary');
      el('stage').textContent = status.stage || '대기 중';
      el('item').textContent = status.item || '';
      var names = {default:'기본 설정', dry_run:'모의 실행', manual_path:'메타 실패 경로 재처리', one_path:'지정 폴더 1회 실행', yaml:'YAML 설정'};
      var jobName = names[status.job_name] || status.job_name;
      el('source').textContent = status.source ? (status.source.endsWith('_yaml') ? 'YAML 작업' : '기본 작업') + (jobName ? ' · ' + jobName : '') + (status.dry_run ? ' · 드라이런' : '') : '';
      var current = Number(status.current) || 0, total = Number(status.total) || 0;
      var percent = total ? Math.min(100, Math.round(current / total * 100)) : 0;
      el('count').textContent = total ? '현재 단계 ' + current + ' / ' + total : (active() ? (status.state === 'queued' ? '실행 대기' : '대상 확인 중') : '대상 없음');
      if (status.state === 'idle') el('count').textContent = '처리 0 / 0';
      el('progress').style.width = (active() && !total ? 100 : percent) + '%';
      el('progress').textContent = total ? percent + '%' : '';
      el('progress').setAttribute('aria-valuenow', percent);
      el('progress').classList.toggle('progress-bar-striped', active());
      el('progress').classList.toggle('progress-bar-animated', active());
      (snapshot.logs || []).forEach(function (entry) {
        if (entry.id <= cursor) return;
        var line = document.createElement('div');
        line.textContent = entry.message;
        el('logs').appendChild(line);
        cursor = entry.id;
      });
      while (el('logs').children.length > 300) el('logs').removeChild(el('logs').firstChild);
      if (el('autoscroll').checked) el('logs').scrollTop = el('logs').scrollHeight;
      buttons();
      clock();
    }
    function command(name, arg, callback, arg2) {
      globalSendCommand(name, arg || '', arg2 || '', '', callback);
    }
    function poll() {
      if (disposed) return;
      command('job_status', '', function (snapshot) {
        if (snapshot && snapshot.status) render(snapshot);
      });
    }
    function stream() {
      if (!window.EventSource) { el('connection').textContent = '주기적으로 상태 갱신'; return; }
      source = new EventSource('/' + encodeURIComponent(panel.dataset.package) + '/normal/' + encodeURIComponent(panel.dataset.module) + '/job_stream');
      source.onopen = function () { connected = true; el('connection').textContent = '실시간 연결됨'; };
      source.onmessage = function (event) {
        try { render(JSON.parse(event.data)); } catch (error) { el('connection').textContent = '상태를 다시 확인합니다'; poll(); }
      };
      source.onerror = function () { connected = false; el('connection').textContent = '연결 재시도 중 · 상태 자동 갱신'; };
    }
    el('start').addEventListener('click', function () {
      pending = true; buttons();
      el('notice').textContent = '실행 요청 중…';
      var pathId = panel.dataset.module.endsWith('_yaml') ? panel.dataset.module + '_filepath' : 'av-job-path';
      var pathInput = document.getElementById(pathId);
      command('job_start', pathInput ? pathInput.value : '', function (result) {
        pending = false;
        el('notice').textContent = result.msg || result.message || '';
        buttons(); poll();
      }, dryToggle ? String(dryRun) : '');
      // Recover the button after a dropped command response; the backend guards duplicates.
      window.setTimeout(function () { pending = false; buttons(); poll(); }, 10000);
    });
    el('stop').addEventListener('click', function () {
      el('stop').disabled = true;
      command('job_stop', status.run_id, function (result) {
        el('notice').textContent = result.msg || '';
        poll();
      });
    });
    el('reset').addEventListener('click', function () {
      pending = true; buttons();
      command('job_reset', status.run_id, function (result) {
        pending = false;
        el('notice').textContent = result.msg || '';
        if (result.status) render(result);
        buttons(); poll();
      });
      window.setTimeout(function () { pending = false; buttons(); poll(); }, 10000);
    });
    el('clear').addEventListener('click', function () { el('logs').textContent = ''; });
    poll(); stream();
    function startTimer() {
      return window.setInterval(function () {
        clock();
        if (!connected && !document.hidden) poll();
      }, 5000);
    }
    var timer = startTimer();
    window.addEventListener('pagehide', function () {
      disposed = true;
      window.clearInterval(timer);
      if (source) source.close();
    });
    window.addEventListener('pageshow', function (event) {
      if (!event.persisted) return;
      disposed = false; connected = false;
      poll(); stream(); timer = startTimer();
    });
  });
}());
