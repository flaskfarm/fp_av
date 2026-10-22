(function () {
  'use strict';
  $(function () {
    var panel = document.querySelector('.av-job-panel');
    if (!panel || panel.dataset.module.endsWith('_yaml')) return;
    var module = panel.dataset.module;
    var key = 'fp_av:job_list:' + module + ':v1';
    var page = 1, dryRun = false, busy = false, queued = null, timer = null;
    var runId, revision = 0;
    var endpoint = '/' + panel.dataset.package + '/ajax/' + module + '/web_list';
    function validPage(value) {
      var number = Number(value);
      return Number.isSafeInteger(number) && number > 0 ? number : 1;
    }
    function save() {
      try {
        window.localStorage.setItem(key, JSON.stringify({
          option1: $('#option1').val(), order: $('#order').val(),
          search: $('#search_word').val(), page: page, dry_run: dryRun
        }));
      } catch (error) { /* Storage can be disabled by the browser. */ }
    }
    try {
      var saved = JSON.parse(window.localStorage.getItem(key));
      if (saved && typeof saved === 'object') {
        ['option1', 'order'].forEach(function (name) {
          var select = document.getElementById(name);
          if (Array.prototype.some.call(select.options, function (option) { return option.value === saved[name]; })) {
            select.value = saved[name];
          }
        });
        if (typeof saved.search === 'string') $('#search_word').val(saved.search);
        page = validPage(saved.page);
        dryRun = saved.dry_run === true;
      }
    } catch (error) { /* Use the page defaults for invalid JSON. */ }

    // FF's search function does not return its jqXHR. Observe only this list's
    // completion to serialize requests, retaining the latest requested page.
    var request = window.globalRequestSearch;
    window.globalRequestSearch = function (nextPage, moveTop) {
      page = validPage(nextPage);
      save();
      if (busy) { queued = {page: page, moveTop: moveTop}; return; }
      busy = true;
      request(page, moveTop);
    };
    $(document).ajaxComplete(function (event, xhr, settings) {
      if (settings.url !== endpoint || !busy) return;
      busy = false;
      var data = xhr.responseJSON;
      if (!queued && data && data.paging) {
        var last = Math.max(1, Number(data.paging.total_page) || 1);
        if (page > last) queued = {page: last, moveTop: false};
      }
      if (queued) {
        var next = queued;
        queued = null;
        window.globalRequestSearch(next.page, next.moveTop);
      }
    });
    window.AvJobList = {
      dryRun: function () { return dryRun; },
      setDryRun: function (value) { dryRun = value === true; save(); },
      observe: function (status) {
        if (status.family !== module) return;
        var next = Number(status.list_revision) || 0;
        var changed = runId !== status.run_id ? next > 0 : next > revision;
        runId = status.run_id;
        revision = next;
        if (!changed) return;
        window.clearTimeout(timer);
        timer = window.setTimeout(function () { window.globalRequestSearch(page, false); }, 250);
      }
    };
    $('#form_search').on('submit', function (event) {
      event.preventDefault();
      window.globalRequestSearch(1);
    });
    $('#reset_btn').on('click', function (event) {
      event.preventDefault();
      $('#order').val('desc'); $('#option1').val('all'); $('#search_word').val('');
      window.globalRequestSearch(1);
    });
    $('#order').on('change', function () { window.globalRequestSearch(1); });
    $('#option1').on('change', save);
    $('#search_word').on('input', save);
    $('body').on('click', '#page', function (event) {
      event.preventDefault();
      window.globalRequestSearch($(this).data('page'), false);
    });
    window.globalRequestSearch(page, false);
  });
}());
