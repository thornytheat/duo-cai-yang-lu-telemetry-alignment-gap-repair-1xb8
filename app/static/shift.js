// 班次交接：核验并发布。失败时明确提示「上一版保留」，成功后就地刷新到最新 live 版。
(function () {
  function qs(sel) { return document.querySelector(sel); }

  var btn = qs('#shift-publish');
  if (!btn) return;
  var msg = qs('#shift-msg');

  function show(text, cls) {
    if (!msg) return;
    msg.hidden = false;
    msg.className = 'alert ' + (cls || 'alert-info');
    msg.innerHTML = text;
  }

  btn.addEventListener('click', async function () {
    btn.disabled = true;
    var lateField = qs('#late-json');
    var payload = {
      device_id: btn.getAttribute('data-device'),
      metric: btn.getAttribute('data-metric'),
      handover_at: qs('#handover-at') ? qs('#handover-at').value : ''
    };
    var thresholdField = qs('#shift-threshold');
    if (thresholdField && thresholdField.value.trim()) {
      payload.offset_threshold = thresholdField.value.trim();
    }
    if (lateField && lateField.value.trim()) {
      try {
        var extra = JSON.parse(lateField.value);
        payload.late_batches = extra.late_batches || [];
        payload.late_records = extra.late_records || [];
      } catch (e) {
        show('发布失败：迟到补报不是合法 JSON。上一版保持 live，未做任何替换。', 'alert-error');
        btn.disabled = false;
        return;
      }
    }
    show('正在整批核验并发布…', 'alert-info');
    try {
      var res = await fetch('/api/shift/publish', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      var data = null;
      try { data = await res.json(); } catch (e) { data = null; }
      if (!res.ok) {
        var detail = (data && data.detail) || ('请求失败：' + res.status);
        show('发布失败：' + detail + '。上一版保持 live，未做任何替换。', 'alert-error');
        return;
      }
      var pub = data.publication;
      if (data.reused) {
        show('该水位已有同内容版本 v' + pub.version + 'r' + pub.revision +
          '，重跑未新增版本（当前 live 不变）。', 'alert-info');
      } else {
        show('发布成功：v' + pub.version + 'r' + pub.revision + ' 已原子替换为 live。' +
          (pub.diff ? pub.diff.summary : ''), 'alert-info');
      }
      setTimeout(function () { window.location.reload(); }, 600);
    } catch (err) {
      show('发布失败：' + (err && err.message ? err.message : String(err)) +
        '。上一版保持 live，未做任何替换。', 'alert-error');
    } finally {
      btn.disabled = false;
    }
  });
})();
