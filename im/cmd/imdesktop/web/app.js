/* 前端逻辑:通过本地 WebSocket 与 Go 端 go-libp2p 节点通信。 */
(() => {
  const $ = (id) => document.getElementById(id);
  const els = {
    selfNick: $("selfNick"), selfId: $("selfId"), room: $("room"),
    status: $("status"), peers: $("peers"), peerCount: $("peerCount"),
    targetLabel: $("targetLabel"), connDot: $("connDot"), connText: $("connText"),
    messages: $("messages"), empty: $("empty"),
    input: $("input"), sendBtn: $("sendBtn"),
    emojiBtn: $("emojiBtn"), emojiPicker: $("emojiPicker"),
    fileInput: $("fileInput"), micBtn: $("micBtn"),
    recording: $("recording"), recTime: $("recTime"), toast: $("toast"),
  };

  let self = { id: "", nick: "", room: "" };
  let peers = [];
  let target = "all"; // "all" 或 peer id
  let ws = null;
  let reconnectTimer = null;

  /* ---------------- WebSocket 桥接 ---------------- */
  function connect() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    ws = new WebSocket(`${proto}://${location.host}/ws`);
    ws.onopen = () => setConn(true);
    ws.onclose = () => { setConn(false); scheduleReconnect(); };
    ws.onerror = () => ws.close();
    ws.onmessage = (e) => {
      let msg;
      try { msg = JSON.parse(e.data); } catch { return; }
      handleEvent(msg.ev, msg.data);
    };
  }
  function scheduleReconnect() {
    if (reconnectTimer) return;
    reconnectTimer = setTimeout(() => { reconnectTimer = null; connect(); }, 1500);
  }
  function setConn(ok) {
    els.connDot.className = "dot " + (ok ? "ok" : "bad");
    els.connText.textContent = ok ? "本地桥接已连接" : "本地桥接断开";
  }
  function wsSend(obj) {
    if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
  }

  /* ---------------- 事件分发 ---------------- */
  function handleEvent(ev, data) {
    switch (ev) {
      case "self":
        self = data;
        els.selfNick.textContent = data.nick || "我";
        els.selfId.textContent = shortId(data.id);
        els.room.textContent = data.room || "lobby";
        break;
      case "peers": peers = data || []; renderPeers(); break;
      case "status": setStatus(data.text); break;
      case "msg": addMessage(data); break;
      case "error": toast(data.text); break;
    }
  }

  function setStatus(text) {
    els.status.textContent = text;
    els.status.classList.toggle("ok", /已连接/.test(text));
    els.status.classList.toggle("bad", /断开|无法/.test(text));
  }
  function shortId(id) { return id ? id.slice(0, 10) + "…" + id.slice(-4) : ""; }

  /* ---------------- 节点列表 ---------------- */
  function renderPeers() {
    els.peerCount.textContent = peers.length;
    els.peers.innerHTML = "";
    els.peers.appendChild(peerRow({ id: "all", nick: "所有人", isAll: true, connected: peers.some(p => p.connected) }));
    peers.forEach(p => els.peers.appendChild(peerRow(p)));
    if (target !== "all" && !peers.some(p => p.id === target)) target = "all";
    renderPeersHighlight();
    updateTargetLabel();
  }
  function renderPeersHighlight() {
    els.peers.querySelectorAll(".peer").forEach(li => {
      li.classList.toggle("active", li.dataset.id === target);
    });
  }
  function peerRow(p) {
    const li = document.createElement("li");
    li.className = "peer" + (p.connected ? " online" : " offline");
    li.dataset.id = p.id;
    const nick = p.nick || shortId(p.id);
    li.innerHTML = `<span class="avatar"></span><span class="pname"></span><span class="pdot"></span>`;
    li.querySelector(".avatar").textContent = (nick[0] || "?").toUpperCase();
    li.querySelector(".pname").textContent = p.isAll ? "所有人" : nick;
    li.title = p.isAll ? "广播给所有在线节点" : p.id;
    li.onclick = () => { target = p.id; renderPeersHighlight(); updateTargetLabel(); };
    return li;
  }
  function updateTargetLabel() {
    if (target === "all") { els.targetLabel.textContent = "所有人"; return; }
    const p = peers.find(x => x.id === target);
    els.targetLabel.textContent = p ? (p.nick || shortId(p.id)) : "所有人";
  }

  /* ---------------- 消息渲染 ---------------- */
  function nickOf(m) { return m.from_nick || (m.self ? (self.nick || "我") : shortId(m.from)); }

  function addMessage(m) {
    if (els.empty) { els.empty.remove(); els.empty = null; }
    const wrap = document.createElement("div");
    wrap.className = "msg" + (m.self ? " self" : "");

    const avatar = document.createElement("div");
    avatar.className = "m-avatar";
    const nick = nickOf(m);
    avatar.textContent = (nick[0] || "?").toUpperCase();

    const body = document.createElement("div");
    body.className = "body";

    const meta = document.createElement("div");
    meta.className = "meta";
    const s1 = document.createElement("span"); s1.textContent = nick;
    const s2 = document.createElement("span"); s2.textContent = fmtTime(m.ts);
    meta.append(s1, s2);

    body.append(meta, renderContent(m));
    wrap.append(avatar, body);
    els.messages.appendChild(wrap);
    els.messages.scrollTop = els.messages.scrollHeight;
  }

  function renderContent(m) {
    if (m.type === "image") {
      const b = document.createElement("div"); b.className = "bubble";
      const img = document.createElement("img");
      img.src = m.media_url; img.alt = m.file_name || "image";
      img.onclick = () => lightbox(m.media_url);
      b.appendChild(img);
      return b;
    }
    if (m.type === "voice") {
      const b = document.createElement("div"); b.className = "bubble";
      const a = document.createElement("audio");
      a.controls = true; a.src = m.media_url; a.preload = "metadata";
      b.appendChild(a);
      return b;
    }
    if (m.type === "file") {
      const b = document.createElement("div"); b.className = "bubble";
      const a = document.createElement("a");
      a.className = "filecard"; a.href = m.media_url; a.download = m.file_name || "";
      a.innerHTML = `<span class="fico">📄</span><span><span class="fname"></span><br><span class="fsize"></span></span>`;
      a.querySelector(".fname").textContent = m.file_name || "文件";
      a.querySelector(".fsize").textContent = fmtSize(m.size || 0);
      b.appendChild(a);
      return b;
    }
    const b = document.createElement("div");
    b.className = "bubble" + (isOnlyEmoji(m.text) ? " big-emoji" : "");
    b.textContent = m.text || "";
    return b;
  }

  function isOnlyEmoji(s) {
    if (!s) return false;
    const stripped = s.replace(/[\s\u200d\uFE0F]/gu, "");
    if (!stripped || stripped.length > 12 || /[A-Za-z0-9]/.test(stripped)) return false;
    return /[\u{1F000}-\u{1FAFF}\u{2600}-\u{27BF}\u{2B00}-\u{2BFF}\u{1F1E6}-\u{1F1FF}\u{2190}-\u{21FF}]/u.test(stripped);
  }

  function fmtTime(ts) {
    const d = ts ? new Date(ts) : new Date();
    return d.toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit" });
  }
  function fmtSize(n) {
    if (n < 1024) return n + " B";
    if (n < 1048576) return (n / 1024).toFixed(1) + " KB";
    return (n / 1048576).toFixed(2) + " MB";
  }
  function lightbox(src) {
    const lb = document.createElement("div");
    lb.className = "lightbox";
    const img = document.createElement("img"); img.src = src;
    lb.appendChild(img);
    lb.onclick = () => lb.remove();
    document.body.appendChild(lb);
  }
  let toastTimer = null;
  function toast(text) {
    els.toast.textContent = text;
    els.toast.hidden = false;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => { els.toast.hidden = true; }, 3200);
  }

  /* ---------------- 发送文字/表情 ---------------- */
  function sendText() {
    const text = els.input.value.trim();
    if (!text) return;
    wsSend({ cmd: "send", to: target, kind: isOnlyEmoji(text) ? "emoji" : "text", text });
    els.input.value = "";
    autoGrow();
  }
  els.sendBtn.onclick = sendText;
  els.input.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); sendText(); }
  });
  function autoGrow() {
    els.input.style.height = "auto";
    els.input.style.height = Math.min(els.input.scrollHeight, 140) + "px";
  }
  els.input.addEventListener("input", autoGrow);

  /* ---------------- 表情选择器 ---------------- */
  const EMOJIS = [
    "😀", "😄", "😁", "😂", "🤣", "😊", "😍", "😘", "😜", "🤔",
    "🤨", "😐", "😴", "😭", "😡", "🥳", "😎", "🤗", "🤝", "🙏",
    "👍", "👎", "👏", "🙌", "💪", "👌", "✌️", "🤙", "👋", "🫶",
    "❤️", "💔", "💯", "🔥", "✨", "🎉", "🎁", "⭐", "🌟", "💡",
    "🐶", "🐱", "🐼", "🦊", "🐷", "🐵", "🦄", "🐝", "🐢", "🦋",
    "🍎", "🍔", "🍕", "🍜", "🍰", "☕", "🍺", "🍦", "🥑", "🍉",
    "⚽", "🏀", "🎮", "🎵", "🎬", "📷", "💻", "📱", "✈️", "🚀",
    "✅", "❌", "❗", "❓", "💤", "💬", "👀", "🧠", "🕐", "🎯",
  ];
  EMOJIS.forEach((e) => {
    const b = document.createElement("button");
    b.type = "button";
    b.textContent = e;
    b.onclick = () => insertAtCursor(els.input, e);
    els.emojiPicker.appendChild(b);
  });
  els.emojiBtn.onclick = (e) => {
    e.stopPropagation();
    const r = els.emojiBtn.getBoundingClientRect();
    els.emojiPicker.style.left = r.left + "px";
    els.emojiPicker.style.bottom = (window.innerHeight - r.top + 8) + "px";
    els.emojiPicker.hidden = !els.emojiPicker.hidden;
  };
  document.addEventListener("click", (e) => {
    if (!els.emojiPicker.hidden && !els.emojiPicker.contains(e.target) && e.target !== els.emojiBtn) {
      els.emojiPicker.hidden = true;
    }
  });
  function insertAtCursor(ta, text) {
    const s = ta.selectionStart, e = ta.selectionEnd;
    ta.value = ta.value.slice(0, s) + text + ta.value.slice(e);
    ta.selectionStart = ta.selectionEnd = s + text.length;
    ta.focus();
    autoGrow();
  }

  /* ---------------- 文件 / 图片上传 ---------------- */
  async function uploadFile(file, kind) {
    const fd = new FormData();
    fd.append("to", target);
    fd.append("kind", kind);
    fd.append("file", file, file.name || (kind + ".bin"));
    const resp = await fetch("/api/upload", { method: "POST", body: fd });
    const data = await resp.json().catch(() => ({}));
    if (data.warning) toast(data.warning);
  }
  function kindOf(file) {
    return (file.type || "").startsWith("image/") ? "image" : "file";
  }
  els.fileInput.onchange = async () => {
    for (const f of els.fileInput.files) await uploadFile(f, kindOf(f));
    els.fileInput.value = "";
  };
  const chat = document.querySelector(".chat");
  ["dragover", "drop"].forEach((ev) => chat.addEventListener(ev, (e) => e.preventDefault()));
  chat.addEventListener("drop", async (e) => {
    if (!e.dataTransfer) return;
    for (const f of e.dataTransfer.files) await uploadFile(f, kindOf(f));
  });
  els.input.addEventListener("paste", async (e) => {
    const items = e.clipboardData && e.clipboardData.items;
    if (!items) return;
    for (const it of items) {
      if (it.type.startsWith("image/")) {
        const f = it.getAsFile();
        if (f) { e.preventDefault(); await uploadFile(f, "image"); }
      }
    }
  });

  /* ---------------- 语音消息 ---------------- */
  let recorder = null, recChunks = [], recStart = 0, recTimer = null;
  els.micBtn.onclick = async () => {
    if (recorder && recorder.state === "recording") { stopRecording(); return; }
    await startRecording();
  };
  async function startRecording() {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      recorder = new MediaRecorder(stream);
      recChunks = [];
      recorder.ondataavailable = (e) => { if (e.data.size) recChunks.push(e.data); };
      recorder.onstop = async () => {
        stream.getTracks().forEach((t) => t.stop());
        const ms = Date.now() - recStart;
        const blob = new Blob(recChunks, { type: recorder.mimeType || "audio/webm" });
        const fd = new FormData();
        fd.append("to", target);
        fd.append("kind", "voice");
        fd.append("duration", String(ms));
        fd.append("file", blob, "voice.webm");
        const resp = await fetch("/api/upload", { method: "POST", body: fd });
        const data = await resp.json().catch(() => ({}));
        if (data.warning) toast(data.warning);
      };
      recorder.start();
      recStart = Date.now();
      els.recording.hidden = false;
      els.micBtn.classList.add("recording-on");
      recTimer = setInterval(() => { els.recTime.textContent = fmtDur(Date.now() - recStart); }, 200);
    } catch (err) {
      toast("无法访问麦克风: " + err.message);
    }
  }
  function stopRecording() {
    if (recorder && recorder.state === "recording") recorder.stop();
    els.recording.hidden = true;
    els.micBtn.classList.remove("recording-on");
    clearInterval(recTimer);
  }
  function fmtDur(ms) {
    const s = Math.floor(ms / 1000);
    return Math.floor(s / 60) + ":" + String(s % 60).padStart(2, "0");
  }

  /* ---------------- 启动 ---------------- */
  connect();
  setInterval(() => wsSend({ cmd: "refresh" }), 8000);
})();
