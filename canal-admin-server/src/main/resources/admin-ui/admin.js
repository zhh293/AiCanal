(() => {
  "use strict";

  const DEFAULT_CONFIG = `namespace: production.main.default
version: draft
cluster:
  mode: standalone
server:
  nodeId: \${HOSTNAME}
  dataDir: ./data
  netty:
    port: 11111
  health:
    port: 11112
destinations:
  - id: example-resources
    enabled: true
    ingress:
      mode: FANOUT
      allowedAgents: [example-agent]
      maxBatchRecords: 500
      maxBatchBytes: 4194304
      maxRecordBytes: 1048576
    receiver:
      type: netty-default
      config:
        requireBatchChecksum: true
    parser:
      type: html-default
      config: {}
    classifier:
      type: rule-default
      config: {}
    deduplicator:
      type: hash-default
      config: {}
    logger:
      type: slf4j-json
      config: {}
    storage:
      type: segmented-wal
      config: {}
    egress:
      type: TCP
      channelId: tcp:default
      tcp: {}
`;

  const state = {
    token: sessionStorage.getItem("ai-canal-token") || "",
    actor: sessionStorage.getItem("ai-canal-actor") || "",
    role: "VIEWER",
    namespaces: [],
    namespace: sessionStorage.getItem("ai-canal-namespace") || "",
    releases: [],
    active: null,
    audit: [],
    statusFilter: "ALL",
    search: "",
    validatedContent: "",
  };

  const $ = (selector, root = document) => root.querySelector(selector);
  const $$ = (selector, root = document) => Array.from(root.querySelectorAll(selector));

  const elements = {
    loginGate: $("#loginGate"),
    loginForm: $("#loginForm"),
    loginError: $("#loginError"),
    tokenInput: $("#tokenInput"),
    actorInput: $("#actorInput"),
    app: $("#app"),
    currentNamespace: $("#currentNamespace"),
    namespaceButton: $("#namespaceButton"),
    namespaceMenu: $("#namespaceMenu"),
    roleBadge: $("#roleBadge"),
    actorName: $("#actorName"),
    operatorAvatar: $("#operatorAvatar"),
    activeVersion: $("#activeVersion"),
    activeVersionMeta: $("#activeVersionMeta"),
    heroVersion: $("#heroVersion"),
    releaseCount: $("#releaseCount"),
    destinationCount: $("#destinationCount"),
    destinationTicks: $("#destinationTicks"),
    contentHash: $("#contentHash"),
    recentReleases: $("#recentReleases"),
    releaseTableBody: $("#releaseTableBody"),
    releaseEmpty: $("#releaseEmpty"),
    releaseSearch: $("#releaseSearch"),
    configEditor: $("#configEditor"),
    lineNumbers: $("#lineNumbers"),
    cursorPosition: $("#cursorPosition"),
    byteCount: $("#byteCount"),
    editorState: $("#editorState"),
    validationIcon: $("#validationIcon"),
    validationTitle: $("#validationTitle"),
    validationMessage: $("#validationMessage"),
    validationResults: $("#validationResults"),
    releaseComment: $("#releaseComment"),
    releaseActor: $("#releaseActor"),
    releaseNamespace: $("#releaseNamespace"),
    auditTimeline: $("#auditTimeline"),
    modalBackdrop: $("#modalBackdrop"),
    modalEyebrow: $("#modalEyebrow"),
    modalTitle: $("#modalTitle"),
    modalBody: $("#modalBody"),
    modalActions: $("#modalActions"),
    toastRegion: $("#toastRegion"),
  };

  function headers(json = false) {
    const result = {
      Authorization: `Bearer ${state.token}`,
      "X-Actor": state.actor || state.role.toLowerCase(),
    };
    if (json) result["Content-Type"] = "application/json";
    return result;
  }

  async function request(path, options = {}) {
    const response = await fetch(path, {
      ...options,
      headers: { ...headers(Boolean(options.body)), ...(options.headers || {}) },
    });
    const type = response.headers.get("content-type") || "";
    const payload = type.includes("application/json")
      ? await response.json().catch(() => ({}))
      : await response.text();
    if (!response.ok) {
      const message = typeof payload === "object" ? payload.error : payload;
      const error = new Error(message || `请求失败 (${response.status})`);
      error.status = response.status;
      throw error;
    }
    return payload;
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function formatDate(value, includeTime = true) {
    if (!value) return "—";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    return new Intl.DateTimeFormat("zh-CN", {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      ...(includeTime ? { hour: "2-digit", minute: "2-digit", second: "2-digit" } : {}),
      hour12: false,
    }).format(date);
  }

  function shortHash(hash) {
    return hash ? `${hash.slice(0, 10)}···${hash.slice(-6)}` : "—";
  }

  function statusLabel(status) {
    return { PUBLISHED: "已发布", CREATED: "待发布", SUPERSEDED: "已替代" }[status] || status;
  }

  function roleCanEdit() {
    return state.role === "EDITOR" || state.role === "ADMIN";
  }

  function roleCanPublish() {
    return state.role === "PUBLISHER" || state.role === "ADMIN";
  }

  function applyRole() {
    document.body.classList.remove("role-viewer", "role-editor", "role-publisher", "role-admin");
    document.body.classList.add(`role-${state.role.toLowerCase()}`);
    elements.roleBadge.textContent = state.role;
    elements.actorName.textContent = state.actor || state.role.toLowerCase();
    elements.releaseActor.textContent = state.actor || state.role.toLowerCase();
    elements.operatorAvatar.textContent = initials(state.actor || state.role);
  }

  function initials(value) {
    const chunks = String(value).trim().split(/[\s._-]+/).filter(Boolean);
    if (!chunks.length) return "OP";
    return chunks.slice(0, 2).map((chunk) => chunk[0]).join("").toUpperCase();
  }

  function toast(message, kind = "success") {
    const node = document.createElement("div");
    node.className = `toast ${kind}`;
    node.textContent = message;
    elements.toastRegion.appendChild(node);
    window.setTimeout(() => {
      node.style.opacity = "0";
      node.style.transform = "translateX(18px)";
      window.setTimeout(() => node.remove(), 250);
    }, 4200);
  }

  function showLogin(message = "") {
    elements.app.classList.add("is-hidden");
    elements.loginGate.classList.remove("is-hidden");
    elements.loginError.textContent = message;
    elements.tokenInput.value = state.token;
    elements.actorInput.value = state.actor;
    window.setTimeout(() => elements.tokenInput.focus(), 50);
  }

  function showApp() {
    elements.loginGate.classList.add("is-hidden");
    elements.app.classList.remove("is-hidden");
  }

  async function authenticate() {
    const session = await request("/api/v1/session");
    state.role = session.role;
    if (!state.actor) state.actor = session.actor;
    sessionStorage.setItem("ai-canal-token", state.token);
    sessionStorage.setItem("ai-canal-actor", state.actor);
    applyRole();
    await loadNamespaces();
    showApp();
  }

  async function loadNamespaces() {
    state.namespaces = await request("/api/v1/namespaces");
    if (state.namespace && !state.namespaces.includes(state.namespace)) state.namespace = "";
    if (!state.namespace && state.namespaces.length) state.namespace = state.namespaces[0];
    sessionStorage.setItem("ai-canal-namespace", state.namespace);
    renderNamespaceMenu();
    if (state.namespace) await loadReleases();
    else {
      state.releases = [];
      state.active = null;
      elements.configEditor.value = DEFAULT_CONFIG;
      updateEditorMetrics();
      renderAll();
      if (roleCanEdit()) openNamespacePrompt(true);
    }
  }

  async function loadReleases() {
    if (!state.namespace) return;
    state.releases = await request(`/api/v1/namespaces/${encodeURIComponent(state.namespace)}/releases`);
    state.active = [...state.releases].reverse().find((release) => release.status === "PUBLISHED") || null;
    renderAll();
  }

  function renderNamespaceMenu() {
    const items = state.namespaces.map((namespace) => `
      <button type="button" role="option" data-namespace="${escapeHtml(namespace)}" class="${namespace === state.namespace ? "is-active" : ""}">
        <span>${escapeHtml(namespace)}</span>${namespace === state.namespace ? "<span>✓</span>" : ""}
      </button>`).join("");
    const create = roleCanEdit()
      ? '<button type="button" class="create-namespace" data-create-namespace><span>＋ 新建命名空间</span><span>↗</span></button>'
      : "";
    elements.namespaceMenu.innerHTML = items || '<div class="table-empty">尚无命名空间</div>';
    elements.namespaceMenu.insertAdjacentHTML("beforeend", create);
    elements.currentNamespace.textContent = state.namespace || "尚未选择";
    elements.releaseNamespace.textContent = state.namespace || "尚未选择";
  }

  function renderAll() {
    renderNamespaceMenu();
    renderOverview();
    renderReleases();
  }

  function countDestinations(content) {
    if (!content) return 0;
    const match = content.match(/^\s{2}-\s+id:\s*/gm);
    return match ? match.length : 0;
  }

  function renderOverview() {
    const active = state.active;
    const count = countDestinations(active?.content);
    elements.activeVersion.textContent = active ? `v${active.version}` : "—";
    elements.heroVersion.textContent = active ? `v${active.version}` : "—";
    elements.activeVersionMeta.textContent = active
      ? `${formatDate(active.publishedAt)} · ${active.publishedBy || "unknown"}`
      : state.namespace ? "当前没有已发布版本" : "等待选择命名空间";
    elements.releaseCount.textContent = String(state.releases.length);
    elements.destinationCount.textContent = String(count);
    elements.contentHash.textContent = shortHash(active?.contentHash);
    elements.contentHash.title = active?.contentHash || "";
    elements.destinationTicks.innerHTML = Array.from({ length: Math.min(Math.max(count, 1), 12) })
      .map(() => "<i></i>").join("");

    const latest = [...state.releases].reverse().slice(0, 4);
    if (!latest.length) {
      elements.recentReleases.className = "release-stream empty-state";
      elements.recentReleases.textContent = state.namespace ? "这个命名空间还没有 release" : "先创建一个命名空间";
      return;
    }
    elements.recentReleases.className = "release-stream";
    elements.recentReleases.innerHTML = latest.map((release) => `
      <article class="release-stream-item">
        <span class="stream-version">v${release.version}</span>
        <div class="stream-copy">
          <strong>${escapeHtml(release.comment || "无版本说明")}</strong>
          <small>${escapeHtml(release.createdBy || "unknown")} · ${escapeHtml(formatDate(release.createdAt))}</small>
        </div>
        <span class="status-chip ${release.status.toLowerCase()}">${statusLabel(release.status)}</span>
      </article>`).join("");
  }

  function filteredReleases() {
    const needle = state.search.trim().toLowerCase();
    return [...state.releases].reverse().filter((release) => {
      if (state.statusFilter !== "ALL" && release.status !== state.statusFilter) return false;
      if (!needle) return true;
      return [release.version, release.createdBy, release.publishedBy, release.comment, release.contentHash]
        .some((value) => String(value || "").toLowerCase().includes(needle));
    });
  }

  function renderReleases() {
    const rows = filteredReleases();
    elements.releaseEmpty.classList.toggle("is-hidden", rows.length > 0);
    elements.releaseTableBody.innerHTML = rows.map((release) => {
      const publish = roleCanPublish() && release.status === "CREATED"
        ? `<button class="mini-button signal" type="button" data-publish="${release.version}">发布</button>` : "";
      const rollback = roleCanPublish() && release.status !== "CREATED"
        ? `<button class="mini-button" type="button" data-rollback="${release.version}">回滚到此</button>` : "";
      return `<tr>
        <td><span class="release-version">v${release.version}</span></td>
        <td><span class="status-chip ${release.status.toLowerCase()}">${statusLabel(release.status)}</span></td>
        <td>${escapeHtml(release.createdBy || "unknown")}<br><small>${escapeHtml(formatDate(release.createdAt))}</small></td>
        <td><span class="hash" title="${escapeHtml(release.contentHash)}">${escapeHtml(shortHash(release.contentHash))}</span></td>
        <td>${escapeHtml(release.comment || "—")}</td>
        <td><div class="table-actions">
          <button class="mini-button" type="button" data-inspect="${release.version}">查看</button>
          <button class="mini-button" type="button" data-diff="${release.version}">对比</button>
          ${publish}${rollback}
        </div></td>
      </tr>`;
    }).join("");
  }

  function switchView(view) {
    if (view === "editor" && !roleCanEdit()) {
      toast("当前角色没有编辑与创建 release 的权限", "warning");
      return;
    }
    $$(".nav-item").forEach((item) => item.classList.toggle("is-active", item.dataset.view === view));
    $$("[data-view-panel]").forEach((panel) => panel.classList.toggle("is-active", panel.dataset.viewPanel === view));
    document.body.classList.remove("nav-open");
    if (view === "editor") prepareEditor();
    if (view === "audit") loadAudit();
    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  function prepareEditor(force = false) {
    if (force || !elements.configEditor.value.trim()) {
      elements.configEditor.value = state.active?.content || DEFAULT_CONFIG.replace("production.main.default", state.namespace || "production.main.default");
    }
    elements.releaseActor.textContent = state.actor || state.role.toLowerCase();
    elements.releaseNamespace.textContent = state.namespace || "尚未选择";
    updateEditorMetrics();
  }

  function updateEditorMetrics() {
    const editor = elements.configEditor;
    const text = editor.value;
    const before = text.slice(0, editor.selectionStart);
    const line = before.split("\n").length;
    const column = before.length - before.lastIndexOf("\n");
    const lines = text.split("\n").length;
    elements.lineNumbers.textContent = Array.from({ length: lines }, (_, index) => index + 1).join("\n");
    elements.cursorPosition.textContent = `Ln ${line}, Col ${column}`;
    elements.byteCount.textContent = `${new TextEncoder().encode(text).length.toLocaleString()} bytes`;
    elements.editorState.textContent = state.validatedContent === text ? "VALIDATED" : "UNSAVED DRAFT";
    elements.editorState.style.color = state.validatedContent === text ? "var(--success)" : "var(--amber)";
  }

  function syncEditorScroll() {
    elements.lineNumbers.scrollTop = elements.configEditor.scrollTop;
  }

  async function validateConfig(silent = false) {
    if (!state.namespace) {
      openNamespacePrompt(true);
      throw new Error("请先创建或选择命名空间");
    }
    elements.validationIcon.className = "validation-icon";
    elements.validationIcon.textContent = "…";
    elements.validationTitle.textContent = "正在验证";
    elements.validationMessage.textContent = "检查 YAML 结构、destination 约束与内联密钥策略。";
    elements.validationResults.innerHTML = "";
    try {
      const errors = await request(`/api/v1/namespaces/${encodeURIComponent(state.namespace)}/validate`, {
        method: "POST",
        body: JSON.stringify({ content: elements.configEditor.value }),
      });
      if (errors.length) {
        state.validatedContent = "";
        elements.validationIcon.textContent = "";
        elements.validationIcon.className = "validation-icon is-invalid";
        elements.validationTitle.textContent = `${errors.length} 项需要修正`;
        elements.validationMessage.textContent = "配置尚未达到 release 标准。修正后重新运行验证。";
        elements.validationResults.innerHTML = errors.map((error) => `<div class="validation-result">${escapeHtml(error)}</div>`).join("");
        updateEditorMetrics();
        if (!silent) toast("配置验证未通过", "error");
        return false;
      }
      state.validatedContent = elements.configEditor.value;
      elements.validationIcon.textContent = "";
      elements.validationIcon.className = "validation-icon is-valid";
      elements.validationTitle.textContent = "可以创建 release";
      elements.validationMessage.textContent = "结构、唯一性、出口类型、批次限制与密钥策略均已通过。";
      elements.validationResults.innerHTML = '<div class="validation-result valid">PRE-FLIGHT CHECKS PASSED</div>';
      updateEditorMetrics();
      if (!silent) toast("配置验证通过");
      return true;
    } catch (error) {
      elements.validationIcon.textContent = "";
      elements.validationIcon.className = "validation-icon is-invalid";
      elements.validationTitle.textContent = "验证请求失败";
      elements.validationMessage.textContent = error.message;
      if (!silent) toast(error.message, "error");
      throw error;
    }
  }

  async function createRelease() {
    if (!(await validateConfig(true))) {
      toast("请先修正配置错误", "error");
      return;
    }
    const release = await request(`/api/v1/namespaces/${encodeURIComponent(state.namespace)}/releases`, {
      method: "POST",
      body: JSON.stringify({ content: elements.configEditor.value, comment: elements.releaseComment.value.trim() }),
    });
    elements.releaseComment.value = "";
    await loadReleases();
    toast(`不可变版本 v${release.version} 已创建`);
    switchView("releases");
  }

  async function loadAudit() {
    try {
      state.audit = await request("/api/v1/audit");
      renderAudit();
    } catch (error) {
      elements.auditTimeline.className = "audit-timeline empty-state";
      elements.auditTimeline.textContent = error.message;
      toast(error.message, "error");
    }
  }

  function parseAudit(line) {
    const timestamp = line.match(/^([^\s]+)/)?.[1] || "";
    const field = (name) => line.match(new RegExp(`${name}=([^\\s]+)`))?.[1] || "";
    const detail = line.match(/detail=(.*)$/)?.[1] || "";
    return { timestamp, actor: field("actor"), operation: field("operation"), namespace: field("namespace"), version: field("version"), detail };
  }

  function renderAudit() {
    if (!state.audit.length) {
      elements.auditTimeline.className = "audit-timeline empty-state";
      elements.auditTimeline.textContent = "暂无审计记录";
      return;
    }
    elements.auditTimeline.className = "audit-timeline";
    elements.auditTimeline.innerHTML = [...state.audit].reverse().map((raw) => {
      const item = parseAudit(raw);
      const operation = { release: "创建了不可变版本", publish: "发布了配置版本", rollback: "发起配置回滚" }[item.operation] || item.operation;
      return `<article class="audit-item">
        <time class="audit-time">${escapeHtml(formatDate(item.timestamp))}</time>
        <span class="audit-node"><span>${escapeHtml((item.operation || "?")[0].toUpperCase())}</span></span>
        <div class="audit-copy">
          <strong>${escapeHtml(item.actor || "unknown")} ${escapeHtml(operation)}</strong>
          <p>${escapeHtml(item.detail || "配置控制动作已记录")}</p>
          <small>${escapeHtml(item.namespace)} · VERSION ${escapeHtml(item.version)}</small>
        </div>
      </article>`;
    }).join("");
  }

  function openModal({ eyebrow = "RELEASE CONTROL", title, body, actions = [] }) {
    elements.modalEyebrow.textContent = eyebrow;
    elements.modalTitle.textContent = title;
    elements.modalBody.innerHTML = body;
    elements.modalActions.innerHTML = "";
    actions.forEach((action) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = action.primary ? "primary-button" : "secondary-button";
      button.textContent = action.label;
      button.addEventListener("click", async () => {
        if (action.close !== false) closeModal();
        try { await action.run?.(); } catch (error) { toast(error.message, "error"); }
      });
      elements.modalActions.appendChild(button);
    });
    elements.modalBackdrop.classList.remove("is-hidden");
  }

  function closeModal() {
    elements.modalBackdrop.classList.add("is-hidden");
  }

  function openNamespacePrompt(required = false) {
    openModal({
      eyebrow: "ENVIRONMENT / CLUSTER / TENANT",
      title: required ? "创建第一个命名空间" : "新建命名空间",
      body: `<p>命名空间必须采用三段式小写格式，例如 <code>production.main.default</code>。</p>
        <label>命名空间<input id="newNamespaceInput" type="text" autocomplete="off" placeholder="production.main.default"></label>`,
      actions: [
        ...(!required ? [{ label: "取消", run: () => {} }] : []),
        { label: "开始配置", primary: true, close: false, run: () => {
          const input = $("#newNamespaceInput");
          const value = input.value.trim();
          if (!/^[a-z0-9-]+\.[a-z0-9-]+\.[a-z0-9-]+$/.test(value)) {
            input.focus();
            toast("命名空间格式必须是 environment.cluster.tenant", "error");
            return;
          }
          state.namespace = value;
          sessionStorage.setItem("ai-canal-namespace", value);
          if (!state.namespaces.includes(value)) state.namespaces.push(value);
          state.releases = [];
          state.active = null;
          elements.configEditor.value = DEFAULT_CONFIG.replace("production.main.default", value);
          state.validatedContent = "";
          renderAll();
          closeModal();
          switchView("editor");
        } },
      ],
    });
    window.setTimeout(() => $("#newNamespaceInput")?.focus(), 30);
  }

  function inspectRelease(version) {
    const release = state.releases.find((item) => item.version === version);
    if (!release) return;
    openModal({
      eyebrow: `${state.namespace} / VERSION ${version}`,
      title: release.comment || `配置版本 v${version}`,
      body: `<p><span class="status-chip ${release.status.toLowerCase()}">${statusLabel(release.status)}</span></p>
        <p>创建人 ${escapeHtml(release.createdBy || "unknown")} · ${escapeHtml(formatDate(release.createdAt))}</p>
        <pre class="diff-view">${escapeHtml(release.content)}</pre>`,
      actions: [{ label: "关闭", run: () => {} }],
    });
  }

  async function openDiff(version) {
    const candidates = state.releases.filter((release) => release.version !== version);
    if (!candidates.length) {
      toast("至少需要两个版本才能进行对比", "warning");
      return;
    }
    const base = state.active && state.active.version !== version ? state.active.version : candidates[candidates.length - 1].version;
    const diff = await request(`/api/v1/namespaces/${encodeURIComponent(state.namespace)}/releases/${base}/diff/${version}`);
    const decorated = escapeHtml(diff).split("\n").map((line) => {
      const type = line.startsWith("+++") || line.startsWith("---") ? "diff-meta" : line.startsWith("+") ? "diff-add" : line.startsWith("-") ? "diff-remove" : "";
      return `<span class="${type}">${line || " "}</span>`;
    }).join("\n");
    openModal({
      eyebrow: "CONFIGURATION DIFF",
      title: `v${base} → v${version}`,
      body: `<pre class="diff-view">${decorated}</pre>`,
      actions: [{ label: "关闭", run: () => {} }],
    });
  }

  function confirmPublish(version) {
    openModal({
      eyebrow: "PRODUCTION GATE",
      title: `发布版本 v${version}？`,
      body: `<p>发布后，当前 PUBLISHED 版本会变为 SUPERSEDED。Canal Server 将在下一轮轮询发现变更，并进入受控重启流程。</p><p>目标命名空间：<strong>${escapeHtml(state.namespace)}</strong></p>`,
      actions: [
        { label: "取消", run: () => {} },
        { label: "确认发布", primary: true, run: async () => {
          await request(`/api/v1/namespaces/${encodeURIComponent(state.namespace)}/releases/${version}/publish`, { method: "POST" });
          await loadReleases();
          toast(`版本 v${version} 已发布`);
        } },
      ],
    });
  }

  function confirmRollback(version) {
    openModal({
      eyebrow: "RECOVERY CONTROL",
      title: `回滚到 v${version}？`,
      body: `<p>系统不会改写历史版本，而是复制 v${version} 内容创建一个新版本并立即发布，完整保留审计链路。</p>
        <label>回滚说明<input id="rollbackComment" type="text" maxlength="500" placeholder="说明回滚原因"></label>`,
      actions: [
        { label: "取消", run: () => {} },
        { label: "确认回滚", primary: true, close: false, run: async () => {
          const comment = $("#rollbackComment").value.trim();
          if (!comment) {
            toast("请填写回滚原因", "warning");
            $("#rollbackComment").focus();
            return;
          }
          await request(`/api/v1/namespaces/${encodeURIComponent(state.namespace)}/releases/${version}/rollback`, {
            method: "POST", body: JSON.stringify({ comment }),
          });
          closeModal();
          await loadReleases();
          toast(`已基于 v${version} 创建并发布回滚版本`);
        } },
      ],
    });
  }

  function bindEvents() {
    elements.loginForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      state.token = elements.tokenInput.value.trim();
      state.actor = elements.actorInput.value.trim();
      elements.loginError.textContent = "正在验证…";
      try {
        await authenticate();
        elements.loginError.textContent = "";
        toast(`欢迎进入控制中枢，${state.actor || state.role.toLowerCase()}`);
      } catch (error) {
        elements.loginError.textContent = error.status === 401 ? "凭据无效，请检查管理 token。" : error.message;
      }
    });

    $("#revealToken").addEventListener("click", () => {
      const visible = elements.tokenInput.type === "text";
      elements.tokenInput.type = visible ? "password" : "text";
      $("#revealToken").textContent = visible ? "VIEW" : "HIDE";
    });

    $("#logoutButton").addEventListener("click", () => {
      sessionStorage.removeItem("ai-canal-token");
      state.token = "";
      elements.tokenInput.value = "";
      showLogin();
    });

    $$(".nav-item").forEach((item) => item.addEventListener("click", () => switchView(item.dataset.view)));
    $$('[data-jump]').forEach((item) => item.addEventListener("click", () => switchView(item.dataset.jump)));
    $("#menuButton").addEventListener("click", () => document.body.classList.toggle("nav-open"));
    $("#navScrim").addEventListener("click", () => document.body.classList.remove("nav-open"));

    elements.namespaceButton.addEventListener("click", () => {
      const hidden = elements.namespaceMenu.classList.toggle("is-hidden");
      elements.namespaceButton.setAttribute("aria-expanded", String(!hidden));
    });

    elements.namespaceMenu.addEventListener("click", async (event) => {
      const namespace = event.target.closest("[data-namespace]")?.dataset.namespace;
      if (namespace) {
        state.namespace = namespace;
        sessionStorage.setItem("ai-canal-namespace", namespace);
        elements.namespaceMenu.classList.add("is-hidden");
        elements.namespaceButton.setAttribute("aria-expanded", "false");
        await loadReleases();
        prepareEditor(true);
        toast(`已切换到 ${namespace}`);
      } else if (event.target.closest("[data-create-namespace]")) {
        elements.namespaceMenu.classList.add("is-hidden");
        openNamespacePrompt();
      }
    });

    document.addEventListener("click", (event) => {
      if (!event.target.closest(".context-switcher")) {
        elements.namespaceMenu.classList.add("is-hidden");
        elements.namespaceButton.setAttribute("aria-expanded", "false");
      }
    });

    elements.releaseSearch.addEventListener("input", () => {
      state.search = elements.releaseSearch.value;
      renderReleases();
    });

    $$("[data-status]").forEach((button) => button.addEventListener("click", () => {
      state.statusFilter = button.dataset.status;
      $$("[data-status]").forEach((item) => item.classList.toggle("is-active", item === button));
      renderReleases();
    }));

    elements.releaseTableBody.addEventListener("click", (event) => {
      const action = event.target.closest("button");
      if (!action) return;
      if (action.dataset.inspect) inspectRelease(Number(action.dataset.inspect));
      if (action.dataset.diff) openDiff(Number(action.dataset.diff)).catch((error) => toast(error.message, "error"));
      if (action.dataset.publish) confirmPublish(Number(action.dataset.publish));
      if (action.dataset.rollback) confirmRollback(Number(action.dataset.rollback));
    });

    elements.configEditor.addEventListener("input", () => {
      updateEditorMetrics();
      elements.validationIcon.className = "validation-icon";
      elements.validationIcon.textContent = "◇";
      elements.validationTitle.textContent = "草稿已变化";
      elements.validationMessage.textContent = "请重新运行验证，确保新内容满足发布约束。";
      elements.validationResults.innerHTML = "";
    });
    elements.configEditor.addEventListener("keyup", updateEditorMetrics);
    elements.configEditor.addEventListener("click", updateEditorMetrics);
    elements.configEditor.addEventListener("scroll", syncEditorScroll);
    elements.configEditor.addEventListener("keydown", (event) => {
      if (event.key === "Tab") {
        event.preventDefault();
        const start = elements.configEditor.selectionStart;
        const end = elements.configEditor.selectionEnd;
        elements.configEditor.setRangeText("  ", start, end, "end");
        elements.configEditor.dispatchEvent(new Event("input"));
      }
    });

    $("#loadActiveButton").addEventListener("click", () => {
      elements.configEditor.value = state.active?.content || DEFAULT_CONFIG.replace("production.main.default", state.namespace || "production.main.default");
      state.validatedContent = "";
      updateEditorMetrics();
      toast(state.active ? `已载入 v${state.active.version}` : "已载入安全配置模板");
    });
    $("#validateButton").addEventListener("click", () => validateConfig().catch(() => {}));
    $("#createReleaseButton").addEventListener("click", () => createRelease().catch((error) => toast(error.message, "error")));
    $("#refreshAuditButton").addEventListener("click", loadAudit);

    $("#modalClose").addEventListener("click", closeModal);
    elements.modalBackdrop.addEventListener("click", (event) => {
      if (event.target === elements.modalBackdrop) closeModal();
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        closeModal();
        document.body.classList.remove("nav-open");
      }
    });
  }

  async function bootstrap() {
    bindEvents();
    elements.configEditor.value = DEFAULT_CONFIG;
    updateEditorMetrics();
    if (!state.token) {
      showLogin();
      return;
    }
    try {
      await authenticate();
    } catch (error) {
      sessionStorage.removeItem("ai-canal-token");
      state.token = "";
      showLogin(error.status === 401 ? "会话已失效，请重新输入管理 token。" : error.message);
    }
  }

  bootstrap();
})();
