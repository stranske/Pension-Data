const DEFAULT_CONFIG_PATH = "./config/default.json";
const RUNTIME_CONFIG_PATH = "./config/runtime.json";
const WORKSPACE_DATA_PATH = "./data/workspace.json";
const SAVED_VIEWS_KEY = "pension-data.saved-views.v1";
const REQUIRED_CONFIG_KEYS = ["environment", "apiBaseUrl", "artifactBaseUrl"];

const state = {
  config: null,
  datasets: [],
  selectedDatasetId: "",
  selectedRowIndex: null,
  filters: {
    entity: "",
    period: "",
    family: "",
    source: "",
    minConfidence: 0,
  },
  savedViews: {},
};

function normalizeText(value) {
  return typeof value === "string" ? value.trim() : "";
}

function normalizeLower(value) {
  return normalizeText(value).toLowerCase();
}

async function loadJson(path) {
  const response = await fetch(path, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`failed to load resource: ${path}`);
  }
  return response.json();
}

function assertConfig(config) {
  for (const key of REQUIRED_CONFIG_KEYS) {
    if (!normalizeText(config[key])) {
      throw new Error(`missing required config key: ${key}`);
    }
  }
}

function applyQueryOverrides(config) {
  const isLocalHost = ["localhost", "127.0.0.1"].includes(window.location.hostname);
  const isLocalContext = window.location.protocol === "file:" || isLocalHost;
  const allowOverrides = config.enableQueryOverrides === true || isLocalContext;
  if (!allowOverrides) {
    return { ...config };
  }
  const params = new URLSearchParams(window.location.search);
  const next = { ...config };
  for (const key of REQUIRED_CONFIG_KEYS) {
    const override = normalizeText(params.get(key));
    if (override) {
      next[key] = override;
    }
  }
  return next;
}

function loadSavedViews() {
  const raw = localStorage.getItem(SAVED_VIEWS_KEY);
  if (!raw) {
    return {};
  }
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function persistSavedViews() {
  localStorage.setItem(SAVED_VIEWS_KEY, JSON.stringify(state.savedViews));
}

function selectedDataset() {
  return state.datasets.find((dataset) => dataset.id === state.selectedDatasetId) || null;
}

function filteredRows() {
  const dataset = selectedDataset();
  if (!dataset) {
    return [];
  }
  return dataset.rows.filter((row) => {
    const entity = normalizeLower(row.entity);
    const period = normalizeLower(row.plan_period);
    const family = normalizeLower(row.metric_family);
    const source = normalizeLower(row.provenance?.source_document);
    const confidence = Number(row.confidence || 0);

    if (state.filters.entity && !entity.includes(normalizeLower(state.filters.entity))) {
      return false;
    }
    if (state.filters.period && !period.includes(normalizeLower(state.filters.period))) {
      return false;
    }
    if (state.filters.family && !family.includes(normalizeLower(state.filters.family))) {
      return false;
    }
    if (state.filters.source && !source.includes(normalizeLower(state.filters.source))) {
      return false;
    }
    if (confidence < state.filters.minConfidence) {
      return false;
    }
    return true;
  });
}

function renderMeta() {
  const environment = document.querySelector("[data-testid='environment-badge']");
  const api = document.getElementById("api-endpoint");
  const artifact = document.getElementById("artifact-endpoint");

  environment.textContent = `Environment: ${state.config.environment}`;
  api.textContent = state.config.apiBaseUrl;
  artifact.textContent = state.config.artifactBaseUrl;
}

function renderInventory() {
  const list = document.getElementById("inventory-list");
  const meta = document.getElementById("dataset-meta");
  list.innerHTML = "";

  for (const dataset of state.datasets) {
    const item = document.createElement("li");
    item.className = `inventory-item${dataset.id === state.selectedDatasetId ? " active" : ""}`;
    item.innerHTML = `
      <div class="inventory-title">${dataset.name}</div>
      <div class="inventory-meta">${dataset.domain} · ${dataset.kind}</div>
      <div class="inventory-meta">${dataset.rows.length} rows · ${dataset.freshness}</div>
    `;
    item.addEventListener("click", () => {
      state.selectedDatasetId = dataset.id;
      state.selectedRowIndex = null;
      renderWorkspace();
    });
    list.appendChild(item);
  }

  const dataset = selectedDataset();
  if (!dataset) {
    meta.textContent = "No dataset selected.";
    return;
  }
  meta.textContent = `Selected: ${dataset.name} · last updated ${dataset.lastUpdated}`;
}

function currentColumns(rows) {
  if (!rows.length) {
    return ["entity", "plan_period", "metric_family", "metric", "value", "confidence"];
  }
  const preferred = ["entity", "plan_period", "metric_family", "metric", "value", "confidence"];
  return preferred.filter((column) => column in rows[0]);
}

function renderTable() {
  const rows = filteredRows();
  const columns = currentColumns(rows);
  const tableHead = document.getElementById("table-head");
  const tableBody = document.getElementById("table-body");
  const count = document.getElementById("result-count");

  tableHead.innerHTML = `<tr>${columns.map((column) => `<th>${column}</th>`).join("")}</tr>`;
  tableBody.innerHTML = "";
  count.textContent = `${rows.length} rows`;

  if (!rows.length) {
    const empty = document.createElement("tr");
    empty.innerHTML = `<td colspan="${columns.length}">No records match current filters.</td>`;
    tableBody.appendChild(empty);
    return;
  }

  rows.forEach((row, index) => {
    const tr = document.createElement("tr");
    if (state.selectedRowIndex === index) {
      tr.classList.add("active-row");
    }
    tr.innerHTML = columns
      .map((column) => `<td>${row[column] !== undefined ? String(row[column]) : ""}</td>`)
      .join("");
    tr.addEventListener("click", () => {
      state.selectedRowIndex = index;
      renderTable();
      renderDetail();
    });
    tableBody.appendChild(tr);
  });
}

function renderDetail() {
  const detail = document.getElementById("detail-content");
  const rows = filteredRows();
  if (state.selectedRowIndex === null || !rows[state.selectedRowIndex]) {
    detail.textContent = "Select a row to inspect details.";
    return;
  }
  const row = rows[state.selectedRowIndex];
  const provenance = row.provenance || {};
  const sourceDocument = normalizeText(provenance.source_document);
  const evidenceRefs = Array.isArray(provenance.evidence_refs) ? provenance.evidence_refs : [];

  const evidenceLinks = evidenceRefs
    .map((ref) => {
      const token = normalizeText(ref);
      if (!token) {
        return "";
      }
      const href = `${state.config.artifactBaseUrl}/${encodeURIComponent(sourceDocument)}#${encodeURIComponent(token)}`;
      return `<a href="${href}" target="_blank" rel="noreferrer">${token}</a>`;
    })
    .filter(Boolean)
    .join(" · ");

  detail.innerHTML = `
    <ul class="detail-list">
      <li>
        <div class="detail-key">Entity</div>
        <div class="detail-value">${row.entity ?? ""}</div>
      </li>
      <li>
        <div class="detail-key">Plan Period</div>
        <div class="detail-value">${row.plan_period ?? ""}</div>
      </li>
      <li>
        <div class="detail-key">Metric Family / Metric</div>
        <div class="detail-value">${row.metric_family ?? ""} / ${row.metric ?? ""}</div>
      </li>
      <li>
        <div class="detail-key">Value / Confidence</div>
        <div class="detail-value">${row.value ?? ""} / ${row.confidence ?? ""}</div>
      </li>
      <li>
        <div class="detail-key">Source Document</div>
        <div class="detail-value">${sourceDocument || "n/a"}</div>
      </li>
      <li>
        <div class="detail-key">Evidence References</div>
        <div class="detail-value">${evidenceLinks || "n/a"}</div>
      </li>
    </ul>
  `;
}

function applyFilterInputs() {
  state.filters.entity = document.getElementById("filter-entity").value;
  state.filters.period = document.getElementById("filter-period").value;
  state.filters.family = document.getElementById("filter-family").value;
  state.filters.source = document.getElementById("filter-source").value;
  state.filters.minConfidence = Number(document.getElementById("filter-confidence").value);
  document.getElementById("filter-confidence-value").textContent = state.filters.minConfidence.toFixed(2);
  state.selectedRowIndex = null;
  renderTable();
  renderDetail();
}

function bindFilterHandlers() {
  const ids = ["filter-entity", "filter-period", "filter-family", "filter-source", "filter-confidence"];
  ids.forEach((id) => {
    document.getElementById(id).addEventListener("input", applyFilterInputs);
  });

  document.getElementById("clear-filters").addEventListener("click", () => {
    document.getElementById("filter-entity").value = "";
    document.getElementById("filter-period").value = "";
    document.getElementById("filter-family").value = "";
    document.getElementById("filter-source").value = "";
    document.getElementById("filter-confidence").value = "0";
    applyFilterInputs();
  });
}

function renderSavedViews() {
  const select = document.getElementById("saved-view-select");
  select.innerHTML = '<option value="">Select a saved view</option>';
  for (const name of Object.keys(state.savedViews).sort()) {
    const option = document.createElement("option");
    option.value = name;
    option.textContent = name;
    select.appendChild(option);
  }
}

function bindSavedViewHandlers() {
  const select = document.getElementById("saved-view-select");

  document.getElementById("save-view").addEventListener("click", () => {
    const name = normalizeText(window.prompt("Save current view as", "analyst-default"));
    if (!name) {
      return;
    }
    state.savedViews[name] = {
      datasetId: state.selectedDatasetId,
      filters: { ...state.filters },
    };
    persistSavedViews();
    renderSavedViews();
    select.value = name;
  });

  document.getElementById("load-view").addEventListener("click", () => {
    const selected = normalizeText(select.value);
    if (!selected || !state.savedViews[selected]) {
      return;
    }
    const view = state.savedViews[selected];
    state.selectedDatasetId = view.datasetId || state.datasets[0]?.id || "";
    state.filters = {
      entity: view.filters?.entity || "",
      period: view.filters?.period || "",
      family: view.filters?.family || "",
      source: view.filters?.source || "",
      minConfidence: Number(view.filters?.minConfidence || 0),
    };

    document.getElementById("filter-entity").value = state.filters.entity;
    document.getElementById("filter-period").value = state.filters.period;
    document.getElementById("filter-family").value = state.filters.family;
    document.getElementById("filter-source").value = state.filters.source;
    document.getElementById("filter-confidence").value = String(state.filters.minConfidence);

    renderWorkspace();
  });

  document.getElementById("delete-view").addEventListener("click", () => {
    const selected = normalizeText(select.value);
    if (!selected || !state.savedViews[selected]) {
      return;
    }
    delete state.savedViews[selected];
    persistSavedViews();
    renderSavedViews();
  });
}

function flattenRows(rows) {
  return rows.map((row) => {
    const provenance = row.provenance || {};
    return {
      entity: row.entity,
      plan_period: row.plan_period,
      metric_family: row.metric_family,
      metric: row.metric,
      value: row.value,
      confidence: row.confidence,
      source_document: provenance.source_document || "",
      evidence_refs: Array.isArray(provenance.evidence_refs)
        ? provenance.evidence_refs.join(" | ")
        : "",
    };
  });
}

function buildCsv(rows) {
  if (!rows.length) {
    return "";
  }
  const columns = Object.keys(rows[0]);
  const escape = (value) => {
    const text = value === null || value === undefined ? "" : String(value);
    if (text.includes(",") || text.includes("\n") || text.includes('"')) {
      return `"${text.replace(/"/g, '""')}"`;
    }
    return text;
  };
  const lines = [columns.join(",")];
  for (const row of rows) {
    lines.push(columns.map((column) => escape(row[column])).join(","));
  }
  return `${lines.join("\n")}\n`;
}

function downloadFile(filename, content, mimeType) {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  URL.revokeObjectURL(url);
}

function bindExportHandlers() {
  document.getElementById("export-json").addEventListener("click", () => {
    const rows = flattenRows(filteredRows());
    downloadFile("pension-data-filtered.json", `${JSON.stringify(rows, null, 2)}\n`, "application/json");
  });

  document.getElementById("export-csv").addEventListener("click", () => {
    const rows = flattenRows(filteredRows());
    downloadFile("pension-data-filtered.csv", buildCsv(rows), "text/csv");
  });
}

function renderWorkspace() {
  renderMeta();
  renderInventory();
  applyFilterInputs();
  renderSavedViews();
}

async function init() {
  const [defaultConfig, runtimeConfig, workspace] = await Promise.all([
    loadJson(DEFAULT_CONFIG_PATH),
    loadJson(RUNTIME_CONFIG_PATH).catch(() => ({})),
    loadJson(WORKSPACE_DATA_PATH),
  ]);

  const config = applyQueryOverrides({ ...defaultConfig, ...runtimeConfig });
  assertConfig(config);

  const datasets = Array.isArray(workspace.datasets) ? workspace.datasets : [];
  if (!datasets.length) {
    throw new Error("workspace dataset inventory is empty");
  }

  state.config = config;
  state.datasets = datasets;
  state.selectedDatasetId = datasets[0].id;
  state.savedViews = loadSavedViews();

  bindFilterHandlers();
  bindSavedViewHandlers();
  bindExportHandlers();
  renderWorkspace();

  window.PensionDataApp = {
    config: state.config,
    datasetCount: state.datasets.length,
    getFilteredRows: () => filteredRows(),
  };
}

init().catch((error) => {
  const badge = document.querySelector("[data-testid='environment-badge']");
  if (badge) {
    badge.textContent = `Configuration error: ${error.message}`;
  }
  throw error;
});
