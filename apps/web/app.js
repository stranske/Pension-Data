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
  storageWarningShown: false,
  currentChartSpec: null,
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

function numeric(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
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
  try {
    localStorage.setItem(SAVED_VIEWS_KEY, JSON.stringify(state.savedViews));
    return true;
  } catch (error) {
    console.warn("Unable to persist saved views.", error);
    if (!state.storageWarningShown) {
      state.storageWarningShown = true;
      window.alert(
        "Unable to save views in local storage. Browser storage appears unavailable."
      );
    }
    return false;
  }
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
  list.textContent = "";

  for (const dataset of state.datasets) {
    const item = document.createElement("li");
    item.className = `inventory-item${dataset.id === state.selectedDatasetId ? " active" : ""}`;
    const button = document.createElement("button");
    button.type = "button";
    button.className = "inventory-button";

    const title = document.createElement("div");
    title.className = "inventory-title";
    title.textContent = String(dataset.name ?? "");

    const metaLine = document.createElement("div");
    metaLine.className = "inventory-meta";
    metaLine.textContent = `${String(dataset.domain ?? "")} · ${String(dataset.kind ?? "")}`;

    const countLine = document.createElement("div");
    countLine.className = "inventory-meta";
    countLine.textContent = `${dataset.rows.length} rows · ${String(dataset.freshness ?? "")}`;

    const selectDataset = () => {
      state.selectedDatasetId = dataset.id;
      state.selectedRowIndex = null;
      renderWorkspace();
    };

    button.addEventListener("click", selectDataset);
    button.append(title, metaLine, countLine);
    item.appendChild(button);
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

  tableHead.textContent = "";
  const headerRow = document.createElement("tr");
  for (const column of columns) {
    const th = document.createElement("th");
    th.textContent = column;
    headerRow.appendChild(th);
  }
  tableHead.appendChild(headerRow);
  tableBody.textContent = "";
  count.textContent = `${rows.length} rows`;

  if (!rows.length) {
    const empty = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = columns.length;
    cell.textContent = "No records match current filters.";
    empty.appendChild(cell);
    tableBody.appendChild(empty);
    return;
  }

  const activateRow = (index) => {
    state.selectedRowIndex = index;
    renderTable();
    renderDetail();
  };

  rows.forEach((row, index) => {
    const tr = document.createElement("tr");
    tr.tabIndex = 0;
    tr.setAttribute("role", "button");
    tr.setAttribute("aria-selected", state.selectedRowIndex === index ? "true" : "false");
    if (state.selectedRowIndex === index) {
      tr.classList.add("active-row");
    }
    for (const column of columns) {
      const td = document.createElement("td");
      td.textContent = row[column] !== undefined ? String(row[column]) : "";
      tr.appendChild(td);
    }
    tr.addEventListener("click", () => activateRow(index));
    tr.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " " || event.key === "Spacebar") {
        event.preventDefault();
        activateRow(index);
      }
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
  detail.textContent = "";
  const list = document.createElement("ul");
  list.className = "detail-list";

  const addDetailItem = (label, valueContent) => {
    const item = document.createElement("li");
    const key = document.createElement("div");
    key.className = "detail-key";
    key.textContent = label;
    const value = document.createElement("div");
    value.className = "detail-value";
    if (typeof valueContent === "string") {
      value.textContent = valueContent;
    } else if (valueContent instanceof Node) {
      value.appendChild(valueContent);
    }
    item.append(key, value);
    list.appendChild(item);
  };

  addDetailItem("Entity", String(row.entity ?? ""));
  addDetailItem("Plan Period", String(row.plan_period ?? ""));
  addDetailItem("Metric Family / Metric", `${String(row.metric_family ?? "")} / ${String(row.metric ?? "")}`);
  addDetailItem("Value / Confidence", `${String(row.value ?? "")} / ${String(row.confidence ?? "")}`);
  addDetailItem("Source Document", sourceDocument || "n/a");

  const evidenceTokens = evidenceRefs.map((ref) => normalizeText(ref)).filter(Boolean);
  if (!evidenceTokens.length || !sourceDocument) {
    addDetailItem("Evidence References", evidenceTokens.join(" · ") || "n/a");
  } else {
    const fragment = document.createDocumentFragment();
    evidenceTokens.forEach((token, index) => {
      const link = document.createElement("a");
      link.href = `${state.config.artifactBaseUrl}/${encodeURIComponent(sourceDocument)}#${encodeURIComponent(token)}`;
      link.target = "_blank";
      link.rel = "noreferrer";
      link.textContent = token;
      fragment.appendChild(link);
      if (index < evidenceTokens.length - 1) {
        fragment.appendChild(document.createTextNode(" · "));
      }
    });
    addDetailItem("Evidence References", fragment);
  }

  detail.appendChild(list);
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
  buildChartFromTemplate();
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
  const anchor = document.createElement("a");
  anchor.download = filename;
  let objectUrl = null;
  if (typeof content === "string" && content.startsWith("data:")) {
    anchor.href = content;
  } else {
    const blob = new Blob([content], { type: mimeType });
    objectUrl = URL.createObjectURL(blob);
    anchor.href = objectUrl;
  }
  anchor.click();
  if (objectUrl) {
    URL.revokeObjectURL(objectUrl);
  }
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

function chartTitle(template) {
  const labels = {
    timeSeries: "Time Series",
    distribution: "Distribution",
    attribution: "Attribution",
    riskReturn: "Risk / Return Scatter",
    heatmap: "Heatmap",
  };
  return labels[template] || "Chart";
}

function chartTemplateSpec(template, rows) {
  if (!rows.length) {
    return {
      data: [],
      layout: {
        title: "No rows available for chart",
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
      },
    };
  }

  if (template === "distribution") {
    return {
      data: [
        {
          type: "histogram",
          x: rows.map((row) => numeric(row.value)),
          marker: { color: "#0f766e" },
        },
      ],
      layout: {
        title: `${chartTitle(template)} · ${rows.length} rows`,
        xaxis: { title: "Value" },
        yaxis: { title: "Count" },
      },
    };
  }

  if (template === "attribution") {
    const byEntity = {};
    rows.forEach((row) => {
      byEntity[row.entity] = (byEntity[row.entity] || 0) + numeric(row.value);
    });
    return {
      data: [
        {
          type: "bar",
          x: Object.keys(byEntity),
          y: Object.values(byEntity),
          marker: { color: "#f06f42" },
        },
      ],
      layout: {
        title: `${chartTitle(template)} · by Entity`,
        xaxis: { title: "Entity" },
        yaxis: { title: "Aggregate Value" },
      },
    };
  }

  if (template === "riskReturn") {
    return {
      data: [
        {
          type: "scatter",
          mode: "markers",
          x: rows.map((row) => numeric(row.confidence)),
          y: rows.map((row) => numeric(row.value)),
          text: rows.map((row) => `${row.entity} · ${row.metric}`),
          marker: {
            size: rows.map((row) => Math.max(8, numeric(row.confidence) * 22)),
            color: rows.map((row) => numeric(row.confidence)),
            colorscale: "Viridis",
          },
        },
      ],
      layout: {
        title: `${chartTitle(template)} · Confidence vs Value`,
        xaxis: { title: "Confidence" },
        yaxis: { title: "Value" },
      },
    };
  }

  if (template === "heatmap") {
    const entities = [...new Set(rows.map((row) => row.entity))];
    const families = [...new Set(rows.map((row) => row.metric_family))];
    const z = entities.map((entity) =>
      families.map((family) => {
        const scoped = rows.filter((row) => row.entity === entity && row.metric_family === family);
        if (!scoped.length) {
          return 0;
        }
        return scoped.reduce((sum, row) => sum + numeric(row.value), 0) / scoped.length;
      })
    );
    return {
      data: [
        {
          type: "heatmap",
          x: families,
          y: entities,
          z,
          colorscale: "YlGnBu",
        },
      ],
      layout: {
        title: `${chartTitle(template)} · Entity x Metric Family`,
      },
    };
  }

  if (template === "timeSeries") {
    const entityGroups = {};
    rows.forEach((row) => {
      const key = row.entity || "Unknown";
      if (!entityGroups[key]) {
        entityGroups[key] = [];
      }
      entityGroups[key].push(row);
    });
    const traces = Object.entries(entityGroups).map(([entity, values]) => {
      const sorted = [...values].sort((left, right) =>
        String(left.plan_period).localeCompare(String(right.plan_period))
      );
      return {
        type: "scatter",
        mode: "lines+markers",
        name: entity,
        x: sorted.map((row) => row.plan_period),
        y: sorted.map((row) => numeric(row.value)),
      };
    });
    return {
      data: traces,
      layout: {
        title: `${chartTitle(template)} · by Plan Period`,
        xaxis: { title: "Plan Period" },
        yaxis: { title: "Value" },
      },
    };
  }

  return { data: [], layout: { title: "Unsupported chart template" } };
}

function normalizeChartSpec(spec) {
  const baseLayout = {
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: {
      family: "Space Grotesk, IBM Plex Sans, Segoe UI, sans-serif",
      color: "#122321",
    },
    margin: { l: 55, r: 24, t: 52, b: 52 },
  };
  return {
    data: Array.isArray(spec.data) ? spec.data : [],
    layout: { ...baseLayout, ...(spec.layout || {}) },
  };
}

function renderChartSpec(spec) {
  const normalized = normalizeChartSpec(spec);
  state.currentChartSpec = normalized;
  document.getElementById("chart-spec").value = `${JSON.stringify(normalized, null, 2)}\n`;
  window.Plotly.react("chart-preview", normalized.data, normalized.layout, {
    responsive: true,
    displaylogo: false,
  });
}

function buildChartFromTemplate() {
  const template = document.getElementById("chart-template").value;
  renderChartSpec(chartTemplateSpec(template, filteredRows()));
}

function bindChartStudio() {
  document.getElementById("chart-build").addEventListener("click", buildChartFromTemplate);
  document.getElementById("chart-template").addEventListener("change", buildChartFromTemplate);
  document.getElementById("chart-apply-spec").addEventListener("click", () => {
    const raw = document.getElementById("chart-spec").value;
    const spec = JSON.parse(raw);
    renderChartSpec(spec);
  });
  document.getElementById("chart-export-json").addEventListener("click", () => {
    const spec = state.currentChartSpec || { data: [], layout: {} };
    downloadFile("pension-data-chart-spec.json", `${JSON.stringify(spec, null, 2)}\n`, "application/json");
  });
  document.getElementById("chart-export-png").addEventListener("click", async () => {
    const dataUrl = await window.Plotly.toImage("chart-preview", {
      format: "png",
      width: 1400,
      height: 840,
      scale: 2,
    });
    downloadFile("pension-data-chart.png", dataUrl, "image/png");
  });
  document.getElementById("chart-export-svg").addEventListener("click", async () => {
    const dataUrl = await window.Plotly.toImage("chart-preview", {
      format: "svg",
      width: 1400,
      height: 840,
    });
    downloadFile("pension-data-chart.svg", dataUrl, "image/svg+xml");
  });
  document.getElementById("chart-export-html").addEventListener("click", () => {
    const spec = state.currentChartSpec || { data: [], layout: {} };
    const html = `<!doctype html>
<html lang=\"en\">
  <head>
    <meta charset=\"UTF-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
    <title>Pension-Data Chart Export</title>
    <script src=\"https://cdn.plot.ly/plotly-2.35.2.min.js\"></script>
  </head>
  <body>
    <div id=\"chart\" style=\"width:100%;height:100vh;\"></div>
    <script>
      const spec = ${JSON.stringify(spec)};
      Plotly.newPlot(\"chart\", spec.data, spec.layout, { responsive: true });
    </script>
  </body>
</html>
`;
    downloadFile("pension-data-chart.html", html, "text/html");
  });
}

function renderWorkspace() {
  renderMeta();
  renderInventory();
  applyFilterInputs();
  renderSavedViews();
  buildChartFromTemplate();
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
  bindChartStudio();
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
    badge.textContent = `Initialization error: ${error.message}`;
  }
  throw error;
});
