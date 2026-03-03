const DEFAULT_CONFIG_PATH = "./config/default.json";
const RUNTIME_CONFIG_PATH = "./config/runtime.json";

const REQUIRED_CONFIG_KEYS = ["environment", "apiBaseUrl", "artifactBaseUrl"];

async function loadJsonConfig(path) {
  const response = await fetch(path, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`failed to load config: ${path}`);
  }
  return response.json();
}

function applyQueryOverrides(config) {
  const params = new URLSearchParams(window.location.search);
  const next = { ...config };
  for (const key of REQUIRED_CONFIG_KEYS) {
    const override = params.get(key);
    if (override && override.trim()) {
      next[key] = override.trim();
    }
  }
  return next;
}

function assertRequiredConfig(config) {
  for (const key of REQUIRED_CONFIG_KEYS) {
    const value = config[key];
    if (typeof value !== "string" || !value.trim()) {
      throw new Error(`missing required config key: ${key}`);
    }
  }
}

function renderConfig(config) {
  const environment = document.querySelector("[data-testid='environment-badge']");
  const api = document.getElementById("api-endpoint");
  const artifact = document.getElementById("artifact-endpoint");

  environment.textContent = `Environment: ${config.environment}`;
  api.textContent = config.apiBaseUrl;
  artifact.textContent = config.artifactBaseUrl;
}

async function init() {
  const [defaultConfig, runtimeConfig] = await Promise.all([
    loadJsonConfig(DEFAULT_CONFIG_PATH),
    loadJsonConfig(RUNTIME_CONFIG_PATH).catch(() => ({})),
  ]);
  const merged = applyQueryOverrides({ ...defaultConfig, ...runtimeConfig });
  assertRequiredConfig(merged);
  renderConfig(merged);
  window.PensionDataApp = { config: merged };
}

init().catch((error) => {
  const environment = document.querySelector("[data-testid='environment-badge']");
  if (environment) {
    environment.textContent = `Configuration error: ${error.message}`;
  }
  throw error;
});
