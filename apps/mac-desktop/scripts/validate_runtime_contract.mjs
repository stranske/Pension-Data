import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const contractPath = path.resolve(root, "..", "contracts", "runtime-contract.json");
const workspacePath = path.resolve(root, "..", "web", "data", "workspace.json");

const contract = JSON.parse(fs.readFileSync(contractPath, "utf8"));
const workspace = JSON.parse(fs.readFileSync(workspacePath, "utf8"));

if (workspace.contractVersion !== contract.version) {
  throw new Error(
    `workspace contractVersion '${workspace.contractVersion}' does not match contract version '${contract.version}'`
  );
}

if (!Array.isArray(workspace.datasets) || workspace.datasets.length === 0) {
  throw new Error("workspace datasets must be a non-empty array");
}

for (const [index, dataset] of workspace.datasets.entries()) {
  for (const required of contract.workspaceBundle.datasetRequiredFields) {
    if (!(required in dataset)) {
      throw new Error(`dataset ${index} missing required field '${required}'`);
    }
  }
  if (!Array.isArray(dataset.rows)) {
    throw new Error(`dataset ${index} has non-array rows`);
  }
}

console.log("Runtime contract validation passed.");
