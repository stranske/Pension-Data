import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const modulePath = fileURLToPath(import.meta.url);
const root = path.resolve(path.dirname(modulePath), "..");
const contractPath = path.resolve(root, "..", "contracts", "runtime-contract.json");
const workspacePath = path.resolve(root, "..", "web", "data", "workspace.json");

export function validateRuntimeContract(workspace, contract) {
  if (workspace.contractVersion !== contract.version) {
    throw new Error(
      `workspace contractVersion '${workspace.contractVersion}' does not match contract version '${contract.version}'`
    );
  }

  for (const required of contract.workspaceBundle.requiredTopLevelFields) {
    if (!(required in workspace)) {
      throw new Error(`workspace missing required top-level field '${required}'`);
    }
  }

  if (!contract.workspaceBundle.dataOrigins.includes(workspace.data_origin)) {
    throw new Error(`workspace data_origin '${workspace.data_origin}' is not allowed`);
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
    const requiredProv = contract.workspaceBundle.provenanceRequiredFields || [
      "source_document",
      "evidence_refs",
    ];
    const optionalProv = contract.workspaceBundle.provenanceOptionalFields || ["source_locator"];
    const allowedProv = new Set([...requiredProv, ...optionalProv]);
    for (const [rowIndex, row] of dataset.rows.entries()) {
      if (!row || typeof row !== "object" || !("provenance" in row)) {
        continue;
      }
      const provenance = row.provenance;
      if (!provenance || typeof provenance !== "object" || Array.isArray(provenance)) {
        throw new Error(`dataset ${index} row ${rowIndex} provenance must be an object`);
      }
      for (const reqField of requiredProv) {
        if (!(reqField in provenance)) {
          throw new Error(`dataset ${index} row ${rowIndex} provenance missing '${reqField}'`);
        }
      }
      for (const key of Object.keys(provenance)) {
        if (!allowedProv.has(key)) {
          throw new Error(
            `dataset ${index} row ${rowIndex} provenance has unsupported field '${key}'`
          );
        }
      }
      if (
        typeof provenance.source_document !== "string" ||
        provenance.source_document.trim().length === 0
      ) {
        throw new Error(
          `dataset ${index} row ${rowIndex} provenance.source_document must be a non-empty string`
        );
      }
      if (
        !Array.isArray(provenance.evidence_refs) ||
        provenance.evidence_refs.some((ref) => typeof ref !== "string" || ref.trim().length === 0)
      ) {
        throw new Error(
          `dataset ${index} row ${rowIndex} provenance.evidence_refs must be an array of non-empty strings`
        );
      }
      if ("source_locator" in provenance) {
        const locator = provenance.source_locator;
        if (
          typeof locator !== "string" ||
          !locator.trim() ||
          locator.startsWith("/") ||
          locator.endsWith("/") ||
          locator.includes("\\") ||
          locator.includes("\0") ||
          locator.includes("://") ||
          locator
            .split("/")
            .some(
              (segment) => !segment || segment === "." || segment === ".." || segment.includes(":")
            )
        ) {
          throw new Error(`dataset ${index} row ${rowIndex} provenance.source_locator is invalid`);
        }
      }
    }
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === modulePath) {
  const contract = JSON.parse(fs.readFileSync(contractPath, "utf8"));
  const workspace = JSON.parse(fs.readFileSync(workspacePath, "utf8"));
  validateRuntimeContract(workspace, contract);
  console.log("Runtime contract validation passed.");
}
