import { createHash } from "node:crypto";
import { execSync, spawnSync } from "node:child_process";
import { existsSync, promises as fs } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const sourcesDir = join(root, "diagrams");
const outputDir = join(root, "docs", "assets", "diagrams");
const config = join(sourcesDir, "mermaid-config.json");
const manifestPath = join(outputDir, "manifest.json");
const version = "12.2.0";
const check = process.argv.includes("--check");
const digest = (data) => createHash("sha256").update(data).digest("hex");
const textHash = async (path) =>
  digest((await fs.readFile(path, "utf8")).replace(/\r\n/g, "\n"));

const names = (await fs.readdir(sourcesDir))
  .filter((name) => name.endsWith(".mmd"))
  .sort();
if (names.length !== 10) throw new Error("Exactly ten Mermaid sources are required.");
if (!check) await fs.mkdir(outputDir, { recursive: true });

const docs = await fs.readFile(join(root, "docs", "architecture", "diagrams.md"), "utf8");
const sourceHashes = Object.fromEntries(
  await Promise.all(names.map(async (name) => [name, await textHash(join(sourcesDir, name))])),
);
const expectedImages = names.map((name) => `${name.slice(0, -4)}.png`);
const outputNames = (await fs.readdir(outputDir)).filter((name) => name.endsWith(".png")).sort();
if (outputNames.some((name) => !expectedImages.includes(name))) {
  throw new Error("An output PNG has no matching Mermaid source.");
}
for (const name of expectedImages) {
  if (!docs.includes(`assets/diagrams/${name}`)) {
    throw new Error(`Diagram is not referenced in documentation: ${name}`);
  }
}

if (!check) {
  let globalRoot;
  const configuredPrefix = process.env.npm_config_prefix || process.env.NPM_CONFIG_PREFIX;
  if (configuredPrefix) {
    globalRoot =
      process.platform === "win32"
        ? join(configuredPrefix, "node_modules")
        : join(configuredPrefix, "lib", "node_modules");
  } else {
    try {
      globalRoot = execSync("npm root --global", { encoding: "utf8", stdio: "pipe" }).trim();
    } catch (error) {
      throw new Error("Could not locate global npm packages; ensure npm is available.", {
        cause: error,
      });
    }
    if (globalRoot.includes("***")) {
      throw new Error("npm redacted its global path; set npm_config_prefix explicitly.");
    }
  }
  const packagePath = join(globalRoot, "@mermaid-js", "mermaid-cli");
  const installCommand = `npm install --global --ignore-scripts @mermaid-js/mermaid-cli@${version}`;
  if (!existsSync(join(packagePath, "package.json"))) {
    throw new Error(`Mermaid CLI ${version} is required. Install it with: ${installCommand}`);
  }
  const installed = JSON.parse(await fs.readFile(join(packagePath, "package.json"), "utf8")).version;
  if (installed !== version) {
    throw new Error(`Mermaid CLI ${version} is required; found ${installed}. Run: ${installCommand}`);
  }
  const cli = join(packagePath, "src", "cli.js");
  for (const name of names) {
    const result = spawnSync(
      process.execPath,
      [
        cli,
        "-i",
        join(sourcesDir, name),
        "-o",
        join(outputDir, `${name.slice(0, -4)}.png`),
        "-c",
        config,
        "-b",
        "white",
        "-s",
        "2",
      ],
      { cwd: root, stdio: "inherit", env: process.env },
    );
    if (result.error) throw result.error;
    if (result.status !== 0) throw new Error(`Rendering failed: ${name}`);
  }
}

const imageHashes = {};
for (const name of expectedImages) {
  const bytes = await fs.readFile(join(outputDir, name));
  if (
    bytes.length < 24 ||
    !bytes.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10])) ||
    bytes.readUInt32BE(16) === 0 ||
    bytes.readUInt32BE(20) === 0
  ) {
    throw new Error(`Invalid PNG: ${name}`);
  }
  imageHashes[name] = digest(bytes);
}
const manifest = {
  renderer: `@mermaid-js/mermaid-cli@${version}`,
  options: { background: "white", scale: 2 },
  config: await textHash(config),
  sources: sourceHashes,
  images: imageHashes,
};
if (check) {
  const actual = JSON.parse(await fs.readFile(manifestPath, "utf8"));
  if (JSON.stringify(actual) !== JSON.stringify(manifest)) {
    throw new Error("Diagram sources, configuration, or rendered assets are stale or changed.");
  }
  console.log(`Validated ${names.length} diagram sources and PNGs.`);
} else {
  await fs.writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
}
