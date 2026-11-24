# Install and evaluate agent skills

Each skill is a directory under `skills/` in the RESOURCERA checkout. Copy the
**whole** `resourcera` or `resourcera-provider` directory, including its
`references/` and any `scripts/`, into a skill location supported by your agent.
The evaluation prompts are optional for ordinary use.

=== "GitHub Copilot"

    For a repository-local skill, use `.github/skills/<skill>/` or
    `.agents/skills/<skill>/`. For a user-level Copilot CLI skill, use
    `~/.copilot/skills/<skill>/`. Start a new session or reload skills according
    to your agent's instructions.

    See [GitHub's skill documentation](https://docs.github.com/en/copilot/how-tos/copilot-cli/customize-copilot/add-skills).

=== "Other agents"

    Place the directories in the skill location documented by your agent. Many
    agents support `.agents/skills/<skill>/`; some also support their own
    agent-specific locations. Reload or restart the agent when required.

## Skills CLI

The repository includes a `skills.sh.json` discovery manifest. The
[skills CLI](https://github.com/vercel-labs/skills) can install either skill:

```shell
npx skills add smuniharish/resourcera --skill resourcera
npx skills add smuniharish/resourcera --skill resourcera-provider
```

For an unpublished local checkout, copy the directories manually instead.
Review the CLI's agent and scope options before installing.

## Skills for dependencies

These are optional instructions for coding agents; installing a skill does not
install its Python package. Check each upstream skill's supported package version
and your agent's skill directory before use.

For applications using refresh-engine directly, its maintainers publish the
`refresh-engine` skill for application integration and `refresh-engine-extend`
for custom integrations and stores. They target refresh-engine 1.1.0 and
Python 3.12 or newer; OpenTelemetry support is optional:

```shell
npx skills add https://github.com/smuniharish/refresh-engine/tree/master/refresh-engine-skills/skills/refresh-engine
npx skills add https://github.com/smuniharish/refresh-engine/tree/master/refresh-engine-skills/skills/refresh-engine-extend
```

See the [upstream installation instructions](https://github.com/smuniharish/refresh-engine/tree/master/refresh-engine-skills).

Pydantic maintains a skill for Pydantic validation and models:

```shell
npx skills add pydantic/skills --skill pydantic
```

See [Pydantic's installation guidance](https://pydantic.dev/articles/pydantic-ai-logfire-claude-code-skills)
for supported agents and alternative installation options. These skills are not
required to use RESOURCERA.

For broader NVIDIA integration work, NVIDIA also publishes a skill catalog and
an optional `nvidia-skill-finder`:

```shell
npx skills add nvidia/skills --skill nvidia-skill-finder --yes
```

See the [NVIDIA skill catalog](https://github.com/NVIDIA/skills) for its
prerequisites, including skills CLI 1.5.16 or newer. This finder is not an
NVML-specific skill and is not required by the optional `nvidia` extra.

## Verify the setup

Run the core skill's check with the same Python environment your application uses:

```shell
python <skill directory>/resourcera/scripts/verify_setup.py
python <skill directory>/resourcera/scripts/verify_setup.py --json
```

The check is offline and opens and closes one manager. It reports whether a
snapshot was obtained, plus the memory status, overhead state, and count of
visible accelerators. It does not require hardware telemetry to be present or
install dependencies. Exit status 0 means the check passed; 1 means it failed.

## Evaluate a skill

Each `evals/` directory contains task prompts with expected assertions in
`evals.json`, plus positive and near-miss queries in `trigger_queries.json`.
The repository's skill tests check the discovery manifest, linked references,
evaluation data, and setup script. These checks do **not** run the prompts
against a coding agent or establish agent success rates.

To evaluate agent behavior, run the prompts with and without the skill in
separate agent sessions and grade the results against their assertions. Record
the agent, environment, and observed outcomes; do not assume the reference
prompts have already passed.
