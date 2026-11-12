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

The repository includes a `skills.sh.json` discovery manifest. **After these
changes are available on GitHub**, the [skills CLI](https://github.com/vercel-labs/skills)
can install either skill:

```shell
npx skills add smuniharish/resourcera --skill resourcera
npx skills add smuniharish/resourcera --skill resourcera-provider
```

For an unpublished local checkout, copy the directories manually instead.
Review the CLI's agent and scope options before installing.

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
