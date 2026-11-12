# RESOURCERA Agent Skills

These are optional instructions for compatible coding agents, not Python runtime
dependencies. Each skill is a self-contained directory:

| Skill | Use it for |
| --- | --- |
| [`resourcera`](resourcera/SKILL.md) | Snapshots, workload capacity, admission, and reservations |
| [`resourcera-provider`](resourcera-provider/SKILL.md) | Custom isolated accelerator providers |

Copy the whole skill directory into your agent's skill location. The core skill
includes an offline setup check; both include focused references and evaluation
prompts. See the [installation and evaluation guide](../docs/agent-skills/install.md).
