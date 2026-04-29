---
description: "Use when running terminal commands in pydsm. Always use Command Prompt (cmd) and always start by activating the dsm2ui conda environment before executing any project commands."
name: "pydsm Terminal And Conda Activation"
---
# Terminal And Environment Rules

- Use Command Prompt (`cmd`) for terminal commands in this repository.
- The terminal may default to PowerShell. ALWAYS run `cmd` first as a separate step, wait for the prompt, then run `conda activate dsm2ui` as a separate step, wait for activation, then run the project command.
- Never chain these three steps with `;` or `&&` — run each as its own command.
- If the environment is unavailable, stop and ask for the correct environment setup instead of continuing in a different shell.

## Standard Command Sequence (three separate steps)

```bat
cmd
```
```bat
conda activate dsm2ui
```
```bat
<project command here>
```

## Examples

```bat
cmd
```
```bat
conda activate dsm2ui
```
```bat
python -m pytest tests/
```

```bat
cmd
```
```bat
conda activate dsm2ui
```
```bat
python -c "from pydsm.output.hydroh5 import HydroH5; ..."
```
