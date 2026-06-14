---
description: "Use when running terminal commands in pydsm. Always start by activating the dsm2ui conda environment before executing any project commands."
name: "pydsm Terminal And Conda Activation"
---
# Terminal And Environment Rules

- Before any project command, run `conda activate dsm2ui` first.
- Do not run project commands before environment activation.
- If the environment is unavailable, stop and ask for the correct environment setup instead of continuing.

## Standard Command Sequence

```
conda activate dsm2ui
<project command here>
```

## Examples

```
conda activate dsm2ui
python -m pytest tests/
```

```
conda activate dsm2ui
python -c "from pydsm.output.hydroh5 import HydroH5; ..."
```
