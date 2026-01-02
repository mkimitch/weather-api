# Contributing

Thanks for your interest in contributing!

## Quick start (local dev)

1. Create a virtualenv and install dependencies:

   ```bash
   python -m venv venv
   . venv/bin/activate
   pip install -r requirements.txt
   ```

2. Create a config file:

   ```bash
   cp config.yaml.example config.yaml
   ```

3. Run the API:

   ```bash
   uvicorn main:app --reload
   ```

## Making changes

- Keep changes small and focused.
- Avoid committing secrets (API keys) or local data.
  - `config.yaml` is intentionally ignored by git.
  - `weather_cache/` and `venv/` are ignored by git.

## Running checks

This repo's CI currently performs a syntax check.

Run the same check locally:

```bash
python -m py_compile main.py cache.py scheduler.py weather.py
```

## Submitting a pull request

- Describe the change and why it's needed.
- Include steps to test the change.
- If you modify configuration behavior, update `config.yaml.example` and the README as needed.
