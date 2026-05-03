# Agent instructions

Before committing any changes, you MUST:
- Ensure tests are passing
- Follow assertion style (actual on left, expected on right)
- Always add new imports to the top of the file rather than locally scoped inside a function; the only exception is if it is needed to avoid a circular dependency
- Always mark AI-generated tests with `ai_generated` Pytest marker
- Run pre-commit
- Always bump the version in `pyproject.toml` appropriately
- Leave a short description of the change or addition in the top `# Upcoming` section of the `CHANGELOG.md`
