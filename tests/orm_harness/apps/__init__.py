"""Stand-in for Dispatcharr's `apps` package, for tests/orm_harness only.

Holds just the models `_action_apply` and `_action_reap` touch, with the field
names, null-ability, related names and on_delete rules copied from Dispatcharr
0.32.0 (`/app/apps/*/models.py`). Columns those code paths never read are left
out. Never put this directory on sys.path in the pytest process itself: it is
added only inside the harness subprocess (see run_scenarios.py).
"""
