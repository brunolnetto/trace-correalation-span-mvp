"""Repository-wide test configuration.

The test-only auth mode is deliberately enabled only while pytest is executing.
Production and Docker Compose default to fail-closed OIDC validation.
"""

import os

os.environ["AUTH_MODE"] = "test"
