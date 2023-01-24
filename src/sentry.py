import os
import sentry_sdk

sentry_sdk.init(
  dsn = os.environ['SENTRY_BACKEND_DSN'],
  traces_sample_rate=1.0,
)
