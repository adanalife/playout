Collapse two redundancies: `STREAM_PLATFORM` is read once and passed into `run`, and the single-caller `telemetry::attrs_with` helper is inlined at its call site.
