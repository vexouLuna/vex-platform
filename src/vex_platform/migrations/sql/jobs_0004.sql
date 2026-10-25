-- A "debug" level for run events, and an index for keeping only the newest progress report per run and step
-- (progress reports no longer count toward a run's cap of log lines).
ALTER TABLE job_run_events DROP CONSTRAINT job_run_events_level_check;
ALTER TABLE job_run_events ADD CONSTRAINT job_run_events_level_check
    CHECK (level IN ('debug', 'info', 'warning', 'error'));
CREATE INDEX job_run_events_progress ON job_run_events (run_id, id) WHERE progress IS NOT NULL;
