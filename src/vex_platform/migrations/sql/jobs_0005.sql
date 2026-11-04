-- A finished run can be dismissed: it stays in history but leaves failure alerts and badge counts.
ALTER TABLE job_runs ADD COLUMN dismissed_at timestamptz;
CREATE INDEX job_runs_undismissed ON job_runs (state, id) WHERE dismissed_at IS NULL;
