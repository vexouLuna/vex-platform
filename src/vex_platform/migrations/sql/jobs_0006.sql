-- Recurring work: a schedule enqueues a run of its kind whenever its cron expression (UTC) is due.
-- A paused schedule skips its turns; resuming does not run the ones it missed.
CREATE TABLE job_schedules (
    id          bigserial PRIMARY KEY,
    name        text NOT NULL UNIQUE,
    kind        text NOT NULL,
    subject     text,
    payload     jsonb NOT NULL DEFAULT '{}',
    cron        text NOT NULL,
    paused_at   timestamptz,
    last_run_at timestamptz,
    next_run_at timestamptz NOT NULL,
    actor_kind  text NOT NULL,
    actor_id    text,
    actor_login text,
    via         text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX job_schedules_due ON job_schedules (next_run_at);
ALTER TABLE job_runs ADD COLUMN schedule_id bigint REFERENCES job_schedules (id) ON DELETE SET NULL;
CREATE INDEX job_runs_schedule ON job_runs (schedule_id, id) WHERE schedule_id IS NOT NULL;
