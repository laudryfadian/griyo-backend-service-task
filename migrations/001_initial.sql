CREATE SCHEMA IF NOT EXISTS task;
CREATE TABLE IF NOT EXISTS task.tasks(id text PRIMARY KEY,project_id text NOT NULL,agent_id text NOT NULL,server_id text NOT NULL,status text NOT NULL,data jsonb NOT NULL,lease_id text,lease_until timestamptz,started_at timestamptz,budget numeric NOT NULL,spent numeric NOT NULL DEFAULT 0,created_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS task_queue ON task.tasks(server_id,status,created_at);
CREATE UNIQUE INDEX IF NOT EXISTS task_one_active_agent ON task.tasks(agent_id) WHERE status='Working';
CREATE TABLE IF NOT EXISTS task.messages(id text PRIMARY KEY,task_id text REFERENCES task.tasks(id),data jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS task.events(id text PRIMARY KEY,data jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS task.approvals(id text PRIMARY KEY,task_id text REFERENCES task.tasks(id),status text NOT NULL,data jsonb NOT NULL);
