package internal

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/laudryfadian/griyo-backend-service-task/internal/domain"
	"github.com/laudryfadian/griyo-backend-service-task/internal/pb"
	"github.com/laudryfadian/griyo-backend-service-task/internal/platform"
	"time"
)

const migration = `CREATE SCHEMA IF NOT EXISTS task;
CREATE TABLE IF NOT EXISTS task.tasks(id text PRIMARY KEY,project_id text NOT NULL,agent_id text NOT NULL,server_id text NOT NULL,status text NOT NULL,data jsonb NOT NULL,lease_id text,lease_until timestamptz,started_at timestamptz,budget numeric NOT NULL,spent numeric NOT NULL DEFAULT 0,created_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS task_queue ON task.tasks(server_id,status,created_at);
CREATE UNIQUE INDEX IF NOT EXISTS task_one_active_agent ON task.tasks(agent_id) WHERE status='Working';
CREATE TABLE IF NOT EXISTS task.messages(id text PRIMARY KEY,task_id text REFERENCES task.tasks(id),data jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS task.events(id text PRIMARY KEY,data jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS task.approvals(id text PRIMARY KEY,task_id text REFERENCES task.tasks(id),status text NOT NULL,data jsonb NOT NULL);`

type Service struct {
	pb.UnimplementedTaskServiceServer
	db        *pgxpool.Pool
	workspace pb.WorkspaceServiceClient
	fleet     pb.FleetServiceClient
}

func New(ctx context.Context, db *pgxpool.Pool, w pb.WorkspaceServiceClient, f pb.FleetServiceClient) (*Service, error) {
	_, e := db.Exec(ctx, migration)
	return &Service{db: db, workspace: w, fleet: f}, e
}
func (s *Service) get(ctx context.Context, id string) (domain.Task, error) {
	var t domain.Task
	var b []byte
	e := s.db.QueryRow(ctx, "SELECT data FROM task.tasks WHERE id=$1", id).Scan(&b)
	if errors.Is(e, pgx.ErrNoRows) {
		return t, platform.NotFound()
	}
	if e != nil {
		return t, platform.Internal(e)
	}
	e = json.Unmarshal(b, &t)
	return t, e
}
func (s *Service) event(ctx context.Context, text string) {
	v := domain.Event{Id: platform.Id(), Text: text, CreatedAt: time.Now().UTC()}
	b, _ := json.Marshal(v)
	_, _ = s.db.Exec(ctx, "INSERT INTO task.events(id,data) VALUES($1,$2)", v.Id, b)
}
func (s *Service) Execute(ctx context.Context, r *pb.Request) (*pb.Response, error) {
	if r.Operation == "hasActive" {
		var exists bool
		e := s.db.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM task.tasks WHERE agent_id=$1 AND status IN ('Working','Review'))", r.Id).Scan(&exists)
		if e != nil {
			return nil, platform.Internal(e)
		}
		return platform.Reply(exists)
	}
	if r.Resource == "messages" {
		return s.messages(ctx, r)
	}
	if r.Resource == "approvals" {
		return s.approvals(ctx, r)
	}
	if r.Resource == "events" {
		rows, e := s.db.Query(ctx, "SELECT data FROM task.events ORDER BY data->>'createdAt' DESC LIMIT 200")
		if e != nil {
			return nil, platform.Internal(e)
		}
		defer rows.Close()
		items := []json.RawMessage{}
		for rows.Next() {
			var b json.RawMessage
			if e = rows.Scan(&b); e != nil {
				return nil, e
			}
			items = append(items, b)
		}
		return platform.Reply(items)
	}
	if r.Resource != "tasks" {
		return nil, platform.Invalid("unknown resource")
	}
	switch r.Operation {
	case "list":
		rows, e := s.db.Query(ctx, "SELECT data FROM task.tasks ORDER BY created_at DESC LIMIT 500")
		if e != nil {
			return nil, platform.Internal(e)
		}
		defer rows.Close()
		items := []json.RawMessage{}
		for rows.Next() {
			var b json.RawMessage
			if e = rows.Scan(&b); e != nil {
				return nil, e
			}
			items = append(items, b)
		}
		if e = rows.Err(); e != nil {
			return nil, e
		}
		return platform.Reply(items)
	case "get":
		t, e := s.get(ctx, r.Id)
		if e != nil {
			return nil, e
		}
		return platform.Reply(t)
	case "create":
		return s.create(ctx, r)
	case "claim":
		return s.claim(ctx, r.Id)
	case "report":
		return s.report(ctx, r)
	case "update":
		var update domain.TaskUpdate
		if e := platform.Decode(r.Body, &update); e != nil {
			return nil, e
		}
		tx, e := s.db.Begin(ctx)
		if e != nil {
			return nil, e
		}
		defer tx.Rollback(ctx)
		var b []byte
		e = tx.QueryRow(ctx, "SELECT data FROM task.tasks WHERE id=$1 FOR UPDATE", r.Id).Scan(&b)
		if e != nil {
			return nil, platform.NotFound()
		}
		var t domain.Task
		if e = json.Unmarshal(b, &t); e != nil {
			return nil, e
		}
		switch update.Status {
		case "Paused":
			if t.Status != "Working" && t.Status != "Queued" {
				return nil, platform.Conflict("only active or queued tasks can be paused")
			}
		case "Queued":
			if t.Status != "Paused" && t.Status != "Failed" && t.Status != "Review" && t.Status != "Working" {
				return nil, platform.Conflict("task cannot be requeued")
			}
		case "Done":
			if t.Status != "Review" {
				return nil, platform.Conflict("only reviewed results can be accepted")
			}
		default:
			return nil, platform.Invalid("allowed statuses: Paused, Queued, Done")
		}
		t.Status = update.Status
		t.LeaseId = ""
		if update.Error != "" {
			t.Error = update.Error
		}
		b, _ = json.Marshal(t)
		_, e = tx.Exec(ctx, "UPDATE task.tasks SET status=$2,data=$3,lease_id=NULL,lease_until=NULL WHERE id=$1", t.Id, t.Status, b)
		if e != nil {
			return nil, e
		}
		if e = tx.Commit(ctx); e != nil {
			return nil, e
		}
		s.event(ctx, "Task "+t.Title+" → "+t.Status)
		return platform.Reply(t)
	default:
		return nil, platform.Invalid("unsupported operation")
	}
}
func (s *Service) create(ctx context.Context, r *pb.Request) (*pb.Response, error) {
	var t domain.Task
	if e := platform.Decode(r.Body, &t); e != nil {
		return nil, e
	}
	if e := platform.ValidateName(t.Title); e != nil {
		return nil, e
	}
	if t.Brief == "" || t.Budget <= 0 {
		return nil, platform.Invalid("brief and positive budget required")
	}
	res, e := s.workspace.Execute(ctx, platform.Request("get", "agents", t.AgentId, nil))
	if e != nil {
		return nil, e
	}
	var a domain.Agent
	if e = json.Unmarshal(res.Body, &a); e != nil {
		return nil, e
	}
	if a.ProjectId == nil || *a.ProjectId != t.ProjectId {
		return nil, platform.Conflict("agent must belong to task project")
	}
	if a.Status == "Paused" {
		return nil, platform.Conflict("agent is paused")
	}
	res, e = s.workspace.Execute(ctx, platform.Request("get", "projects", t.ProjectId, nil))
	if e != nil {
		return nil, e
	}
	var p domain.Project
	if e = json.Unmarshal(res.Body, &p); e != nil {
		return nil, e
	}
	if t.WorkServerId == "" {
		t.WorkServerId = a.ServerId
		if t.WorkServerId == "" {
			t.WorkServerId = p.WorkServerId
		}
	}
	if t.DeployServerId == "" {
		t.DeployServerId = p.DeployServerId
	}
	res, e = s.fleet.Execute(ctx, platform.Request("get", "servers", t.WorkServerId, nil))
	if e != nil {
		return nil, e
	}
	var server domain.Server
	if e = json.Unmarshal(res.Body, &server); e != nil {
		return nil, e
	}
	if server.Status != "Online" {
		return nil, platform.Conflict("work server offline")
	}
	if t.Budget > server.Config.DailyBudget {
		return nil, platform.Conflict("task budget exceeds VPS daily budget")
	}
	res, e = s.fleet.Execute(ctx, platform.Request("get", "servers", t.DeployServerId, nil))
	if e != nil {
		return nil, e
	}
	t.Model = server.Config.Model
	if model := server.Config.ModelOverrides[a.Role]; model != "" {
		t.Model = model
	}
	t.Provider = server.Config.Provider
	t.Execution = domain.TaskExecution{Model: t.Model, Endpoint: server.Config.Endpoint, CredentialRef: server.Config.CredentialRef, Instructions: a.Instructions, RepositoryUrl: p.RepositoryUrl, TimeoutMinutes: server.Config.TimeoutMinutes, MaxRetries: server.Config.MaxRetries}
	t.Execution.Instructions = "Role: " + a.Role + "\n" + a.Description + "\n" + a.Instructions
	docsResponse, docsError := s.workspace.Execute(ctx, platform.Request("list", "documents", "", nil))
	if docsError != nil {
		return nil, docsError
	}
	var documents []domain.Document
	if e = json.Unmarshal(docsResponse.Body, &documents); e != nil {
		return nil, e
	}
	for _, document := range documents {
		if document.ProjectId == t.ProjectId && len(t.Execution.Instructions)+len(document.Body) < 24000 {
			t.Execution.Instructions += "\nProject knowledge: " + document.Title + "\n" + document.Body
		}
	}

	t.Id = platform.Id()
	t.Status = "Queued"
	t.Progress = 0
	t.Spent = 0
	t.Result = ""
	t.Error = ""
	t.LeaseId = ""
	t.Branch = "task/" + t.Id
	t.CreatedAt = time.Now().UTC()
	b, _ := json.Marshal(t)
	_, e = s.db.Exec(ctx, "INSERT INTO task.tasks(id,project_id,agent_id,server_id,status,data,budget) VALUES($1,$2,$3,$4,$5,$6,$7)", t.Id, t.ProjectId, t.AgentId, t.WorkServerId, t.Status, b, t.Budget)
	if e != nil {
		return nil, platform.Internal(e)
	}
	s.event(ctx, "Task dibuat: "+t.Title)
	return platform.Reply(t)
}
func (s *Service) claim(ctx context.Context, serverId string) (*pb.Response, error) {
	res, e := s.fleet.Execute(ctx, platform.Request("get", "servers", serverId, nil))
	if e != nil {
		return nil, e
	}
	var server domain.Server
	if e = json.Unmarshal(res.Body, &server); e != nil {
		return nil, e
	}
	if server.Status != "Online" {
		return nil, platform.Conflict("server offline")
	}
	tx, e := s.db.Begin(ctx)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", serverId); e != nil {
		return nil, e
	}
	// Expired leases fail closed. They are never automatically executed twice.
	_, e = tx.Exec(ctx, `UPDATE task.tasks SET status='Failed',lease_id=NULL,lease_until=NULL,data=jsonb_set(jsonb_set(data,'{status}','"Failed"'),'{error}','"Runner lease expired; review before retry"') WHERE server_id=$1 AND status='Working' AND lease_until<now()`, serverId)
	if e != nil {
		return nil, e
	}
	var running int
	var reserved float64
	e = tx.QueryRow(ctx, "SELECT count(*) FILTER(WHERE status='Working'),COALESCE(sum(CASE WHEN status='Working' THEN budget ELSE spent END) FILTER(WHERE started_at>=date_trunc('day',now())),0) FROM task.tasks WHERE server_id=$1", serverId).Scan(&running, &reserved)
	if e != nil {
		return nil, e
	}
	if running >= server.Config.MaxConcurrency {
		return &pb.Response{Body: []byte("null")}, nil
	}
	var b []byte
	e = tx.QueryRow(ctx, "SELECT data FROM task.tasks WHERE server_id=$1 AND status='Queued' AND agent_id NOT IN(SELECT agent_id FROM task.tasks WHERE status='Working') ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1", serverId).Scan(&b)
	if errors.Is(e, pgx.ErrNoRows) {
		return &pb.Response{Body: []byte("null")}, nil
	}
	if e != nil {
		return nil, e
	}
	var t domain.Task
	if e = json.Unmarshal(b, &t); e != nil {
		return nil, e
	}
	if server.Config.StopOnBudget && reserved+t.Budget > server.Config.DailyBudget {
		return &pb.Response{Body: []byte("null")}, nil
	}
	res, e = s.workspace.Execute(ctx, platform.Request("get", "agents", t.AgentId, nil))
	if e != nil {
		return nil, e
	}
	var a domain.Agent
	if e = json.Unmarshal(res.Body, &a); e != nil {
		return nil, e
	}
	if a.ProjectId == nil || *a.ProjectId != t.ProjectId || a.Status == "Paused" {
		return nil, platform.Conflict("agent assignment changed; update queued task")
	}
	t.Status = "Working"
	t.LeaseId = platform.Id()
	t.Progress = 5
	b, _ = json.Marshal(t)
	_, e = tx.Exec(ctx, "UPDATE task.tasks SET status='Working',data=$2,lease_id=$3,lease_until=now()+interval '90 seconds',started_at=now() WHERE id=$1", t.Id, b, t.LeaseId)
	if e != nil {
		return nil, e
	}
	if e = tx.Commit(ctx); e != nil {
		return nil, e
	}
	return platform.Reply(t)
}
func (s *Service) report(ctx context.Context, r *pb.Request) (*pb.Response, error) {
	var update domain.TaskUpdate
	if e := platform.Decode(r.Body, &update); e != nil {
		return nil, e
	}
	if update.Status != "Working" && update.Status != "Review" && update.Status != "Failed" {
		return nil, platform.Invalid("invalid runner report status")
	}
	tx, e := s.db.Begin(ctx)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback(ctx)
	var b []byte
	var lease string
	e = tx.QueryRow(ctx, "SELECT data,lease_id FROM task.tasks WHERE id=$1 AND status='Working' FOR UPDATE", r.Id).Scan(&b, &lease)
	if e != nil {
		return nil, platform.Conflict("task no longer running")
	}
	if !platform.Equal(lease, update.LeaseId) {
		return nil, platform.Conflict("stale runner lease")
	}
	var t domain.Task
	if e = json.Unmarshal(b, &t); e != nil {
		return nil, e
	}
	if update.Spent < t.Spent || update.Spent > t.Budget || update.Progress < 0 || update.Progress > 100 {
		return nil, platform.Invalid("invalid cost or progress")
	}
	t.Status = update.Status
	t.Progress = update.Progress
	t.Result = update.Result
	t.Error = update.Error
	t.Spent = update.Spent
	b, _ = json.Marshal(t)
	_, e = tx.Exec(ctx, "UPDATE task.tasks SET status=$2,data=$3,spent=$4,lease_until=now()+interval '90 seconds' WHERE id=$1", t.Id, t.Status, b, t.Spent)
	if e != nil {
		return nil, e
	}
	if e = tx.Commit(ctx); e != nil {
		return nil, e
	}
	if t.Status != "Working" {
		s.event(ctx, "Runner selesai: "+t.Title+" → "+t.Status)
	}
	return platform.Reply(t)
}
func (s *Service) messages(ctx context.Context, r *pb.Request) (*pb.Response, error) {
	if _, e := s.get(ctx, r.Id); e != nil {
		return nil, e
	}
	if r.Operation == "create" {
		var m domain.Message
		if e := platform.Decode(r.Body, &m); e != nil {
			return nil, e
		}
		if m.Text == "" {
			return nil, platform.Invalid("message text required")
		}
		m.Id = platform.Id()
		m.TaskId = r.Id
		m.Author = "Owner"
		m.CreatedAt = time.Now().UTC()
		b, _ := json.Marshal(m)
		_, e := s.db.Exec(ctx, "INSERT INTO task.messages(id,task_id,data) VALUES($1,$2,$3)", m.Id, r.Id, b)
		if e != nil {
			return nil, e
		}
		return platform.Reply(m)
	}
	rows, e := s.db.Query(ctx, "SELECT data FROM task.messages WHERE task_id=$1 ORDER BY data->>'createdAt'", r.Id)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	items := []json.RawMessage{}
	for rows.Next() {
		var b json.RawMessage
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		items = append(items, b)
	}
	return platform.Reply(items)
}
func (s *Service) approvals(ctx context.Context, r *pb.Request) (*pb.Response, error) {
	switch r.Operation {
	case "list":
		rows, e := s.db.Query(ctx, "SELECT data FROM task.approvals ORDER BY data->>'createdAt' DESC")
		if e != nil {
			return nil, e
		}
		defer rows.Close()
		items := []json.RawMessage{}
		for rows.Next() {
			var b json.RawMessage
			if e = rows.Scan(&b); e != nil {
				return nil, e
			}
			items = append(items, b)
		}
		return platform.Reply(items)
	case "create":
		t, e := s.get(ctx, r.Id)
		if e != nil {
			return nil, e
		}
		if t.Status != "Review" && t.Status != "Done" {
			return nil, platform.Conflict("task must be reviewed before approval request")
		}
		a := domain.Approval{Id: platform.Id(), TaskId: t.Id, ServerId: t.DeployServerId, Status: "Pending", CreatedAt: time.Now().UTC()}
		b, _ := json.Marshal(a)
		_, e = s.db.Exec(ctx, "INSERT INTO task.approvals(id,task_id,status,data) VALUES($1,$2,$3,$4)", a.Id, a.TaskId, a.Status, b)
		if e != nil {
			return nil, e
		}
		return platform.Reply(a)
	case "update":
		var d domain.ApprovalDecision
		if e := platform.Decode(r.Body, &d); e != nil {
			return nil, e
		}
		if d.Status != "Approved" && d.Status != "Rejected" {
			return nil, platform.Invalid("invalid approval decision")
		}
		var b []byte
		e := s.db.QueryRow(ctx, "SELECT data FROM task.approvals WHERE id=$1 AND status='Pending'", r.Id).Scan(&b)
		if e != nil {
			return nil, platform.NotFound()
		}
		var a domain.Approval
		if e = json.Unmarshal(b, &a); e != nil {
			return nil, e
		}
		if d.Status == "Approved" {
			res, e := s.fleet.Execute(ctx, platform.Request("get", "servers", a.ServerId, nil))
			if e != nil {
				return nil, e
			}
			var server domain.Server
			if e = json.Unmarshal(res.Body, &server); e != nil {
				return nil, e
			}
			if server.Status != "Online" {
				return nil, platform.Conflict("deployment target offline")
			}
		}
		a.Status = d.Status
		a.Reason = d.Reason
		b, _ = json.Marshal(a)
		tag, e := s.db.Exec(ctx, "UPDATE task.approvals SET data=$2,status=$3 WHERE id=$1 AND status='Pending'", a.Id, b, a.Status)
		if e != nil {
			return nil, e
		}
		if tag.RowsAffected() == 0 {
			return nil, platform.Conflict("approval already decided")
		}
		s.event(ctx, "Approval "+a.Id+" → "+a.Status)
		return platform.Reply(a)
	}
	return nil, platform.Invalid("unsupported approval operation")
}
