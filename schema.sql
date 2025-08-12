create table if not exists worker_health(
  id bigserial primary key,
  worker_id text not null,
  ts timestamptz not null default now(),
  ws_status text,
  model_local boolean,
  backend_ok boolean,
  latency_ms integer
);
create index if not exists idx_worker_health_ts on worker_health(ts desc);

create table if not exists signals(
  id bigserial primary key,
  ts timestamptz not null default now(),
  symbol text not null,
  decision text not null,
  score double precision,
  model_confidence double precision,
  qty integer,
  entry_price double precision,
  stop_loss double precision,
  take_profit double precision,
  rationale text
);
create index if not exists idx_signals_symbol_ts on signals(symbol, ts desc);

