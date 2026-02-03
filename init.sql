CREATE TABLE IF NOT EXISTS secs_messages (
  id              bigserial PRIMARY KEY,
  received_at     timestamptz NOT NULL DEFAULT now(),

  msg_timestamp   timestamptz NULL,
  stream          int NOT NULL,
  function        int NOT NULL,
  wbit            boolean NOT NULL,
  device_id       int NULL,
  system_bytes    text NULL,
  ptype           int NULL,
  stype           int NULL,

  -- "S2F49" 같은 식별자
  msg_type        text NOT NULL,

  -- 원본/파싱 결과
  raw_json        jsonb NOT NULL,
  parsed_json     jsonb NULL,

  -- 대표 필드(메시지 타입에 따라 일부만 채워짐)
  command_name    text NULL,
  command_id      text NULL,
  priority        int NULL,
  carrier_id      text NULL,
  source          text NULL,
  dest            text NULL
);

CREATE INDEX IF NOT EXISTS ix_secs_messages_ts
  ON secs_messages (msg_timestamp);

CREATE INDEX IF NOT EXISTS ix_secs_messages_type_ts
  ON secs_messages (msg_type, msg_timestamp DESC);

CREATE INDEX IF NOT EXISTS ix_secs_messages_raw_gin
  ON secs_messages USING gin (raw_json);

CREATE INDEX IF NOT EXISTS ix_secs_messages_parsed_gin
  ON secs_messages USING gin (parsed_json);
