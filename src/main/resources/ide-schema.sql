create table if not exists  ide_checkpoints
(
    thread_id     text                  not null,
    checkpoint_ts text default ''::text not null,
    checkpoint_id text                  not null,
    blob          bytea                 not null,
    task_path     text default ''::text not null,
    primary key (thread_id, checkpoint_ts, checkpoint_id)
);

create index if not exists  checkpoint_writes_thread_id_idx
    on checkpoint_writes (thread_id);

