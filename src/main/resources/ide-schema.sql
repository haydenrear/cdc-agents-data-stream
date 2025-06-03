create table if not exists  ide_checkpoints
(
    thread_id     text                  not null,
    prompt_id     text                  not null,
    session_id    text                  not null,
    checkpoint_ts text default ''::text not null,
    checkpoint_id text                  not null,
    blob          bytea                 not null,
    task_path     text default ''::text not null,
    primary key (thread_id, checkpoint_id)
);

create index if not exists  ide_checkpoints_thread_id_idx
    on ide_checkpoints (thread_id);
create index if not exists  ide_checkpoints_thread_id_checkpoint_id_idx
    on ide_checkpoints (thread_id, checkpoint_id);
