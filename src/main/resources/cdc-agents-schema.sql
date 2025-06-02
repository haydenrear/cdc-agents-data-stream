create table if not exists checkpoint_migrations
(
    v integer not null
        primary key
);

alter table checkpoint_migrations
    owner to postgres;

create table if not exists checkpoints
(
    thread_id            text                      not null,
    checkpoint_ns        text  default ''::text    not null,
    checkpoint_id        text                      not null,
    parent_checkpoint_id text,
    type                 text,
    checkpoint           jsonb                     not null,
    metadata             jsonb default '{}'::jsonb not null,
    primary key (thread_id, checkpoint_ns, checkpoint_id)
);

alter table checkpoints
    owner to postgres;

create  index if not exists checkpoints_thread_id_idx
    on checkpoints (thread_id);

create table if not exists checkpoint_blobs
(
    thread_id     text                  not null,
    checkpoint_ns text default ''::text not null,
    channel       text                  not null,
    version       text                  not null,
    type          text                  not null,
    blob          bytea,
    primary key (thread_id, checkpoint_ns, channel, version)
);

alter table checkpoint_blobs
    owner to postgres;

create index if not exists  checkpoint_blobs_thread_id_idx
    on checkpoint_blobs (thread_id);

create table if not exists  checkpoint_writes
(
    thread_id     text                  not null,
    checkpoint_ns text default ''::text not null,
    checkpoint_id text                  not null,
    task_id       text                  not null,
    idx           integer               not null,
    channel       text                  not null,
    type          text,
    blob          bytea                 not null,
    task_path     text default ''::text not null,
    primary key (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
);

alter table checkpoint_writes
    owner to postgres;

create index if not exists  checkpoint_writes_thread_id_idx
    on checkpoint_writes (thread_id);

