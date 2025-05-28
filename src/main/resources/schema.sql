create table if not exists public.checkpoint_migrations
(
    v integer not null
        primary key
);

alter table public.checkpoint_migrations
    owner to postgres;

create table if not exists public.checkpoints
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

alter table public.checkpoints
    owner to postgres;

create  index if not exists checkpoints_thread_id_idx
    on public.checkpoints (thread_id);

create table if not exists public.checkpoint_blobs
(
    thread_id     text                  not null,
    checkpoint_ns text default ''::text not null,
    channel       text                  not null,
    version       text                  not null,
    type          text                  not null,
    blob          bytea,
    primary key (thread_id, checkpoint_ns, channel, version)
);

alter table public.checkpoint_blobs
    owner to postgres;

create index if not exists  checkpoint_blobs_thread_id_idx
    on public.checkpoint_blobs (thread_id);

create table if not exists  public.checkpoint_writes
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

alter table public.checkpoint_writes
    owner to postgres;

create index if not exists  checkpoint_writes_thread_id_idx
    on public.checkpoint_writes (thread_id);

