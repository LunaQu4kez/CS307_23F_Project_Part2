--------------- create database ---------------
/*
create database project2 with encoding = 'UTF8' lc_collate = 'C' template = template0;
*/


--------------- create user -----------------
/*
create user manager with password '123456';
grant connect on database project2 to manager;
grant usage on schema public to manager;
grant select, insert, update, delete, truncate on all tables in schema public to manager;
*/


------------ create table ----------------
create table user_info (
    mid             bigint not null
                    primary key,
    name            varchar(400) not null,
    sex             varchar(10) not null default 'UNKNOWN',
    birthday        varchar(20),
    level           integer not null,
    coin            integer not null default 0,
    sign            varchar(8000),
    identity        varchar(20) not null
);

create table user_auth (
    mid             bigint not null
                    primary key
                    references user_info (mid)
                    on delete cascade,
    password        varchar(400) not null,
    qq              varchar(400),
    wechat          varchar(400)
);

create table follow (
    up_mid          bigint not null
                    references user_info (mid)
                    on delete cascade,
    fans_mid        bigint not null
                    references user_info (mid)
                    on delete cascade,
    primary key (up_mid, fans_mid)
);

create table video_info (
    bv              varchar(400)
                    primary key,
    title           varchar(400) not null,
    duration        float4 not null,
    description     varchar(8000) not null,
    owner_mid       bigint not null
                    references user_info (mid)
                    on delete cascade,
    reviewer_mid    bigint
                    references user_info (mid)
                    on delete cascade,
    commit_time     timestamp not null,
    review_time     timestamp,
    public_time     timestamp,
    can_see         bool default false,
    check ( char_length(title) >= 1 )
);

create table like_video (
    mid             bigint not null
                    references user_info (mid)
                    on delete cascade,
    bv              varchar(400) not null
                    references video_info (bv)
                    on delete cascade,
    primary key (mid, bv)
);

create table coin_video (
    mid             bigint not null
                    references user_info (mid)
                    on delete cascade,
    bv              varchar(400) not null
                    references video_info (bv)
                    on delete cascade,
    primary key (mid, bv)
);

create table fav_video (
    mid             bigint not null
                    references user_info (mid)
                    on delete cascade,
    bv              varchar(400) not null
                    references video_info (bv)
                    on delete cascade,
    primary key (mid, bv)
);

create table view_video (
    mid             bigint not null
                    references user_info (mid)
                    on delete cascade,
    bv              varchar(400) not null
                    references video_info (bv)
                    on delete cascade,
    time            float4 not null,
    primary key (mid, bv)
);

create table danmu_info (
    danmu_id        serial primary key,
    bv              varchar(400) not null
                    references video_info (bv)
                    on delete cascade,
    mid             bigint not null
                    references user_info (mid)
                    on delete cascade,
    time            float4 not null,
    content         varchar(400),
    post_time       timestamp not null
);

create table like_danmu (
    mid             bigint not null
                    references user_info (mid)
                    on delete cascade,
    danmu_id        integer not null
                    references danmu_info (danmu_id)
                    on delete cascade,
    primary key (mid, danmu_id)
);

------------ help table -------------
create table max_mid (
    max_mid         bigint not null primary key
                    references user_info (mid)
                    on delete cascade
);


/*
-------------- trigger --------------
create or replace function likev_update() returns trigger as $$
begin
    if (TG_OP = 'INSERT') then
        update video_info set like_cnt = like_cnt + 1 where bv = new.bv;
    elsif (TG_OP = 'DELETE') then
        update video_info set like_cnt = like_cnt - 1 where bv = new.bv;
    end if;
    return null;
end;
$$ language plpgsql;

create trigger likev
after insert or delete on like_video
for each row
execute function likev_update();

drop trigger likev on like_video;

create or replace function coinv_update() returns trigger as $$
begin
    if (TG_OP = 'INSERT') then
        update video_info set coin_cnt = coin_cnt + 1 where bv = new.bv;
        update user_info set coin = coin - 1 where mid = new.mid;
    elsif (TG_OP = 'DELETE') then
        update video_info set coin_cnt = coin_cnt - 1 where bv = new.bv;
    end if;
    return null;
end;
$$ language plpgsql;

create trigger coinv
after insert or delete on coin_video
for each row
execute function coinv_update();

drop trigger coinv on coin_video;

create or replace function favv_update() returns trigger as $$
begin
    if (TG_OP = 'INSERT') then
        update video_info set fav_cnt = fav_cnt + 1 where bv = new.bv;
    elsif (TG_OP = 'DELETE') then
        update video_info set fav_cnt = fav_cnt - 1 where bv = new.bv;
    end if;
    return null;
end;
$$ language plpgsql;

drop trigger favv on fav_video;

create trigger favv
after insert or delete on fav_video
for each row
execute function favv_update();


create or replace function viewv_update() returns trigger as $$
begin
    if (TG_OP = 'INSERT') then
        update video_info set view_cnt = view_cnt + 1 where bv = new.bv;
        update video_info set view_time = view_time + new.time where bv = new.bv;
    elsif (TG_OP = 'UPDATE') then
        update video_info set view_time = view_time + new.time - old.time where bv = new.bv;
    elsif (TG_OP = 'DELETE') then
        update video_info set view_cnt = view_cnt - 1 where bv = new.bv;
        update video_info set view_time = view_time - old.time where bv = new.bv;
    end if;
    return null;
end;
$$ language plpgsql;

create trigger viewv
after insert or delete on view_video
for each row
execute function viewv_update();

drop trigger viewv on view_video;
*/






