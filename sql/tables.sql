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
                    references user_info (mid),
    password        varchar(400) not null,
    qq              varchar(400),
    wechat          varchar(400)
);

create table follow (
    up_mid          bigint not null
                    references user_info (mid),
    fans_mid        bigint not null
                    references user_info (mid),
    primary key (up_mid, fans_mid)
);

create table video_info (
    bv              varchar(400)
                    primary key,
    title           varchar(400) not null,
    duration        float4 not null,
    description     varchar(8000) not null,
    owner_mid       bigint not null
                    references user_info (mid),
    reviewer_mid    bigint
                    references user_info (mid),
    commit_time     timestamp not null,
    review_time     timestamp,
    public_time     timestamp,
    like_cnt        integer default 0,
    coin_cnt        integer default 0,
    fav_cnt         integer default 0,
    view_cnt        integer default 0,
    view_time       float4 default 0,
    can_see         bool default false,
    check ( char_length(title) >= 1 )
);

create table like_video (
    mid             bigint not null
                    references user_info (mid),
    bv              varchar(400) not null
                    references video_info (bv),
    primary key (mid, bv)
);

create table coin_video (
    mid             bigint not null
                    references user_info (mid),
    bv              varchar(400) not null
                    references video_info (bv),
    primary key (mid, bv)
);

create table fav_video (
    mid             bigint not null
                    references user_info (mid),
    bv              varchar(400) not null
                    references video_info (bv),
    primary key (mid, bv)
);

create table view_video (
    mid             bigint not null
                    references user_info (mid),
    bv              varchar(400) not null
                    references video_info (bv),
    time            float4 not null,
    primary key (mid, bv)
);

create table danmu_info (
    danmu_id        serial primary key,
    bv              varchar(400) not null
                    references video_info (bv),
    mid             bigint not null
                    references user_info (mid),
    time            float4 not null,
    content         varchar(400),
    post_time       timestamp not null
);

create table like_danmu (
    mid             bigint not null
                    references user_info (mid),
    danmu_id        integer not null
                    references danmu_info (danmu_id),
    primary key (mid, danmu_id)
);

------------ help table -------------
create table max_mid (
    max_mid         bigint not null primary key
                    references user_info (mid)
);

create table max_bv (
    bv              varchar(400) not null primary key
                    references video_info (bv),
    num             bigint not null
);

