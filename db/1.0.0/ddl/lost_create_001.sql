create table phone_member(
member_id character varying(100) not null,
member_name character varying(100) ,
created_by text default 'SYSTEM'::text,
created_date timestamp without time zone Default now(),
updated_by text default 'SYSTEM'::text,
updated_date timestamp without time zone Default now(),
constraint phone_member_py primary key (member_id)
)
with (oids = false);

alter table phone_member owner to lostdata;

grant all on table phone_member to lostdata;
grant select,update,insert,delete on table phone_member to r_lost_dml;
grant select on table phone_member to r_lost_qry;

comment on table phone_member is '成员表';
comment on column phone_member.member_id is '成员ID';
comment on column phone_member.member_name is '成员姓名';