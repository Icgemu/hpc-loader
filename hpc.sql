CREATE TABLE job_dispatch
(
    id bigserial NOT NULL ,
    job_id character varying(20),
    job_run_node character varying(20),
    job_run_ncpu integer,
    create_time timestamp without time zone,
    update_time timestamp without time zone,
    job_status integer,
    CONSTRAINT job_dispatch_pkey PRIMARY KEY (id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.job_dispatch
    OWNER to eshgfuu;
COMMENT ON TABLE job_dispatch
    IS '任务调度表';
	
	
	
CREATE TABLE job_submit
(
    id bigserial NOT NULL,
    job_queue character varying(100),
    job_owner character varying(100) ,
    job_id character varying(20),
    create_time timestamp without time zone,
    update_time timestamp without time zone,
    job_name character varying(100),
    job_status integer,
    CONSTRAINT job_info_pkey PRIMARY KEY (id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE job_submit
    OWNER to eshgfuu;
COMMENT ON TABLE job_submit
    IS '提交的任务信息';
	
	
CREATE TABLE job_result
(
    id bigserial NOT NULL,
    st timestamp without time zone,
    et timestamp without time zone,
    job_queue character varying(100) ,
    job_owner character varying(100) ,
    job_name character varying(100) ,
    job_status integer,
    job_cpupercent double precision,
    job_cput character varying(20) ,
    job_mem character varying(20) ,
    job_vmem character varying(20) ,
    job_ncpu integer,
    job_walltime character varying(20),
    create_time timestamp without time zone,
    update_time timestamp without time zone,
    CONSTRAINT job_result_pkey PRIMARY KEY (id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE job_result
    OWNER to eshgfuu;
COMMENT ON TABLE job_result
    IS '任务执行结果表';
	

	
	
CREATE TABLE node_status
(
    cpu_usage double precision,
    create_time timestamp without time zone,
    disk_usage double precision,
    id bigserial NOT NULL ,
    mem_usage double precision,
    name character varying(20) ,
    update_time timestamp without time zone,
    CONSTRAINT node_status_pkey PRIMARY KEY (id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE node_status
    OWNER to eshgfuu;
COMMENT ON TABLE node_status
    IS '计算节点CPU 内存 磁盘状态表';