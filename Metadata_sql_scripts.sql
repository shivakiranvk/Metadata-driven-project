create schema config;
go
create schema control;
go
create schema log;

--config has below tables 
--jobs, tasks, task_parameter, job_tasks, job_task_parameter, enviorment

--control schema have below tables 
--job_task_control, task_paramter_control, job_task_watermark

--log schema have below tables
--job_task_log

drop table if exists config.jobs
go
create table config.jobs
(
job varchar(4000) not null,
active char(1) not null,
insert_dt datetime not null default getdate(),
update_dt datetime
)
on [primary]
go
alter table config.jobs
add constraint uk_jobs_job unique(job)

go

drop table if exists config.tasks
go
create table config.tasks
(
task varchar(4000) not null,
active char(1) not null,
insert_dt datetime not null default getdate(),
update_dt datetime
)
on [primary]
go
alter table config.tasks
add constraint uk_tasks_task unique(task)
go



drop table if exists config.task_parameters
go

create table config.task_parameters
(
task varchar(4000) not null,
parameter varchar(4000) not null,
parameter_type varchar(4000) not null,
active char(1) not null,
insert_dt datetime not null default getdate(),
update_dt datetime
)
on [primary]
go



alter table config.task_parameters
add constraint uk_task_parameters_task_parameter unique(task,parameter)
go
alter table config.task_parameters
add constraint fk_task_parameters_task
foreign key(task) references config.tasks(task)
go



drop table if exists config.job_tasks
go
create table config.job_tasks
(
job varchar(4000) not null,
task varchar(4000) not null,
task_sequence int not null,
active char(1) not null,
insert_dt datetime not null default getdate(),
update_dt datetime
)
on [primary]
go



alter table config.job_tasks
add constraint uk_job_tasks_job_task unique(job,task)
go
alter table config.job_tasks
add constraint fk_job_tasks_job
foreign key(job) references config.jobs(job)
go



alter table config.job_tasks
add constraint fk_job_tasks_task
foreign key(task) references config.tasks(task)
go

drop table if exists config.job_task_parameters
go
create table config.job_task_parameters
(
job varchar(4000) not null,
task varchar(4000) not null,
parameter varchar(4000) not null,
value varchar(4000) not null,
active char(1) not null,
task_sequence int not null,
insert_dt datetime not null default getdate(),
update_dt datetime
)
on [primary]
go

alter table config.job_task_parameters
add constraint uk_job_task_parameters_job_task_parameter unique(job,task,parameter)
go

alter table config.job_task_parameters
add constraint fk_job_task_parameters_job_task
foreign key(job,task) references config.job_tasks(job,task)
go

alter table config.job_task_parameters
add constraint fk_job_task_parameters_task_parameter
foreign key(task,parameter) references config.task_parameters(task,parameter)
go

drop table if exists config.environment
go
create table config.environment

(
parameter varchar(4000) not null,
value varchar(4000) not null,
insert_dt datetime not null default getdate(),
update_dt datetime
)
on [primary]
go
alter table config.environment
add constraint uk_environment_parameter unique(parameter)
go

drop table if exists control.job_task_control
go
create table control.job_task_control
(
job_id varchar(4000) not null,
task_id varchar(4000) not null,
job varchar(4000) not null,
job_dt date not null,
task varchar(4000) not null,
task_sequence int not null,
task_status varchar(4000) not null,
insert_dt datetime not null default getdate(),
update_dt datetime
)
on [primary]
go
drop table if exists control.task_parameter_control
go

create table control.task_parameter_control
(
job_id varchar(4000) not null,
task_id varchar(4000) not null,
job varchar(4000) not null,
task varchar(4000) not null,
task_sequence int not null,
parameter varchar(4000) not null,
value varchar(4000) not null,
insert_dt datetime not null default getdate(),
update_dt datetime
)
on [primary]
go
drop table if exists control.job_task_watermark
go

create table control.job_task_watermark
(
job varchar(4000) not null,
task varchar(4000) not null,
last_run_date datetime not null,
insert_dt datetime not null default getdate(),
update_dt datetime
)
on [primary]

go
drop table if exists log.job_task_log
go

create table log.job_task_log
(
job_id varchar(4000) not null,
task_id varchar(4000) not null,
job varchar(4000) not null,
task varchar(4000),
task_sequence int,
job_status varchar(4000),
task_status varchar(4000),
status_description varchar(4000) not null,
insert_dt datetime not null default getdate()

)
on [primary]
go


We insert the information of our pipeline name, task, src table, traget table, parameters info in the tables ins sql server

Lab Script - Metadata Table Configurations
1. Config.Jobs
==========

insert into config.jobs (job,active)
values ('load_customer','Y')


2. Config.Tasks
============
insert into config.tasks(task,active)
values ('pl_extract_from_sqldb','Y')



3. Config.Job_Tasks
===============

insert into config.job_tasks(job,task,task_sequence,active)
values ('load_customer','pl_extract_from_sqldb',1,'Y')


4. Config.Task_Parameters
====================
insert into config.task_parameters(task,parameter,parameter_type,active)
values ('pl_extract_from_sqldb','src_sql','static','Y')
insert into config.task_parameters(task,parameter,parameter_type,active)
values ('pl_extract_from_sqldb','tgt_folder','static','Y')
insert into config.task_parameters(task,parameter,parameter_type,active)
values ('pl_extract_from_sqldb','tgt_filename','static','Y')



5. Config.Job_Task_Parameters
=======================

insert into config.job_task_parameters(job,task,parameter,value,active,task_sequence)
values ('load_customer','pl_extract_from_sqldb','src_sql','select * from saleslt.customer','Y',1)
insert into config.job_task_parameters(job,task,parameter,value,active,task_sequence)
values ('load_customer','pl_extract_from_sqldb','tgt_folder','customer','Y',1)
insert into config.job_task_parameters(job,task,parameter,value,active,task_sequence)
values ('load_customer','pl_extract_from_sqldb','tgt_filename','customer.csv','Y',1)
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--Lab Script - sp_task_parameters

DROP PROCEDURE IF EXISTS control.sp_task_parameters;
GO

CREATE PROCEDURE control.sp_task_parameters
(
    @job_id VARCHAR(4000),
    @task_id VARCHAR(4000),
    @job VARCHAR(4000),
    @task VARCHAR(4000),
    @task_sequence INT
)
AS
BEGIN
    INSERT INTO control.task_parameter_control
    (
        job_id,
        task_id,
        job,
        task,
        task_sequence,
        parameter,
        value
    )
    SELECT
        @job_id,
        @task_id,
        @job,
        @task,
        @task_sequence,
        jtp.parameter,
        jtp.value
    FROM
        config.job_task_parameters jtp
    JOIN
        config.task_parameters tp ON jtp.task = tp.task
                                   AND jtp.parameter = tp.parameter
                                   AND jtp.active = 'Y'
                                   AND tp.active = 'Y'
                                   AND tp.parameter_type = 'static'
    WHERE
        jtp.job = @job
        AND jtp.task = @task
        AND jtp.task_sequence = @task_sequence;

    DECLARE @sql NVARCHAR(MAX);
    DECLARE @columns NVARCHAR(MAX);

    SET @sql = N'';
    SET @columns = N'';

    SELECT @columns += N', ' + QUOTENAME(parameter)
    FROM
    (
        SELECT parameter
        FROM control.task_parameter_control tpc
        WHERE tpc.job = @job
              AND tpc.task = @task
              AND tpc.task_sequence = @task_sequence
              AND tpc.job_id = @job_id
              AND tpc.task_id = @task_id
    ) AS N;

    SET @sql = N'
        SELECT ' + STUFF(@columns, 1, 1, '') + '
        FROM
        (
            SELECT parameter, value
            FROM control.task_parameter_control tpc
            WHERE tpc.job = @j_name
                  AND tpc.task = @t_name
                  AND tpc.job_id = @j_id
                  AND tpc.task_id = @t_id
                  AND tpc.task_sequence = @t_sequence
        ) AS N
        PIVOT
        (
            MAX(value) FOR parameter IN (' + STUFF(@columns, 1, 1, '') + ')
        ) AS P;';

    EXEC sp_executesql @sql,
                       N'@j_name NVARCHAR(4000),
                         @t_name NVARCHAR(4000),
                         @j_id NVARCHAR(4000),
                         @t_id NVARCHAR(4000),
                         @t_sequence NVARCHAR(4000)',
                       @job,
                       @task,
                       @job_id,
                       @task_id,
                       @task_sequence;
END

here job id and task id are generated at runtime which are the pipeline generated id during excution 

/*the functionality of the control.sp_task_parameters stored procedure step by step:

Drop Procedure (IF EXISTS):

Checks if the procedure control.sp_task_parameters already exists.
If it exists, it's dropped to ensure a fresh creation.
Create Procedure:

Defines a new stored procedure named control.sp_task_parameters.
Accepts input parameters:
@job_id, @task_id: IDs related to the job and task.
@job, @task: Names of the job and task.
@task_sequence: Indicates the sequence of the task.
Insert Statement:

Populates the control.task_parameter_control table.
Selects and inserts parameter-value pairs from config.job_task_parameters and config.task_parameters.
Conditions for insertion:
Matching parameters between job and task configurations.
Active status ('Y' indicates active).
Specific parameter type ('static').
Dynamic SQL Generation:

Constructs dynamic SQL statements for selecting and pivoting data.
Constructs two variables:
@sql: Stores the dynamically generated SQL query.
@columns: Holds column names for the pivot operation.
Column Selection for Pivot:

Selects columns (parameters) for pivoting based on specific conditions from control.task_parameter_control.
Appends these columns to the @columns variable.
Dynamic SQL Execution (Pivot Operation):

Builds a SQL query using the @columns variable for pivoting the data.
Executes the dynamic SQL query using sp_executesql.
The pivot operation transforms rows into columns based on the selected parameters.
End of Procedure:

Ends the stored procedure's definition.
In essence, this procedure:

Inserts relevant parameter-value pairs into control.task_parameter_control based on specific conditions.
Dynamically constructs and executes a SQL query to pivot and retrieve parameter values based on job, task, and sequence provided as input parameters.
It's designed to manage and retrieve task parameters associated with specific jobs and tasks, allowing for dynamic data retrieval based on different job and task configurations.*/

------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Check if the procedure exists; if so, drop it
DROP PROCEDURE IF EXISTS control.sp_job_task_log;
GO

-- Create the procedure 'sp_job_task_log' in the 'control' schema
CREATE PROCEDURE control.sp_job_task_log
(
    @job_id VARCHAR(4000),
    @task_id VARCHAR(4000),
    @job VARCHAR(4000),
    @task VARCHAR(4000),
    @task_sequence INT,
    @job_status VARCHAR(4000),
    @task_status VARCHAR(4000),
    @status_description VARCHAR(4000)
)
AS
BEGIN
    -- Insert data into the 'log.job_task_log' table

    INSERT INTO log.job_task_log
    (
        job_id,
        task_id,
        job,
        task,
        task_sequence,
        job_status,
        task_status,
        status_description
    )
    VALUES
    (
        @job_id,
        @task_id,
        @job,
        @task,
        @task_sequence,
        @job_status,
        @task_status,
        @status_description
    );
END
GO

/*This script creates a stored procedure named sp_job_task_log in the control schema.
 The procedure takes in several input parameters (@job_id, @task_id, @job, @task, @task_sequence, @job_status,
 @task_status, @status_description) and inserts data into the log.job_task_log table. The comments help clarify the purpose of each section of the script.*/
 
 
 ----------------------------------------------------------------------------------------------------------------------------------------
 
 -- Check if the procedure exists; if so, drop it
DROP PROCEDURE IF EXISTS control.sp_update_job_control;
GO

-- Create the procedure 'sp_update_job_control' in the 'control' schema
CREATE PROCEDURE control.sp_update_job_control
(
    @job_id VARCHAR(4000),
    @job VARCHAR(4000),
    @task_id VARCHAR(4000),
    @task VARCHAR(4000),
    @task_status VARCHAR(4000)
)
AS
BEGIN
    -- Declare variables
    DECLARE @job_dt DATE, @error VARCHAR(4000)

    -- Set @job_dt variable to the current date and time
    SET @job_dt = GETDATE()

    -- Update the 'control.job_task_control' table
    UPDATE control.job_task_control
    SET
        task_id = @task_id,
        task_status = @task_status,
        update_dt = GETDATE()
    WHERE
        job = @job
        AND job_id = @job_id
        AND task = @task
END
GO
/*This script creates a stored procedure named sp_update_job_control in the control schema.
 The procedure takes in parameters (@job_id, @job, @task_id, @task, @task_status) and 
performs an update operation on the control.job_task_control table based on the provided conditions. The comments provide clarity on each section of the script.*/
------------------------------------------------------------------------------------------
Lab Script - sp_get_last_run_date

-- Drop the procedure if it already exists to avoid conflicts
DROP PROCEDURE IF EXISTS control.sp_get_last_run_date;
GO

-- Create a new stored procedure to retrieve the last run date based on job and task
CREATE PROCEDURE control.sp_get_last_run_date
(
    @job VARCHAR(4000),  -- Parameter: Job name
    @task VARCHAR(4000)  -- Parameter: Task name
)
AS
BEGIN
    -- Declare a variable to hold the last run date
    DECLARE @last_run_date DATETIME;

    -- Retrieve the last run date from the job_task_watermark table based on job and task
    SELECT @last_run_date = last_run_date
    FROM control.job_task_watermark
    WHERE job = @job
    AND task = @task;

    -- If last_run_date is empty or null, set it to a default date ('1900-01-01')
    IF @last_run_date = '' OR @last_run_date IS NULL
        SET @last_run_date = '1900-01-01';

    -- Return the last_run_date in a specific format (yyyy-mm-dd hh:mi:ss.mmm)
    SELECT CONVERT(VARCHAR, @last_run_date, 121) AS last_run_date;
END
GO

/*This script creates a stored procedure sp_get_last_run_date that takes @job and @task parameters to fetch the last run 
date from the job_task_watermark table. It ensures that if no date is found, it defaults to '1900-01-01'.
 The CONVERT function formats the retrieved date to a specific style (yyyy-mm-dd hh:mi:ss.mmm) before returning it.*/
 
 ----------------------------------------------------------------------------------------------------------------------------------------------------------------
 
 /*This script creates a stored procedure sp_update_last_run_date that takes @job, @task, and @last_run_date parameters. 
 It updates the job_task_watermark table with the provided last_run_date for the corresponding job and task. 
 If no rows are affected by the update (indicating that there is no existing record for the job and task), it inserts a new record with these details.*/
 
 -- Drop the procedure if it already exists to avoid conflicts
DROP PROCEDURE IF EXISTS control.sp_update_last_run_date;
GO

-- Create a new stored procedure to update the last run date based on job, task, and provided date
CREATE PROCEDURE control.sp_update_last_run_date
(
    @job VARCHAR(4000),           -- Parameter: Job name
    @task VARCHAR(4000),          -- Parameter: Task name
    @last_run_date DATETIME       -- Parameter: Last run date to be updated
)
AS
BEGIN
    -- Update the job_task_watermark table with the provided last run date and update date
    UPDATE control.job_task_watermark
    SET last_run_date = @last_run_date,
        update_dt = GETDATE()    -- Record the update timestamp

    WHERE job = @job
    AND task = @task;

    -- If no rows were affected by the update, insert a new record with job, task, and last run date
    IF @@ROWCOUNT = 0
    BEGIN
        INSERT INTO control.job_task_watermark (job, task, last_run_date)
        VALUES (@job, @task, @last_run_date);
    END
END
GO
-------------------------------------------------------------------------------------------------------

-- next for loading data into staging add pipeline details to metadata tables

Lab Script - Metadata Table Configurations
1. Config.Jobs

===========

insert into config.jobs (job,active)
values ('load_customer','Y')

select * from config.jobs

2. Config.Tasks

===========
insert into config.tasks(task,active)
values ('pl_load_into_stg','Y')

select * from config.tasks

3. Config.Job_Tasks

===============

insert into config.job_tasks(job,task,task_sequence,active)
values ('load_customer','pl_load_into_stg',2,'Y')

select * from config.job_tasks

4. Config.Task_Parameters

====================

insert into config.task_parameters(task,parameter,parameter_type,active)
values ('pl_load_into_stg','src_path','static','Y')

insert into config.task_parameters(task,parameter,parameter_type,active)
values ('pl_load_into_stg','tgt_stg_table','static','Y')

select * from config.task_parameters

5. Config.Job_Task_Parameters

========================

insert into config.job_task_parameters(job,task,parameter,value,active,task_sequence)
values ('load_customer','pl_load_into_stg','src_path','landing/customer/customer.csv','Y',2)

insert into config.job_task_parameters(job,task,parameter,value,active,task_sequence)
values ('load_customer','pl_load_into_stg','tgt_stg_table','saleslt.customer_target_Stg','Y',2)

select * from config.job_task_parameters
-----------------------------------------------------------------------------------------------------------------------------------------------------------

-- Drop the table if it already exists
IF OBJECT_ID('saleslt.Customer_Target_Stg', 'U') IS NOT NULL
    DROP TABLE saleslt.Customer_Target_Stg;

GO

-- Create the Customer_Target_Stg table
CREATE TABLE saleslt.Customer_Target_Stg (
    CustomerID     varchar(4000),
    NameStyle      varchar(4000),
    Title          varchar(4000),
    FirstName      varchar(4000),
    MiddleName     varchar(4000),
    LastName       varchar(4000),
    Suffix         varchar(4000),
    CompanyName    varchar(4000),
    SalesPerson    varchar(4000),
    EmailAddress   varchar(4000),
    Phone          varchar(4000),
    PasswordHash   varchar(4000),
    PasswordSalt   varchar(4000),
    rowguid        varchar(4000),
    ModifiedDate   varchar(4000)
);

GO
--------------------------------------------------------------------------------------------------

Staging table 
-- Drop the table if it already exists
IF OBJECT_ID('saleslt.Customer_Target_Stg', 'U') IS NOT NULL
    DROP TABLE saleslt.Customer_Target_Stg;

GO
-----------------------------------------------------------------------------------------------------------------------------------------------------------

--This script is a series of SQL commands aimed at configuring a database for bulk loads, likely involving Azure Blob Storage as an external data source. 

-- Drop the existing master key if it exists
DROP MASTER KEY;

-- Create a new master key encrypted by a strong password
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Svk@7019368730';

-- Create a scoped credential for Azure Blob Storage access
CREATE DATABASE SCOPED CREDENTIAL DevAdlsContainer
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = '?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-11-30T15:29:20Z&st=2023-11-20T07:29:20Z&spr=https&sig=bOAyqI1VAn81YsqZ67hoWBC5d9AnY1uGSDzS35n7ptg%3D';

--This creates a scoped credential named DevAdlsContainer, likely used to authenticate against an Azure Blob Storage account using a Shared Access Signature (SAS) token.

-- Drop the existing external data source if it exists
DROP EXTERNAL DATA SOURCE DevStorage;

-- Create an external data source for Azure Blob Storage
CREATE EXTERNAL DATA SOURCE DevStorage
WITH (
    TYPE = BLOB_STORAGE,
    LOCATION = 'https://devaaccount202318.blob.core.windows.net',
    CREDENTIAL = DevAdlsContainer
);

--This command creates an external data source named DevStorage, specifying it as a blob storage type and linking it to the 
--DevAdlsContainer credential. It points to an Azure Blob Storage URL.

-- Grant necessary privileges to roles and users
ALTER ROLE db_datawriter ADD MEMBER [dev-etl-sk];
ALTER ROLE db_executor ADD MEMBER [dev-etl-sk];

GRANT SELECT, UPDATE, DELETE, INSERT, ALTER, CONTROL ON SCHEMA::SalesLt TO [dev-etl-sk];
GRANT ADMINISTER DATABASE BULK OPERATIONS TO [dev-etl-sk];

/*These commands grant permissions to the database roles and the specific user/group [dev-etl-sp]. 
It allows them to perform read, write, and schema modification operations (SELECT, UPDATE, DELETE, INSERT, ALTER, CONTROL) in the SalesLt schema 
and administers database bulk operations.
This script essentially sets up the necessary keys, credentials, and permissions to facilitate bulk loading operations from Azure Blob Storage
 into the database schema named SalesLt.*/
 
 ---------------------------------------------------------------------------------------------------------------------------------------------------
 -- Drop the stored procedure if it exists
IF OBJECT_ID('saleslt.sp_load_stg', 'P') IS NOT NULL
    DROP PROCEDURE saleslt.sp_load_stg;
GO

-- Create the stored procedure sp_load_stg
CREATE PROCEDURE saleslt.sp_load_stg
(
    @stgtablename varchar(4000),
    @filepath varchar(4000)
)
AS
BEGIN
    DECLARE @sql varchar(4000);

    -- Truncate the target table
    SET @sql = 'TRUNCATE TABLE ' + @stgtablename;
    EXEC (@sql);

    -- Perform bulk insert into the target table
    SET @sql = 'BULK INSERT ' + @stgtablename +
               ' FROM ' + '''' + @filepath + '''' +
               ' WITH (' +
               '   DATA_SOURCE = ''DevStorage'',' +
               '   DATAFILETYPE = ''char'',' +
               '   FIELDTERMINATOR = '','''',' +
               '   ROWTERMINATOR = ''\n''' +
               ')';
    EXEC (@sql);
END
GO

/*This script creates a stored procedure named sp_load_stg in the saleslt schema. This procedure takes in two parameters: @stgtablename 
4(the target table name) and @filepath (the file path for bulk loading).

Inside the procedure, it truncates the target table specified by @stgtablename and then performs a bulk insert operation into that table from 
the file specified by @filepath, using the BULK INSERT SQL command.
 Adjust the parameters and options in the BULK INSERT statement as needed for your specific use case.*/
 
 ---------------------------------------------------------------------------------------------------------------------------
pipeline stg to tgt 

Lab Script - Metadata Table Configurations
1. Config.Jobs

==========
insert into config.jobs (job,active)
values ('load_customer','Y')

2. Config.Tasks
===========
insert into config.tasks(task,active)
values ('pl_load_into_tgt','Y')

3. Config.Job_Tasks
===============
insert into config.job_tasks(job,task,task_sequence,active)
values ('load_customer','pl_load_into_tgt',3,'Y')

4. Config.Task_Parameters
====================
insert into config.task_parameters(task,parameter,parameter_type,active)
values ('pl_load_into_tgt','sp_name','static','Y')


5. Config.Job_Task_Parameters
========================
insert into config.job_task_parameters(job,task,parameter,value,active,task_sequence)
values ('load_customer','pl_load_into_tgt','sp_name','saleslt.sp_load_customer','Y',3)

-- Drop the table if it already exists
IF OBJECT_ID('saleslt.Customer_Target', 'U') IS NOT NULL
    DROP TABLE saleslt.Customer_Target;

GO

-- Create the Customer_Target table
CREATE TABLE saleslt.Customer_Target (
    [CustomerID] [varchar](4000),
    [NameStyle] [varchar](4000),
    [Title] [varchar](4000),
    [FirstName] [varchar](4000),
    [MiddleName] [varchar](4000),
    [LastName] [varchar](4000),
    [Suffix] [varchar](4000),
    [CompanyName] [varchar](4000),
    [SalesPerson] [varchar](4000),
    [EmailAddress] [varchar](4000),
    [Phone] [varchar](4000),
    [PasswordHash] [varchar](4000),
    [PasswordSalt] [varchar](4000),
    [rowguid] [varchar](4000),
    [ModifiedDate] [varchar](4000)
);
--------------------------------------------------------------------------------------------------------

load_runner_pipeline

insert into config.environment(parameter, value)
values ('subscription', 'c16facec-26d0-4d53-89de-f460cfd5a370');

insert into config.environment(parameter, value)
values ('rg','Metadatalaab');

insert into config.environment(parameter, value)
values ('adf','Etlmetadata');

---------------------------------------------------------
IF OBJECT_ID('control.sp_get_tasks', 'P') IS NOT NULL
    DROP PROCEDURE control.sp_get_tasks;
GO

CREATE PROCEDURE control.sp_get_tasks
(
    @job_id varchar(4000),
    @job varchar(4000)
)
AS
BEGIN
    DECLARE @job_date date, @error varchar(4000), @tasks int;

    SET @job_date = GETDATE();

    /* Consider adding logging within sp_job_task_log */
    
    IF NOT EXISTS (
        SELECT job
        FROM config.job_tasks
        WHERE job = @job
        AND active = 'Y'
    )
    BEGIN
        SET @error = 'No active tasks found for the job';
        EXEC control.sp_job_task_log @job_id, '', @job, '', '', 'ERROR', '', @error;
        THROW 50001, @error, 1;
    END

    SELECT job, task, task_sequence
    FROM config.job_tasks
    WHERE job = @job
    AND active = 'Y';
END
GO

/*This script creates a stored procedure control.sp_get_tasks that takes @job_id and @job parameters. It retrieves active tasks associated with the specified job from the config.job_tasks table. If no active tasks are found for the provided job, it logs an error using sp_job_task_log and throws an exception.

Adjust the logic and error handling as needed for your specific requirements and ensure that the sp_job_task_log procedure exists and handles logging appropriately*/

-----------------------------------------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('control.sp_init_job_control', 'P') IS NOT NULL
    DROP PROCEDURE control.sp_init_job_control;
GO

CREATE PROCEDURE control.sp_init_job_control
(
    @job_id varchar(4000),
    @job varchar(4000)
)
AS
BEGIN
    DECLARE @job_dt date, @error varchar(4000);

    SET @job_dt = GETDATE();

    IF NOT EXISTS (
        SELECT 1
        FROM config.jobs
        WHERE job = @job
        AND active = 'Y'
    )
    BEGIN
        SET @error = 'Job ' + @job + ' is inactive in control.jobs table';
        THROW 50001, @error, 1;
    END

    IF EXISTS (
        SELECT 1
        FROM control.job_task_control
        WHERE job = @job
        AND task_status IN ('RUNNING', 'SCHEDULED')
    )
    BEGIN
        SET @error = 'Previous job run for ' + @job + ' is not yet complete';
        THROW 50002, @error, 1;
    END

    INSERT INTO control.job_task_control
    (
        job_id,
        task_id,
        job,
        job_dt,
        task,
        task_sequence,
        task_status
    )
    SELECT
        @job_id,
        '0',
        @job,
        @job_dt,
        task,
        task_sequence,
        'SCHEDULED'
    FROM config.job_tasks
    WHERE job = @job
    AND active = 'Y'
    ORDER BY task_sequence;
END
GO
-------------------------------------------------
enviorment variable 
Select * from
(
select parameter, value
from config.enviornment) as i
pivot 
(
 max(value) for parameter in (subscription,rg,adf)) as o
 ----------------------------
-- in orchestration
 IF OBJECT_ID('control.sp_task_status', 'P') IS NOT NULL
    DROP PROCEDURE control.sp_task_status;
GO

CREATE PROCEDURE control.sp_task_status
(
    @task_status varchar(4000)
)
AS
BEGIN
    IF @task_status = 'ERROR'
    BEGIN
        THROW 50001, 'Prior task has failed execution, unable to start next task run', 1;
    END
END
GO
-----------------------------------------------------

--web activitu
@concat(
'https://management.azure.com/subscriptions/',
activity('get_enviornment_variable').output.firstRow.subscription,
'/resourceGroups/',
activity('get_enviornment_variable').output.firstRow.rg,
'/providers/Microsoft.DataFactory/factories/',
activity('get_enviornment_variable').output.firstRow.adf,
'/pipelines/',
item().tasks,
'/createRun?api-version=2018-06-01'
)

@concat (
'{ ',
'"p_job" : "', item().job,
'" , "p_job_id" : "', variables()'v_job_id'),
'" , "p_task_sequence" : "', item().task_sequence,
'" }'
)


@concat(
'https://management.azure.com/subscriptions/',
activity('get_enviornment_variable').output.firstRow.subscription,
'/resourceGroups/',
activity('get_enviornment_variable').output.firstRow.rg,
'/providers/Microsoft.DataFactory/factories/',
activity('get_enviornment_variable').output.firstRow.adf,
'/pipelines/',
pipeline().parameters.p_run_id,
'/createRun?api-version=2018-06-01'
)