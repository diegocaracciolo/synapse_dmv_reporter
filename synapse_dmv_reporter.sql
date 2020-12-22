/****** Object:  StoredProcedure [dbo].[synapse_dmv_reporter]    Script Date: 10/21/2020 3:50:13 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.ROUTINES where ROUTINE_NAME = 'synapse_dmv_reporter' AND ROUTINE_SCHEMA = 'SYNAPSE_TOOLKIT')
       DROP PROCEDURE [SYNAPSE_TOOLKIT].[synapse_dmv_reporter]; 
GO

CREATE PROC [SYNAPSE_TOOLKIT].[synapse_dmv_reporter] @label [nvarchar](255), @requestid [nvarchar](32) AS
-- ============================================================================================================================================
-- General syntax use:	EXEC [SYNAPSE_TOOLKIT].[synapse_dmv_reporter] NULL, NULL
--						EXEC [SYNAPSE_TOOLKIT].[synapse_dmv_reporter] 'query label', NULL
--						EXEC [SYNAPSE_TOOLKIT].[synapse_dmv_reporter] NULL, QID123456
-- 
-- Author:      Diego Caracciolo, Customer Architecture and Engineering (CAE), Microsoft
-- Create Date: 10/20/2020
-- Description: This sproc compiles several queries -- to DMVs that provide
-- helpful information for troubleshooting long running operations in
-- Synapse Dedicated SQL Pools based on either a query label or a QID
-- ============================================================================================================================================

BEGIN
SET NOCOUNT ON

-- Declare variables
DECLARE @reqid nvarchar(32) = @requestid
DECLARE @optype nvarchar(35)
DECLARE @stepindex INT
DECLARE @sessionid nvarchar(32)

IF ((@requestid IS NULL or @requestid = '') AND @label IS NULL or @label = '') -- We show everything that is running as we haven't been provided with a label nor a QID
BEGIN
	
	PRINT 'We show everything that is running as we have not been provided with a label nor a QID'

	-- Output all the requests and sessions that are running
	SELECT r.request_id, r.session_id, r.[label], r.resource_class, r.[status], r.submit_time, r.start_time, r.end_compile_time, r.total_elapsed_time/60000 as total_elapsed_time_minutes, 
		r.total_elapsed_time/1000 as total_elapsed_time_seconds, r.command
	FROM sys.dm_pdw_exec_requests AS r
		JOIN sys.dm_pdw_exec_sessions AS s ON r.SESSION_ID = s.SESSION_ID
	WHERE r.status ='running'

END
IF ((@requestid IS NULL or @requestid = '') AND @label IS NOT NULL ) -- We work with @label and report based on that label for a running request
BEGIN

	PRINT 'We work with @label and report based on that label, not a QID'
	
	-- Obtain request_id_/QID based on the label of a running query or sproc
	SELECT @reqid = r.request_id
	, @sessionid = r.session_id
	FROM sys.dm_pdw_exec_requests AS r
		JOIN sys.dm_pdw_exec_sessions AS s ON r.SESSION_ID = s.SESSION_ID
	WHERE 1=1
		AND r.status ='running'
		AND r.[label] LIKE '%'+ @label + '%'

	-- Output the full session steps
	SELECT * , total_elapsed_time/1000 as total_elapsed_time_seconds, total_elapsed_time/60000 as total_elapsed_time_minutes
	FROM sys.dm_pdw_exec_requests
	WHERE session_id = @sessionid
	ORDER BY submit_time DESC	

	--​ Output the steps for the obtained request_id/QID
	SELECT *, total_elapsed_time/1000 as total_elapsed_time_seconds, total_elapsed_time/60000 as total_elapsed_time_minutes
	FROM sys.dm_pdw_request_steps
	WHERE request_id = @reqid
		--AND status = 'Running'

	-- Assign values to the valiables for the running request_id
	SELECT @optype = operation_type, @stepindex = step_index
	FROM sys.dm_pdw_request_steps
	WHERE request_id = @reqid
		AND status = 'Running'

	-- Output the sessions working for the identified request_id
	SELECT --#RemoveThisMetaQueryInResult
		Ses.session_id ,Ses.request_id
	,Req.[label],Req.command
	,steps.operation_type ,steps.distribution_type ,steps.[status] ,steps.start_time ,steps.end_time ,steps.row_count ,steps.command
	FROM sys.dm_pdw_exec_requests Req
		join sys.dm_pdw_exec_sessions Ses
			ON Ses.request_id=Req.request_id
		join sys.dm_pdw_request_steps steps
			ON steps.request_id=Req.request_id
	WHERE steps.request_id = @reqid
		AND Req.command not like '%RemoveThisMetaQueryInResult%'
	ORDER BY steps.start_time

	IF @optype IN ('OnOperation','RemoteOperation','ReturnOperation')
	BEGIN
		PRINT 'Running a SQL Operation'
		PRINT @optype
		PRINT @stepindex
		-- Output information from dm_pdw_nodes_exec_requests related to the nodes that are executing a SQL request for this request_id/QID
		SELECT DISTINCT
			getdate() as CaptureTime,
			per.request_id, per.session_id, per.[status] as PERStatus, per.start_time, per.total_elapsed_time, per.database_id, per.command as PERCommand,
			per.resource_class, psr.step_index, psr.distribution_ID, psr.[status] as PSRStatus, psr.Start_time as PSR_StartTime,
			psr.total_elapsed_time as PSRTotal_Elapsed_Time, psr.command as  PSRCommand, ser.session_id as CMPSPID, ser.start_time as CMPStartTime,
			ser.[status] as CMPStatus, ser.blocking_session_id as CMPBlocking_SPID, ser.wait_type as CMPWait_type, ser.wait_time as CMPwait_time,
			ser.last_wait_type as CMPLast_Wait_Type, ser.wait_resource as CMPWait_resource, ser.cpu_time as CMPcpu_time, ser.total_elapsed_time as CMPtotal_elapsed_time,
			ser.reads as CMPReads, ser.writes as CMPWrites, ser.logical_reads as CMPlogical_reads, ser.granted_query_memory as CMPgranted_query_memory,
			ser.pdw_node_id as PDW_node_id, ser.writes
		FROM sys.dm_pdw_exec_requests per
			INNER JOIN sys.dm_pdw_sql_requests psr
				ON per.request_id = psr.request_id
			INNER JOIN sys.dm_pdw_nodes_exec_requests ser
				ON psr.spid = ser.session_id
				AND ser.pdw_node_id = psr.pdw_node_id
		WHERE per.status='Running' AND per.request_id = @reqid
	END

	IF @optype IN ('ShuffleMoveOperation','BroadcastMoveOperation','TrimMoveOperation','PartitionMoveOperation','MoveOperation','CopyOperation')
	BEGIN
		PRINT 'Running a Data Movement OperatiON of type: ' + @optype
		PRINT 'Running step_index: ' + CONVERT(char,@stepindex)
		-- Output information about all the workers completing a Data Movement Step
		SELECT DISTINCT @stepindex AS step_index, *
		FROM sys.dm_pdw_exec_requests per               
			INNER JOIN sys.dm_pdw_request_steps prs               
				ON per.request_id = prs.request_id       
			left outer join sys.dm_pdw_dms_workers pdw                
				ON prs.request_id = pdw.request_id       
				AND prs.step_index = pdw.step_index
		WHERE per.request_id=@reqid
		ORDER BY per.request_id, prs.step_index, pdw.dms_step_index
	END
END

IF (@label IS NULL OR @label = '') AND @requestid IS NOT NULL --  We work with a QID and report based on it whether is running or not 
BEGIN

	PRINT 'We work with a QID and report based on that, not a label'
	
	PRINT 'Request ID is: ' + CONVERT(char,@reqid)

	--​ Obtain the steps for the request_id/QID
	SELECT *, total_elapsed_time/1000 as total_elapsed_time_seconds, total_elapsed_time/60000 as total_elapsed_time_minutes
	FROM sys.dm_pdw_request_steps
	WHERE request_id = @reqid
		--AND status = 'Running'

	-- Assign values to the valiables for the running 
	SELECT @optype = operation_type, @stepindex = step_index
	FROM sys.dm_pdw_request_steps
	WHERE request_id = @reqid
		AND status = 'Running'

	-- Output the sessions working for the identified request_id
	SELECT --#RemoveThisMetaQueryInResult
		Ses.session_id ,Ses.request_id
	,Req.[label],Req.command
	,steps.operation_type ,steps.distribution_type ,steps.[status] ,steps.start_time ,steps.end_time ,steps.row_count ,steps.command
	FROM sys.dm_pdw_exec_requests Req
		join sys.dm_pdw_exec_sessions Ses
			ON Ses.request_id=Req.request_id
		join sys.dm_pdw_request_steps steps
			ON steps.request_id=Req.request_id
	WHERE steps.request_id = @reqid
		AND Req.command not like '%RemoveThisMetaQueryInResult%'
	ORDER BY steps.start_time

	IF @optype IN ('OnOperation','RemoteOperation','ReturnOperation')
	BEGIN
		PRINT 'Running a SQL Operation'
		PRINT @optype
		PRINT @stepindex
		-- Output information from dm_pdw_nodes_exec_requests related to the nodes that are executing a SQL request for this request_id/QID
		SELECT DISTINCT
			getdate() as CaptureTime,
			per.request_id, per.session_id, per.[status] as PERStatus, per.start_time, per.total_elapsed_time, per.database_id, per.command as PERCommand,
			per.resource_class, psr.step_index, psr.distribution_ID, psr.[status] as PSRStatus, psr.Start_time as PSR_StartTime,
			psr.total_elapsed_time as PSRTotal_Elapsed_Time, psr.command as  PSRCommand, ser.session_id as CMPSPID, ser.start_time as CMPStartTime,
			ser.[status] as CMPStatus, ser.blocking_session_id as CMPBlocking_SPID, ser.wait_type as CMPWait_type, ser.wait_time as CMPwait_time,
			ser.last_wait_type as CMPLast_Wait_Type, ser.wait_resource as CMPWait_resource, ser.cpu_time as CMPcpu_time, ser.total_elapsed_time as CMPtotal_elapsed_time,
			ser.reads as CMPReads, ser.writes as CMPWrites, ser.logical_reads as CMPlogical_reads, ser.granted_query_memory as CMPgranted_query_memory,
			ser.pdw_node_id as PDW_node_id, ser.writes
		FROM sys.dm_pdw_exec_requests per
			INNER JOIN sys.dm_pdw_sql_requests psr
				ON per.request_id = psr.request_id
			INNER JOIN sys.dm_pdw_nodes_exec_requests ser
				ON psr.spid = ser.session_id
				AND ser.pdw_node_id = psr.pdw_node_id
		WHERE per.status='Running' AND per.request_id = @reqid
	END

	IF @optype IN ('ShuffleMoveOperation','BroadcastMoveOperation','TrimMoveOperation','PartitionMoveOperation','MoveOperation','CopyOperation')
	BEGIN
		PRINT 'Running a Data Movement OperatiON of type: ' + @optype
		PRINT 'Running step_index: ' + CONVERT(char,@stepindex)
		-- Find informatiON about all the workers completing a Data Movement Step
		SELECT DISTINCT @stepindex AS step_index, *
		FROM sys.dm_pdw_exec_requests per               
			INNER JOIN sys.dm_pdw_request_steps prs               
				ON per.request_id = prs.request_id       
			left outer join sys.dm_pdw_dms_workers pdw                
				ON prs.request_id = pdw.request_id       
				AND prs.step_index = pdw.step_index
		WHERE per.request_id=@reqid
		ORDER BY per.request_id, prs.step_index, pdw.dms_step_index
	END
END

END
GO


