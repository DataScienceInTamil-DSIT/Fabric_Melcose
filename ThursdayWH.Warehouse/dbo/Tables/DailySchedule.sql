CREATE TABLE [dbo].[DailySchedule] (

	[ScheduleID] int NULL, 
	[ActivityName] varchar(100) NULL, 
	[StartTime] time(1) NULL, 
	[EndTime] time(3) NULL, 
	[BreakTime] time(3) NULL, 
	[Duration] int NULL
);