drop database if exists employeeDB;
create database employeeDB;
use employeeDB;
drop table if exists employee;
drop table if exists practice;
drop table if exists status;

create table if not exists Practice (id int primary key,name varchar(20));
create table if not exists Status (id int primary key,name varchar(20));

show tables;

create table if not exists Employee 
(
	id int primary key,
	name varchar(50) not null,
	practiceId int not null,
	statusId int null,
	phoneNumber bigint not null CONSTRAINT TenDigits CHECK (phoneNumber BETWEEN 1000000000 and 9999999999),
	IsDeleted int default 0,
	foreign key (practiceId) references practice(id),	
	foreign key (statusId) references status(id)
);

desc Employee;

insert into practice values
(1,'Data Engineer'),
(2,'Appdev'),
(3,'Testing');


insert into status values
(1,'Full time'),
(2,'Part time'),
(3,'Notice period'),
(4,'Contractor'),
(5,'Terminated');


drop procedure if exists employee_proc;
create procedure employee_proc(in id int,in name varchar(50),in PracticeID int,in statusID int,in phonenumber bigint)
begin 	
	insert into employee values(id, name, PracticeID, statusID, phonenumber,0);
end;

call employee_proc(1,"Yuvateja Reddy",1,1,8978243567);
call employee_proc(6,"Naveen",20,400,7777777777);

drop trigger if exists trigger_validate_practice_status;

create trigger trigger_validate_practice_status
before insert on Employee
for each row
begin	
	INSERT INTO practice (id, name)
SELECT * FROM (SELECT new.practiceId,'NO NAME') as tmp
WHERE NOT EXISTS (
    select p.id from practice p  WHERE p.id = new.practiceId
) LIMIT 1;
INSERT INTO status(id, name)
SELECT * FROM (SELECT new.statusId,'NO NAME') as tmp
WHERE NOT EXISTS (
    SELECT s.id FROM status s  WHERE s.id = new.statusId
) LIMIT 1;
end;


select * from employee;
select * from status;
select * from practice;

