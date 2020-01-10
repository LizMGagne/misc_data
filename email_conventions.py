import sys
import re
import os
import pandas as pd

CONVENTIONS = ('`fn``ln`' , '`fi``ln`', '`fn``li`', '`fn`.`ln`', '`fn`_`ln`', '`fn`-`ln`', '`fn2``ln`', '`fn2``ln6`', 
	'`fi``ln6`', '`fi``ln7`', '`fn``ln7`', '`fi``ln`' , '`ln``fn`' , '`ln``fi`', '`ln`.`fn`', '`ln`_`fn`' , '`ln`-`fn`' , 
	'`ln``fn2`', '`ln6``fn2`', '`ln6``f1`', '`ln7``f1`', '`ln``fi`', '`li``fn`', '`fi`.`ln`', '`fi`-`ln`', '`fi``mi``ln`', 
	'`fn2``mi``ln`', '`fn``mi``ln`', '`fn``mi``ln`', '`fn``mi``fln`', '`fi``mi``fln`', '`fi``mi``ln4`', '`fi``mi``ln6`', 
	'`ln``fn3`', '`fn``fln`', '`fn`.`fln`'  , '`ln`'  , '`fn`'  , '`ln`' , '`fn`.`ln`', '`fn`_`ln`', '`fn`-`ln`', '`fn``ln`', 
	'`fi`.`ln`', '`fi`_`ln`', '`fi``ln`', '`fn2``ln`', '`fn2``ln6`', '`fi``ln6`', '`ln6``fi`', '`fi``ln7`', '`fn``ln7`', 
	'`fn``li`', '`mn``ln`', '`mn`.`ln`', '`mn`_`ln`', '`mn`-`ln`', '`mi``ln`', '`mi`.`ln`', '`mi`-`ln`', '`mi`_`ln`', 
	'`ln4``fn3`', '`ln5``fn3`', '`fn``flnhy`', '`fn`.`flnhy`', '`fn`_`flnhy`', '`fn`-`flnhy`', '`fi``flnhy`', '`fi`.`flnhy`', 
	'`fi`_`flnhy`', '`fi`-`flnhy`', '`fn``slnhy`', '`fn`.`slnhy`', '`fn`_`slnhy`', '`fn`-`slnhy`', '`fi``slnhy`', '`fi`.`slnhy`', 
	'`fi`_`slnhy`', '`fi`-`slnhy`', '`fi``slnhy7`', '`slnhy``fn`', '`slnhy`.`fn`', '`slnhy`_`fn`', '`slnhy`-`fn`', '`slnhy``fi`', 
	'`slnhy`.`fi`', '`slnhy`_`fi`', '`slnhy`-`fi`', '`sln``fn`', '`sln`.`fn`', '`sln`_`fn`', '`sln`-`fn`', '`sln``fi`', '`sln`.`fi`', 
	'`sln`_`fi`', '`sln`-`fi`', '`fn``sln`', '`fn`.`sln`', '`fn`_`sln`', '`fn`-`sln`', '`fi``sln`', '`fi`.`sln`', '`fi`_`sln`', 
	'`fi`-`sln`', '`fi``sln7`'  , '`fn`.*.`ln`', '`fn`*`ln`', '`fi``li`*', '`fn2``li`*', '`fn``ln`*', '`fn`.`ln`*', '`fn`_`ln`*', 
	'`fi``ln`*', '`fi`*`ln`*', '`fi`*`ln`', '`fi``li`*', '`fi``mi``li`*', '`fi`*`li`*', '`ln5`*', '`ln4`*', '`fn`*') 

class Translator(dict):
	def create_regex(self):
		return re.compile('|'.join(map(re.escape, self.keys())))

	def __call__(self,match):
		return self.get(match.group(0),'')

	def translate(self,s):
		return self.create_regex().sub(self,s)	

replace_errors = Translator({
	' ':'',
	'__':'_',
	"'":'',
	'..':'.',
})


def get_db_conventions(amaid=None, organizationid=None,max_conventions=None):
	assert any((amaid,organizationid))
	# prefer organizationid because it yields a wider pattern
	criterion = f'c.organizationid = {organizationid}' if organizationid else f'gme.amaid = {amaid}'
	sql = f'''select
			(ifnull(count(clicked) / count(sent),0) + ifnull(count(opened)/count(sent),0) - ifnull(count(bouncestring)/count(sent),0) ) * count(sent) total_ratio,		   
			convention, domain, organizationid, programname from (
			select gme.userid, gme.amaid, programname,
			firstname, middlename, maidenname, lastname, 
			gme.transitionyear, c.organizationid, ce.email, ce.emailid, 
			substring_index(ce.email,'@',-1) domain, substring_index(ce.email,'@',1) username,
			coalesce(msr.stamp) sent, 
			coalesce(eo.stamp) opened, 
			coalesce(nlc.ctrdate) clicked, 
			coalesce(
				case 
			    when b.bouncestring like '%%hop count%%' then null
				when b.bouncestring not like '%%many hops%%' then null 
				when b.bouncestring not like '%%out%%office%%' then null
				when b.bouncestring not like '%%reach me%%' then null
				when b.bouncestring not like '%%delayed%%' then null
				when b.bouncestring not like '%%Probe failed%%' then null
				when b.bouncestring not like '%%mailbox full%%' then null
				when b.bouncestring not like '%%vacation%%' then null
				when b.bouncestring not like '%%i will %%' then null
				when b.bouncestring not like '%%i am %%' then null
				when b.bouncestring not like '%%too many recipients%%' then null
				when b.bouncestring not like '%%thank you%%' then null
				when b.bouncestring not like '%%too large%%' then null
				when b.bouncestring not like '%%policy%%' then null
				when b.bouncestring not like '%%access denied%%' then null
				when b.bouncestring not like '%%time expired%%' then null
			    else bouncestring end) bouncestring,
			case when te.teid is not null then 1 else 0 end is_trial,
			case when substring_index(ce.email,'@',1) = concat(firstname,replace(lastname,' ','')) then '`fn``ln`' 
				when substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),replace(lastname,' ','')) then '`fi``ln`'
				when substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),left(replace(lastname,' ',''),1)) then '`fn``li`'
				when substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),'.',replace(lastname,' ','')) then '`fn`.`ln`'  
				when substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),'_',replace(lastname,' ','')) then '`fn`_`ln`' 
				when substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),'-',replace(lastname,' ','')) then '`fn`-`ln`' 
				when substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),2),replace(lastname,' ','')) then '`fn2``ln`'
				when substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),2),left(replace(lastname,' ',''),6)) then '`fn2``ln6`'
				when substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),left(replace(lastname,' ',''),6)) then '`fi``ln6`'
				when substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),left(replace(lastname,' ',''),7)) then '`fi``ln7`'
				when substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),left(replace(lastname,' ',''),7)) then '`fn``ln7`'  
				when substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),replace(lastname,' ','')) then '`fi``ln`' 
				when substring_index(ce.email,'@',1) = concat(replace(lastname,' ',''),replace(firstname,' ','')) then '`ln``fn`' 
				when substring_index(ce.email,'@',1) = concat(replace(lastname,' ',''),left(replace(firstname,' ',''),1)) then '`ln``fi`'  
				when substring_index(ce.email,'@',1) = concat(replace(lastname,' ',''),'.',replace(firstname,' ','')) then '`ln`.`fn`'  
				when substring_index(ce.email,'@',1) = concat(replace(lastname,' ',''),'_',replace(firstname,' ','')) then '`ln`_`fn`' 
				when substring_index(ce.email,'@',1) = concat(replace(lastname,' ',''),'-',replace(firstname,' ','')) then '`ln`-`fn`' 
				when substring_index(ce.email,'@',1) = concat(replace(lastname,' ',''),left(replace(firstname,' ',''),2)) then '`ln``fn2`'    
				when substring_index(ce.email,'@',1) = concat(left(replace(lastname,' ',''),6),left(replace(firstname,' ',''),2)) then '`ln6``fn2`'
				when substring_index(ce.email,'@',1) = concat(left(replace(lastname,' ',''),6),left(replace(firstname,' ',''),1)) then '`ln6``f1`'
				when substring_index(ce.email,'@',1) = concat(left(replace(lastname,' ',''),7),left(replace(firstname,' ',''),1)) then '`ln7``f1`'
				when substring_index(ce.email,'@',1) = concat(replace(lastname,' ',''),left(replace(firstname,' ',''),1)) then '`ln``fi`'     
				when substring_index(ce.email,'@',1) = concat(left(replace(lastname,' ',''),1),replace(firstname,' ','')) then '`li``fn`'    
				when substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),'.',replace(lastname,' ','')) then '`fi`.`ln`'
				when substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),'-',replace(lastname,' ','')) then '`fi`-`ln`'    
				when substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),left(replace(ifnull(middlename,''),' ',''),1),replace(lastname,' ','')) then '`fi``mi``ln`'
				when substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),2),left(replace(ifnull(middlename,''),' ',''),1),replace(lastname,' ','')) then '`fn2``mi``ln`'
				when substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),left(replace(ifnull(middlename,''),' ',''),1),replace(lastname,' ','')) then '`fn``mi``ln`'
				when substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),'.',left(replace(ifnull(middlename,''),' ',''),1),'.',replace(lastname,' ','')) then '`fn``mi``ln`'
				when substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),left(replace(ifnull(middlename,''),' ',''),1),substring_index(lastname,' ',1)) then '`fn``mi``fln`'
				when substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),left(replace(ifnull(middlename,''),' ',''),1),substring_index(lastname,' ',1)) then '`fi``mi``fln`'
				when substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),left(replace(ifnull(middlename,''),' ',''),1),left(replace(lastname,' ',''),4)) then '`fi``mi``ln4`'
				when substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),left(replace(ifnull(middlename,''),' ',''),1),left(replace(lastname,' ',''),6)) then '`fi``mi``ln6`'
				when substring_index(ce.email,'@',1) = concat(replace(lastname,' ',''),left(replace(firstname,' ',''),3)) then '`ln``fn3`'
				when substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),substring_index(lastname,' ',1)) then '`fn``fln`'
				when substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),'.',substring_index(lastname,' ',1)) then '`fn`.`fln`'  
				when substring_index(ce.email,'@',1) = replace(lastname,' ','') then '`ln`'  
				when substring_index(ce.email,'@',1) = replace(firstname,' ','') then '`fn`'  
				when substring_index(ce.email,'@',1) = replace(maidenname,' ','') then '`ln`' 
				when substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),'.',replace(ifnull(maidenname,''),' ','')) then '`fn`.`ln`'
				when substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),'_',replace(ifnull(maidenname,''),' ','')) then '`fn`_`ln`'
				when substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),'-',replace(ifnull(maidenname,''),' ','')) then '`fn`-`ln`'
				when substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),replace(ifnull(maidenname,''),' ','')) then '`fn``ln`'
				when substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),'.',replace(ifnull(maidenname,''),' ','')) then '`fi`.`ln`'
				when substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),'_',replace(ifnull(maidenname,''),' ','')) then '`fi`_`ln`'
				when substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),replace(ifnull(maidenname,''),' ','')) then '`fi``ln`'
				
				when substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),2),replace(maidenname,' ','')) then '`fn2``ln`'
				when substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),2),left(replace(maidenname,' ',''),6)) then '`fn2``ln6`'
				when substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),left(replace(maidenname,' ',''),6)) then '`fi``ln6`'
				when substring_index(ce.email,'@',1) = concat(left(replace(maidenname,' ',''),6),left(replace(firstname,' ',''),1)) then '`ln6``fi`'
				when substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),left(replace(maidenname,' ',''),7)) then '`fi``ln7`'
				when substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),left(replace(maidenname,' ',''),7)) then '`fn``ln7`'
				when substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),left(replace(maidenname,' ',''),1)) then '`fn``li`'
				
					
				when substring_index(ce.email,'@',1) = concat(replace(ifnull(middlename,''),' ',''), replace(lastname,' ','')) then '`mn``ln`'
				when substring_index(ce.email,'@',1) = concat(replace(ifnull(middlename,''),' ',''),'.', replace(lastname,' ','')) then '`mn`.`ln`'
				when substring_index(ce.email,'@',1) = concat(replace(ifnull(middlename,''),' ',''),'_', replace(lastname,' ','')) then '`mn`_`ln`'
				when substring_index(ce.email,'@',1) = concat(replace(ifnull(middlename,''),' ',''),'-', replace(lastname,' ','')) then '`mn`-`ln`'
				when substring_index(ce.email,'@',1) = concat(left(replace(ifnull(middlename,''),' ',''),1), replace(lastname,' ','')) then '`mi``ln`'
				when substring_index(ce.email,'@',1) = concat(left(replace(ifnull(middlename,''),' ',''),1),'.', replace(lastname,' ','')) then '`mi`.`ln`'
				when substring_index(ce.email,'@',1) = concat(left(replace(ifnull(middlename,''),' ',''),1),'-', replace(lastname,' ','')) then '`mi`-`ln`'
				when substring_index(ce.email,'@',1) = concat(left(replace(ifnull(middlename,''),' ',''),1),'_', replace(lastname,' ','')) then '`mi`_`ln`'
				when substring_index(ce.email,'@',1) = concat(left(replace(lastname,' ',''),4),left(replace(firstname,' ',''),3)) then '`ln4``fn3`'
				when substring_index(ce.email,'@',1) = concat(left(replace(lastname,' ',''),5),left(replace(firstname,' ',''),3)) then '`ln5``fn3`'
				
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),substring_index(lastname,'-',1)) then '`fn``flnhy`'
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),'.',substring_index(lastname,'-',1)) then '`fn`.`flnhy`'
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),'_',substring_index(lastname,'-',1)) then '`fn`_`flnhy`'
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),'-',substring_index(lastname,'-',1)) then '`fn`-`flnhy`'
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),substring_index(lastname,'-',1)) then '`fi``flnhy`'
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),'.',substring_index(lastname,'-',1)) then '`fi`.`flnhy`'
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),'_',substring_index(lastname,'-',1)) then '`fi`_`flnhy`'
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),'-',substring_index(lastname,'-',1)) then '`fi`-`flnhy`'
				
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),substring_index(lastname,'-',-1)) then '`fn``slnhy`'
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),'.',substring_index(lastname,'-',-1)) then '`fn`.`slnhy`'
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),'_',substring_index(lastname,'-',-1)) then '`fn`_`slnhy`'
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),'-',substring_index(lastname,'-',-1)) then '`fn`-`slnhy`'
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),substring_index(lastname,'-',-1)) then '`fi``slnhy`'
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),'.',substring_index(lastname,'-',-1)) then '`fi`.`slnhy`'
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),'_',substring_index(lastname,'-',-1)) then '`fi`_`slnhy`'
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),'-',substring_index(lastname,'-',-1)) then '`fi`-`slnhy`'
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),left(substring_index(lastname,' ',-1),7)) then '`fi``slnhy7`'
				
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(substring_index(lastname,'-',-1),replace(firstname,' ','')) then '`slnhy``fn`'
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(substring_index(lastname,'-',-1),'.',replace(firstname,' ','')) then '`slnhy`.`fn`'
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(substring_index(lastname,'-',-1),'_',replace(firstname,' ','')) then '`slnhy`_`fn`'
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(substring_index(lastname,'-',-1),'-',replace(firstname,' ','')) then '`slnhy`-`fn`'
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(substring_index(lastname,'-',-1),left(replace(firstname,' ',''),1)) then '`slnhy``fi`'
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(substring_index(lastname,'-',-1),left(replace(firstname,' ',''),1)) then '`slnhy`.`fi`'
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(substring_index(lastname,'-',-1),'_',left(replace(firstname,' ',''),1)) then '`slnhy`_`fi`'
				when lastname like '%%-%%' and substring_index(ce.email,'@',1) = concat(substring_index(lastname,'-',-1),'-',left(replace(firstname,' ',''),1)) then '`slnhy`-`fi`'
				
				when lastname like '%% %%' and substring_index(ce.email,'@',1) = concat(substring_index(lastname,' ',-1),replace(firstname,' ','')) then '`sln``fn`'
				when lastname like '%% %%' and substring_index(ce.email,'@',1) = concat(substring_index(lastname,' ',-1),'.',replace(firstname,' ','')) then '`sln`.`fn`'
				when lastname like '%% %%' and substring_index(ce.email,'@',1) = concat(substring_index(lastname,' ',-1),'_',replace(firstname,' ','')) then '`sln`_`fn`'
				when lastname like '%% %%' and substring_index(ce.email,'@',1) = concat(substring_index(lastname,' ',-1),'-',replace(firstname,' ','')) then '`sln`-`fn`'
				when lastname like '%% %%' and substring_index(ce.email,'@',1) = concat(substring_index(lastname,' ',-1),left(replace(firstname,' ',''),1)) then '`sln``fi`'
				when lastname like '%% %%' and substring_index(ce.email,'@',1) = concat(substring_index(lastname,' ',-1),left(replace(firstname,' ',''),1)) then '`sln`.`fi`'
				when lastname like '%% %%' and substring_index(ce.email,'@',1) = concat(substring_index(lastname,' ',-1),'_',left(replace(firstname,' ',''),1)) then '`sln`_`fi`'
				when lastname like '%% %%' and substring_index(ce.email,'@',1) = concat(substring_index(lastname,' ',-1),'-',left(replace(firstname,' ',''),1)) then '`sln`-`fi`'
				
				when lastname like '%% %%' and substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),substring_index(lastname,' ',-1)) then '`fn``sln`'
				when lastname like '%% %%' and substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),'.',substring_index(lastname,' ',-1)) then '`fn`.`sln`'
				when lastname like '%% %%' and substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),'_',substring_index(lastname,' ',-1)) then '`fn`_`sln`'
				when lastname like '%% %%' and substring_index(ce.email,'@',1) = concat(replace(firstname,' ',''),'-',substring_index(lastname,' ',-1)) then '`fn`-`sln`'
				when lastname like '%% %%' and substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),substring_index(lastname,' ',-1)) then '`fi``sln`'
				when lastname like '%% %%' and substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),substring_index(lastname,' ',-1)) then '`fi`.`sln`'
				when lastname like '%% %%' and substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),'_',substring_index(lastname,' ',-1)) then '`fi`_`sln`'
				when lastname like '%% %%' and substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),'-',substring_index(lastname,' ',-1)) then '`fi`-`sln`'
				when lastname like '%% %%' and substring_index(ce.email,'@',1) = concat(left(replace(firstname,' ',''),1),left(substring_index(lastname,' ',-1),7)) then '`fi``sln7`'
				
				when substring_index(ce.email,'@',1) like concat(replace(firstname,' ',''),'.%%.',replace(lastname,' ','')) then '`fn`.*.`ln`'
				when substring_index(ce.email,'@',1) like concat(replace(firstname,' ',''),'%%',replace(lastname,' ','')) then '`fn``mi``ln`'
				when substring_index(ce.email,'@',1) like concat(left(replace(firstname,' ',''),1), left(replace(lastname,' ',''),1),'%%') then '`fi``li`'
				when substring_index(ce.email,'@',1) like concat(left(replace(firstname,' ',''),2), left(replace(lastname,' ',''),1),'%%') then '`fn2``li`'
				when substring_index(ce.email,'@',1) like concat(replace(firstname,' ',''), replace(lastname,' ',''),'%%') then '`fn``ln`'
				when substring_index(ce.email,'@',1) like concat(replace(firstname,' ',''),'.', replace(lastname,' ',''),'%%') then '`fn`.`ln`'
				when substring_index(ce.email,'@',1) like concat(replace(firstname,' ',''),'_', replace(lastname,' ',''),'%%') then '`fn`_`ln`'			    
				when substring_index(ce.email,'@',1) like concat(left(replace(firstname,' ',''),1), replace(lastname,' ',''),'%%') then '`fi``ln`'
				when substring_index(ce.email,'@',1) like concat(left(replace(firstname,' ',''),1),'%%',replace(lastname,' ',''),'%%') then '`fi``mi``ln`'
				when substring_index(ce.email,'@',1) like concat(left(replace(firstname,' ',''),1),'%%',replace(lastname,' ','')) then '`fi``mi``ln`'
				when substring_index(ce.email,'@',1) like concat(left(replace(firstname,' ',''),1),left(replace(lastname,' ',''),1),'%%') then '`fi``li`*'
				when substring_index(ce.email,'@',1) like concat(left(replace(firstname,' ',''),1),left(replace(ifnull(middlename,''),' ',''),1),left(replace(lastname,' ',''),1),'%%') then '`fi``mi``li`*'
				when substring_index(ce.email,'@',1) like concat(left(replace(firstname,' ',''),1),'%%',left(replace(lastname,' ',''),1),'%%') then '`fi``mi``li`*'
				when substring_index(ce.email,'@',1) like concat(left(replace(lastname,' ',''),5),'%%') then '`ln5`*'
				when substring_index(ce.email,'@',1) like concat(left(replace(lastname,' ',''),4),'%%') then '`ln4`*'
				when substring_index(ce.email,'@',1) like concat(replace(firstname,' ',''),'%%') then '`fn`*'
				
				else null
				end convention
			from mv_candidateswithlatestproginfo gme 
			left join clients c on gme.amaid = c.amaid
			left join contactemails ce on gme.userid = ce.userid	
			left join mv_aggregate_bounces b on b.emailid = ce.emailid 
			 left join msrecipients msr on msr.email = ce.email
			left join emailopens eo on eo.emailid = ce.emailid
			 left join nlclickthroughs nlc on nlc.userid = gme.userid
			left join users u on u.userid = gme.userid
			left join trialemails te on te.email = ce.email
			left join fairsinvitedschools fis on fis.organizationid = c.organizationid
			left join fairs f on f.fairlocation = fis.fairlocation
			left join fairlocations fl on fl.locationid = fis.fairlocation
			left join bademails be on be.email = ce.email
			where transitionyear >= year(now()) and be.email is null
			and emailtypeid in (6,7)
			and {criterion}
			and ce.email is not null and ce.email not like '%%*%%'
			group by ce.emailid ) x 
			group by organizationid, convention, domain order by total_ratio desc;'''

	convs = pd.read_sql_query(sql=sql,con=os.environ['MYSQL_LIVEDBSLAVE'])
	convs = convs[convs['total_ratio'] > 0.0].sort_values(by=['total_ratio'],ascending=False)
	if max_conventions:
		convs = convs.head(max_conventions)
	return convs.to_dict('records')

def email_gen_dispatch(user):
	if not all([user.get('firstname',None),user.get('lastname',None)]):
		return None, None

	user['middlename'] = user.get('middlename',None)
	fln = None if ' ' not in user['lastname'].strip() else user['lastname'].strip().split(' ',1)[0]
	flnhy = None if '-' not in user['lastname'] else user['lastname'].split('-',1)[0] # first of a hypenated last name
	sln = None if ' ' not in user['lastname'].strip() else user['lastname'].strip().split(' ',1)[1]
	slnhy = None if '-' not in user['lastname'] else user['lastname'].split('-',1)[1]	

	dispatch = {'`flnhy`':flnhy, '`prehyphen`':flnhy,'`fl`':user['lastname'].replace('-',''),
		'`ln7`':user['lastname'][:7],'`fn`':user['firstname'],'`ln`':user['lastname'], '`mn`':user['middlename'],'`fi`':user['firstname'][0],
		'`f1`':user['firstname'][0], '`li`':user['lastname'][0],'`mi`':user['middlename'][0] if user['middlename'] else None,'`sln`':sln}
	dispatch.update({f'`ln{i}`':user['lastname'][:i] for i in range(2,len(user['lastname']))})
	dispatch.update({f'`fn{i}`':user['firstname'][:i] for i in range(2,len(user['firstname']))})

	return re.compile('|'.join([k for k,v in dispatch.items() if v])), {k:v.lower() for k,v in dispatch.items() if v}

def guess_email(user,max_conventions=None):
	emails = []
	pattern, dispatch = email_gen_dispatch(user)
	if not all([pattern,dispatch]):
		return emails

	for conv in get_db_conventions(user.get('amaid',None),user.get('organizationid',None),max_conventions):						
		try:
			result = pattern.sub(lambda x: dispatch.get(x.group(),None), conv['convention'])
			email = replace_errors.translate('{u}@{d}'.format(u=result.strip(),d=conv['domain']).strip().lower())

			# * was used in cmdadmin as a placeholder by users indicating a bad email for a while				
			if '*' not in email and '`' not in email:
				emails.append(email)
		except TypeError:
			continue
		except Exception as e:
			raise
	
	return emails

if __name__ == '__main__':
	print(guess_email({'firstname':'Murph','lastname':'de Smurf','organizationid':398059}))
	print(guess_email({'firstname':'Murph','lastname':'de Smurf','amaid':1103900197},max_conventions=2))
