proc delete data=work.sessions;
run;
cas conn ;
proc cas;
session conn;
session.listSessions 
result=r;
saveresult r dataout=work.sessions;
run;
quit;
proc print data=work.sessions;
run;

data _null_;
   set work.sessions;
   Name=scan(sessionName,1,':');
   if State='Disconnected' then do;
	   command=catx(' ','cas',name,'uuid=',cats('"',uuid,'";'));
           call  execute(command);
   end;
   command=catx(' ','cas',name,'terminate;');
   call  execute(command);
run;
