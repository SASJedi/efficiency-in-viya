%let path=/home/student/Courses/efficiency;
libname sas "&path/data";
proc datasets library=db kill nolist nodetails;
run;
proc datasets library=sas kill nolist nodetails;
run; quit;

%INCLUDE "&path/data/create_BIG_CUSTOMER_DIM.sas" /encoding=latin1;
%INCLUDE "&path/data/BIG_ORDER_FACT.sas" /encoding=latin1;
%INCLUDE "&path/data/BIG_PRODUCT_DIM.sas" /encoding=latin1;

libname xl xlsx "&path/data/customers.xlsx";

data sas.my_customer_ids
     xl.my_customer_ids;
   set sas.big_customer_dim (keep=customer_id);
   where (mod(customer_id,80)=0);
run;
libname xl clear;

%let MaxObs=1000000;
data sas.narrow
     db.narrow;
  do ID=1 to &maxobs by 10521;
     output;
  end;
run;

data sas.narrow2
     db.narrow2;
   call streaminit(123456);
   do id=1 to &maxobs;
      ru=ceil(rand('UNIFORM')*10);
      rn=ceil(rand('NORMAL',1000,200));
      output;
   end;
run;

%let Groups=99;
data sas.wide
     db.wide;
   Length ID GroupID 8;
   array norm[300] ;
   array uni[300] ;
   array txt[200] $ 5;
   call streaminit(123456);
   drop i;
   do GroupID=1 to &groups;
      do id= groupID*100 to (groupID*100)+500;
         do i=1 to dim(norm);
            uni[i]=rand('Uniform');
            norm[i]=ceil(rand('Normal',uni[i]*5000,uni[i]*100));
         end;
         txt[1]=SUBSTRN('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
                      ,ceil(uni[1]*52),1);
         do i=2 to dim(txt);
            txt[i]=cats(SUBSTRN('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
                         ,ceil(uni[i]*52),1),txt[i-1]);
         end;
         output;
      end;
   end;
run;
