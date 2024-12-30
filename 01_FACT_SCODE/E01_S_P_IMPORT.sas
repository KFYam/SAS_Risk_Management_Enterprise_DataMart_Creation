/* ************************************************************************************************ */
/* To import the source file in SAS dataset in characters format for avoiding information loss.		*/
/* The SAS dataset in characters format is saved in libname "STG", we will have regular				*/
/* housekeeping on every 2 years in this libname "STG".												*/
/* ************************************************************************************************ */

%macro gen_S_P();
	x "cd &dir_s_p.";
	x "ls -al > filelist.txt";
	data work.sp_rating_files;
		infile "&&dir_s_p./filelist.txt" 
		DLM='7F'x MISSOVER DSD ;
		INPUT  text : $CHAR100.;
		length t_yyyymm fname fname2 $50 st_src $200;
		fname=scan(text,-1,' ');
		st_src=cats("&dir_s_p./",fname,"");
		t_yyyymm=tranwrd(scan(fname,-1,'_'),".xls","");
		if 	not missing(t_yyyymm) and missing(compress(t_yyyymm," 0123456789")) and 
			lengthn(t_yyyymm) eq 8 then t_date=input(t_yyyymm,yymmdd8.);
		if not missing(t_date) then do;
			suffix=put(intnx("MONTH",t_date,-1,"END"),yymmn6.);
			fname2=cats("raw_s_p.SP_Bank_Screen_",suffix);
			output;
		end;
	run;
	x "rm filelist.txt";
	proc sql noprint;
		select count(1) into :cnt from work.sp_rating_files;
	quit;
	%do i=1 %to &cnt.;
		data _null_;
			set work.sp_rating_files(firstobs=&i. obs=&i.);
			call symput("st_src", strip(st_src));
			call symput("fname2", upcase(strip(fname2)));
		run;
		%if %sysfunc(exist(&fname2.)) %then %do;
			%put ---- NCB LOG ---- : !! raw_s_p.&fname2. is already existed.;
		%end;
		%else %do;
			proc import datafile="&st_src." out=&fname2. dbms=xls REPLACE;
   				sheet='Screening'; NAMEROW=8; datarow=9; GETNAMES=YES; MIXED=yes;
			run;
		%end;
	%end;
%mend;

%exportlog(ind=START, path=&dir_log., name=S_P_&st_RptMth._LOAD_&st_cltinfo.);
%gen_S_P;
%exportlog(ind=STOP);
